import traceback
import uuid
from typing import TYPE_CHECKING, Any, Mapping, Optional, Sequence, Union
import os

import tenacity
from dagster import (
    DagsterInstance,
    Field,
    Permissive,
    StringSource,
    _check as check,
)
from dagster._core.events import EngineEventData
from dagster._core.launcher.base import (
    CheckRunHealthResult,
    LaunchRunContext,
    RunLauncher,
    WorkerStatus,
)
from dagster._core.storage.dagster_run import DagsterRun
from dagster._grpc.types import ExecuteRunArgs
from dagster._serdes import ConfigurableClass, ConfigurableClassData
from azure.core.exceptions import AzureError, HttpResponseError
from azure.identity import DefaultAzureCredential
from azure.mgmt.containerinstance import ContainerInstanceManagementClient
from azure.mgmt.containerinstance.models import (
    Container,
    ContainerGroup,
    ContainerGroupRestartPolicy,
    EnvironmentVariable,
    OperatingSystemTypes,
    ResourceRequests,
    ResourceRequirements,
)

from typing_extensions import Self

if TYPE_CHECKING:
    from dagster._config.config_schema import UserConfigSchema

ENV_KEY = "env"
SECRETS_KEY = "secret_name"


class ContainerInstancesRunLauncher(RunLauncher, ConfigurableClass):
    """Run launcher for launching runs as Azure Container Instances."""

    def __init__(
        self,
        subscription_id: str,
        resource_group: str,
        location: str,
        container_group_by_code_location: "dict[str, Union[str, dict[str, str]]]",
        container_image: str,
        cpu_limit: float = 1.0,
        memory_limit: float = 1.5,
        run_timeout: int = 3600,
        run_job_retry: "dict[str, int]" = None,
        inst_data: Optional[ConfigurableClassData] = None,
    ):
        self._inst_data = inst_data
        self.subscription_id = subscription_id
        self.resource_group = resource_group
        self.location = location
        self.container_group_by_code_location = container_group_by_code_location
        self.container_image = container_image
        self.cpu_limit = cpu_limit
        self.memory_limit = memory_limit
        self.run_timeout = run_timeout

        if run_job_retry is None:
            run_job_retry = {"wait": 10, "timeout": 300}
        self.run_job_retry_wait = run_job_retry["wait"]
        self.run_job_retry_timeout = run_job_retry["timeout"]

        # Initialize Azure credentials and client
        self.credential = DefaultAzureCredential()
        self.container_client = ContainerInstanceManagementClient(
            credential=self.credential, subscription_id=self.subscription_id
        )

    def launch_run(self, context: LaunchRunContext) -> None:
        remote_job_origin = check.not_none(context.dagster_run.remote_job_origin)
        current_code_location = remote_job_origin.location_name

        job_origin = check.not_none(context.job_code_origin)
        repository_origin = job_origin.repository_origin

        stripped_repository_origin = repository_origin._replace(container_context={})
        stripped_job_origin = job_origin._replace(
            repository_origin=stripped_repository_origin
        )

        args = ExecuteRunArgs(
            job_origin=stripped_job_origin,
            run_id=context.dagster_run.run_id,
            instance_ref=self._instance.get_ref(),
        )

        command_args = args.get_command_args()

        container_group_name = self.create_container_instance(
            current_code_location, command_args
        )

        instance: DagsterInstance = self._instance
        instance.report_engine_event(
            message="Launched run in Azure Container Instance",
            dagster_run=context.dagster_run,
            engine_event_data=EngineEventData({"Container Group": container_group_name}),
            cls=self.__class__,
        )
        instance.add_run_tags(
            context.dagster_run.run_id, {"azure_container_group_name": container_group_name}
        )

    def get_subscription_id_for_code_location_or_default(
        self, container_config: dict[str, Any]
    ) -> str:
        container_config = check.dict_param(container_config, "container_config")
        return container_config.get("subscription_id", self.subscription_id)

    def get_resource_group_for_code_location_or_default(
        self, container_config: dict[str, Any]
    ) -> str:
        container_config = check.dict_param(container_config, "container_config")
        return container_config.get("resource_group", self.resource_group)

    def get_location_for_code_location_or_default(
        self, container_config: dict[str, Any]
    ) -> str:
        container_config = check.dict_param(container_config, "container_config")
        return container_config.get("location", self.location)

    def get_container_group_name_for_code_location(
        self, container_config: dict[str, Any]
    ) -> str:
        container_config = check.dict_param(container_config, "container_config")
        return container_config["name"]

    def get_container_config_for_code_location(self, code_location_name: str) -> dict[str, Any]:
        try:
            config = self.container_group_by_code_location[code_location_name]
        except KeyError:
            raise Exception(
                f"No run launcher defined for code location: {code_location_name}"
            )

        # Simple string configuration
        if isinstance(config, str):
            return {
                "name": config,
                "subscription_id": self.subscription_id,
                "resource_group": self.resource_group,
                "location": self.location,
            }

        # Dictionary configuration - validate required name field
        if "name" not in config:
            raise Exception(
                f"Container group configuration for {code_location_name} must include 'name' field"
            )

        return {
            "name": config["name"],
            "subscription_id": self.get_subscription_id_for_code_location_or_default(config),
            "resource_group": self.get_resource_group_for_code_location_or_default(config),
            "location": self.get_location_for_code_location_or_default(config),
        }

    def resolve_secret(self, secret_name: str) -> Any:
        # In a real implementation, this would integrate with Azure Key Vault
        # For now, we'll treat it as an environment variable
        # TODO: Implement Azure Key Vault integration
        return os.getenv(secret_name)

    def env_override_for_code_location(
        self, code_location_name: str
    ) -> Optional[dict[str, str]]:
        """
        Build environment variable override context to pass to Container Instance if configured
        """
        try:
            config = self.container_group_by_code_location[code_location_name]
        except KeyError:
            raise Exception(
                f"No run launcher defined for code location: {code_location_name}"
            )

        # No custom configuration at all
        if isinstance(config, str):
            return None

        env = {}
        for setting_name in config:
            # container group names are expected to be explicit
            if setting_name in ["name", "subscription_id", "resource_group", "location"]:
                continue

            node_config = config.get(setting_name)
            try:
                node_config = check.dict_param(node_config, "node_config")
            except check.ParameterCheckError:
                # Explicit config
                env[setting_name] = node_config
                continue

            if ENV_KEY in node_config:
                # Configuration use environment variables
                env_var = node_config[ENV_KEY]
                env[setting_name] = os.getenv(env_var) if env_var is not None else None

            elif SECRETS_KEY in node_config:
                # Configuration use secrets (Key Vault)
                secret_name = node_config[SECRETS_KEY]
                env[setting_name] = self.resolve_secret(secret_name)
            else:
                raise KeyError(
                    f"Unsupported Code Location configuration. Missing required keys for {code_location_name}"
                )
        return env

    def create_container_instance(
        self, code_location_name: str, args: Sequence[str]
    ) -> str:
        container_config = self.get_container_config_for_code_location(code_location_name)
        container_env = self.env_override_for_code_location(code_location_name)
        
        return self.create_container_group(
            container_config=container_config,
            args=args,
            env=container_env,
        )

    def create_container_group(
        self,
        container_config: dict[str, Any],
        args: Optional[Sequence[str]] = None,
        env: Optional["dict[str, str]"] = None,
    ) -> str:
        # Generate unique container group name for this run
        base_name = container_config["name"]
        unique_suffix = str(uuid.uuid4())[:8]
        container_group_name = f"{base_name}-{unique_suffix}"

        # Prepare environment variables
        environment_variables = []
        if env:
            for name, value in env.items():
                if value is not None:
                    environment_variables.append(
                        EnvironmentVariable(name=name, value=str(value))
                    )

        # Create container definition
        container = Container(
            name="dagster-run",
            image=self.container_image,
            resources=ResourceRequirements(
                requests=ResourceRequests(
                    cpu=self.cpu_limit,
                    memory_in_gb=self.memory_limit,
                )
            ),
            command=list(args) if args else None,
            environment_variables=environment_variables,
        )

        # Create container group definition
        container_group = ContainerGroup(
            location=container_config["location"],
            containers=[container],
            os_type=OperatingSystemTypes.LINUX,
            restart_policy=ContainerGroupRestartPolicy.NEVER,
        )

        @tenacity.retry(
            wait=tenacity.wait_fixed(self.run_job_retry_wait),
            stop=tenacity.stop_after_delay(self.run_job_retry_timeout),
            retry=tenacity.retry_if_exception_type(HttpResponseError),
        )
        def create_container_group_with_retries():
            return self.container_client.container_groups.begin_create_or_update(
                resource_group_name=container_config["resource_group"],
                container_group_name=container_group_name,
                container_group=container_group,
            )

        operation = create_container_group_with_retries()
        # Wait for the operation to complete
        operation.result()

        return container_group_name

    def terminate(self, run_id: str) -> bool:
        instance: DagsterInstance = self._instance
        run = check.not_none(instance.get_run_by_id(run_id))
        container_group_name = run.tags.get("azure_container_group_name")

        if not container_group_name:
            self._instance.report_engine_event(
                message="Unable to identify Azure Container Group for termination",
                dagster_run=run,
                cls=self.__class__,
            )
            return False

        instance.report_run_canceling(run)
        remote_job_origin = check.not_none(run.remote_job_origin)
        
        try:
            container_config = self.get_container_config_for_code_location(
                remote_job_origin.location_name
            )
            
            self.container_client.container_groups.begin_delete(
                resource_group_name=container_config["resource_group"],
                container_group_name=container_group_name,
            )
        except (AzureError, HttpResponseError):
            self._instance.report_engine_event(
                message=f"Failed to terminate Azure Container Group: {container_group_name}. Error:\n{traceback.format_exc()}",
                dagster_run=run,
                cls=self.__class__,
            )
            return False

        instance.report_run_canceled(run)
        return True

    @property
    def inst_data(self) -> Optional[ConfigurableClassData]:
        return self._inst_data

    @classmethod
    def config_type(cls) -> "UserConfigSchema":
        return {
            "subscription_id": Field(
                StringSource,
                is_required=True,
                description="Azure subscription ID",
            ),
            "resource_group": Field(
                StringSource,
                is_required=True,
                description="Azure resource group for the Container Instances",
            ),
            "location": Field(
                StringSource,
                is_required=True,
                description="Azure location/region for the Container Instances",
            ),
            "container_image": Field(
                StringSource,
                is_required=True,
                description="Container image to use for running Dagster jobs",
            ),
            "container_group_by_code_location": Field(
                Permissive({}),
                is_required=True,
                description=(
                    "Container group configuration for each code location. Each item in this map may be a key-value"
                    " pair where the key is the code location name and the value is the container group name. "
                    "Optionally, each code location key may specify additional configuration like 'subscription_id', "
                    "'resource_group', and 'location' override values."
                ),
            ),
            "cpu_limit": Field(
                float,
                is_required=False,
                default_value=1.0,
                description="CPU limit for container instances (in CPU cores)",
            ),
            "memory_limit": Field(
                float,
                is_required=False,
                default_value=1.5,
                description="Memory limit for container instances (in GB)",
            ),
            "run_job_retry": Field(
                {
                    "wait": Field(
                        int,
                        is_required=False,
                        default_value=10,
                        description="Number of seconds to wait between retries",
                    ),
                    "timeout": Field(
                        int,
                        is_required=False,
                        default_value=300,
                        description="Number of seconds to wait before timing out",
                    ),
                },
                is_required=False,
                default_value={"wait": 10, "timeout": 300},
                description="Retry configuration for container instance creation requests.",
            ),
            "run_timeout": Field(
                int,
                is_required=False,
                default_value=3600,
                description="Timeout for the Container Instance execution in seconds",
            ),
        }

    @classmethod
    def from_config_value(
        cls, inst_data: ConfigurableClassData, config_value: Mapping[str, Any]
    ) -> Self:
        return cls(inst_data=inst_data, **config_value)

    @property
    def supports_check_run_worker_health(self):
        return True

    def check_run_worker_health(self, run: DagsterRun) -> CheckRunHealthResult:
        container_group_name = run.tags.get("azure_container_group_name")

        if not container_group_name:
            return CheckRunHealthResult(WorkerStatus.UNKNOWN)

        remote_job_origin = check.not_none(run.remote_job_origin)
        
        try:
            container_config = self.get_container_config_for_code_location(
                remote_job_origin.location_name
            )
            
            container_group = self.container_client.container_groups.get(
                resource_group_name=container_config["resource_group"],
                container_group_name=container_group_name,
            )
            
            # Check the state of the container group
            if container_group.instance_view and container_group.instance_view.state:
                state = container_group.instance_view.state
                
                if state in ["Pending", "Running"]:
                    return CheckRunHealthResult(WorkerStatus.RUNNING)
                elif state in ["Failed", "Stopped"]:
                    # Check if any containers failed
                    if (container_group.containers and 
                        any(c.instance_view and c.instance_view.current_state and 
                            c.instance_view.current_state.exit_code != 0 
                            for c in container_group.containers)):
                        return CheckRunHealthResult(WorkerStatus.FAILED)
                    else:
                        return CheckRunHealthResult(WorkerStatus.SUCCESS)
                elif state == "Succeeded":
                    return CheckRunHealthResult(WorkerStatus.SUCCESS)
                else:
                    return CheckRunHealthResult(
                        WorkerStatus.UNKNOWN, msg=f"Unknown container state: {state}"
                    )
            else:
                return CheckRunHealthResult(
                    WorkerStatus.UNKNOWN, msg="Unable to determine container state"
                )
                
        except (AzureError, HttpResponseError):
            return CheckRunHealthResult(
                WorkerStatus.UNKNOWN, msg="Unable to fetch container status"
            )
