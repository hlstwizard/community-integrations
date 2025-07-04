"""
Example demonstrating how to use the Azure Container Instances Run Launcher.
"""

from dagster import DagsterInstance
from dagster_contrib_azure.container_instances.run_launcher import ContainerInstancesRunLauncher

def create_instance_with_azure_launcher():
    """Create a Dagster instance configured to use Azure Container Instances."""
    
    launcher = ContainerInstancesRunLauncher(
        subscription_id="your-azure-subscription-id",
        resource_group="your-resource-group",
        location="eastus",
        container_image="your-registry.azurecr.io/dagster:latest",
        cpu_limit=2.0,
        memory_limit=4.0,
        container_group_by_code_location={
            # Simple configuration
            "my-code-location": "my-container-group",
            
            # Advanced configuration with overrides
            "advanced-location": {
                "name": "advanced-container-group",
                "resource_group": "different-rg",
                "location": "westus",
                # Environment variables for this location
                "DATABASE_URL": {"env": "DATABASE_URL"},
                "API_KEY": {"secret_name": "api-key-secret"},
            }
        },
        run_job_retry={
            "wait": 15,
            "timeout": 600
        },
        run_timeout=3600
    )
    
    # In a real scenario, you would configure this through dagster.yaml
    # This is just for demonstration purposes
    return DagsterInstance(
        instance_type=DagsterInstance.EPHEMERAL,
        local_artifact_storage_dir="/tmp/dagster",
        run_launcher=launcher
    )

if __name__ == "__main__":
    # Example usage
    instance = create_instance_with_azure_launcher()
    print(f"Created instance with Azure Container Instances launcher: {instance}")
    print(f"Launcher supports health checks: {instance.run_launcher.supports_check_run_worker_health}")
