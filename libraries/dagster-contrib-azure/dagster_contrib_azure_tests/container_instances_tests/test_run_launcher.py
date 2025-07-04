import pytest
from unittest.mock import MagicMock, patch
from dagster import (
    DagsterInstance,
    DagsterRun,
    DagsterRunStatus,
    _check as check,
)
from dagster._core.launcher import WorkerStatus
from dagster_contrib_azure.container_instances.run_launcher import ContainerInstancesRunLauncher


@pytest.fixture
def mock_container_client():
    with patch("dagster_contrib_azure.container_instances.run_launcher.ContainerInstanceManagementClient") as mock_client:
        mock_instance = MagicMock()
        mock_client.return_value = mock_instance
        
        # Mock the container group creation
        mock_operation = MagicMock()
        mock_operation.result.return_value = None
        mock_instance.container_groups.begin_create_or_update.return_value = mock_operation
        
        # Mock the container group get for health checks
        mock_container_group = MagicMock()
        mock_container_group.instance_view.state = "Succeeded"
        mock_instance.container_groups.get.return_value = mock_container_group
        
        yield mock_instance


@pytest.fixture
def launcher():
    return ContainerInstancesRunLauncher(
        subscription_id="test_subscription",
        resource_group="test_resource_group", 
        location="eastus",
        container_image="test-image:latest",
        container_group_by_code_location={
            "test_location": "test_container_group"
        }
    )


def test_get_container_config_simple_string(launcher):
    """Test simple string configuration for container group."""
    config = launcher.get_container_config_for_code_location("test_location")
    
    assert config["name"] == "test_container_group"
    assert config["subscription_id"] == "test_subscription"
    assert config["resource_group"] == "test_resource_group"
    assert config["location"] == "eastus"


def test_get_container_config_dict_config(launcher):
    """Test dictionary configuration with overrides."""
    launcher.container_group_by_code_location = {
        "test_location": {
            "name": "custom_container_group",
            "resource_group": "custom_resource_group",
            "location": "westus"
        }
    }
    
    config = launcher.get_container_config_for_code_location("test_location")
    
    assert config["name"] == "custom_container_group"
    assert config["subscription_id"] == "test_subscription"  # default
    assert config["resource_group"] == "custom_resource_group"  # override
    assert config["location"] == "westus"  # override


def test_get_container_config_missing_location():
    """Test error when code location is not configured."""
    launcher = ContainerInstancesRunLauncher(
        subscription_id="test_subscription",
        resource_group="test_resource_group",
        location="eastus", 
        container_image="test-image:latest",
        container_group_by_code_location={}
    )
    
    with pytest.raises(Exception, match="No run launcher defined for code location"):
        launcher.get_container_config_for_code_location("missing_location")


def test_env_override_simple_config(launcher):
    """Test that simple string config returns no environment overrides."""
    env = launcher.env_override_for_code_location("test_location")
    assert env is None


def test_env_override_with_env_vars(launcher):
    """Test environment variable configuration."""
    launcher.container_group_by_code_location = {
        "test_location": {
            "name": "test_container_group",
            "CUSTOM_VAR": {"env": "SOME_ENV_VAR"}
        }
    }
    
    with patch.dict("os.environ", {"SOME_ENV_VAR": "test_value"}):
        env = launcher.env_override_for_code_location("test_location")
        assert env["CUSTOM_VAR"] == "test_value"


def test_config_type():
    """Test the configuration schema."""
    config_schema = ContainerInstancesRunLauncher.config_type()
    
    # Check required fields are present
    assert "subscription_id" in config_schema
    assert "resource_group" in config_schema  
    assert "location" in config_schema
    assert "container_image" in config_schema
    assert "container_group_by_code_location" in config_schema
    
    # Check optional fields with defaults
    assert "cpu_limit" in config_schema
    assert "memory_limit" in config_schema
    assert "run_timeout" in config_schema
    assert "run_job_retry" in config_schema
