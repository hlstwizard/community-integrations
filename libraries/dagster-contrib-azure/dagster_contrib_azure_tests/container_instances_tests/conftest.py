import pytest
from unittest.mock import MagicMock, patch


@pytest.fixture
def mock_azure_credential():
    """Mock Azure credentials."""
    with patch("dagster_contrib_azure.container_instances.run_launcher.DefaultAzureCredential") as mock_cred:
        yield mock_cred.return_value


@pytest.fixture  
def mock_container_client():
    """Mock Azure Container Instance Management Client."""
    with patch("dagster_contrib_azure.container_instances.run_launcher.ContainerInstanceManagementClient") as mock_client:
        mock_instance = MagicMock()
        mock_client.return_value = mock_instance
        
        # Mock successful container group creation
        mock_operation = MagicMock()
        mock_operation.result.return_value = None
        mock_instance.container_groups.begin_create_or_update.return_value = mock_operation
        
        # Mock container group status for health checks
        mock_container_group = MagicMock()
        mock_container_group.instance_view.state = "Succeeded"
        mock_instance.container_groups.get.return_value = mock_container_group
        
        # Mock container group deletion
        mock_delete_operation = MagicMock()
        mock_instance.container_groups.begin_delete.return_value = mock_delete_operation
        
        yield mock_instance
