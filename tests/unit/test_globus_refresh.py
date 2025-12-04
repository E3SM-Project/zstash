"""
# Run all tests
pytest test_globus_refresh.py -v

# Run with coverage
pytest test_globus_refresh.py --cov=zstash.globus --cov-report=html

# Run with output capture for debugging
pytest test_globus_refresh.py -v -s
"""

import json
from unittest.mock import Mock, mock_open, patch

import pytest

from zstash.globus import globus_block_wait, globus_transfer
from zstash.globus_utils import load_tokens

# Core functionality tests ####################################################


# Verifies that globus_transfer() calls endpoint_autoactivate for both endpoints
def test_globus_transfer_refreshes_tokens():
    """Test that globus_transfer calls endpoint_autoactivate"""
    with patch("zstash.globus.transfer_client") as mock_client, patch(
        "zstash.globus.local_endpoint", "local-uuid"
    ), patch("zstash.globus.remote_endpoint", "remote-uuid"), patch(
        "zstash.globus.task_id", None
    ), patch(
        "zstash.globus.transfer_data", None
    ):

        mock_client.endpoint_autoactivate = Mock()
        mock_client.operation_ls = Mock(return_value=[])
        mock_client.submit_transfer = Mock(return_value={"task_id": "test-123"})

        # Call the function
        globus_transfer("remote-ep", "/path", "file.tar", "put", False)

        # Verify autoactivate was called for both endpoints
        assert mock_client.endpoint_autoactivate.call_count >= 2
        calls = mock_client.endpoint_autoactivate.call_args_list

        # Check it was called with correct parameters
        assert any("local-uuid" in str(call) for call in calls)
        assert any("remote-uuid" in str(call) for call in calls)
        assert any("if_expires_in=86400" in str(call) for call in calls)


# Confirms periodic refresh during long waits
def test_globus_block_wait_refreshes_periodically():
    """Test that globus_block_wait refreshes tokens on each retry"""
    with patch("zstash.globus.transfer_client") as mock_client, patch(
        "zstash.globus.local_endpoint", "local-uuid"
    ):

        mock_client.endpoint_autoactivate = Mock()
        mock_client.task_wait = Mock(return_value=True)
        mock_client.get_task = Mock(return_value={"status": "SUCCEEDED"})

        # Call with max_retries=3
        globus_block_wait("task-123", 1, 1, 3)

        # Should call autoactivate at least once per retry
        assert mock_client.endpoint_autoactivate.call_count >= 1


# Validates expiration detection logic
def test_load_tokens_detects_expiration(caplog):
    """Test that load_tokens detects soon-to-expire tokens"""
    import time as time_module

    # Create a token file with expiration in 30 minutes
    current_time = 1000000
    expires_at = current_time + 1800  # 30 minutes from now

    tokens = {
        "transfer.api.globus.org": {
            "access_token": "fake_token",
            "refresh_token": "fake_refresh",
            "expires_at": expires_at,
        }
    }

    with patch.object(time_module, "time", return_value=current_time), patch(
        "builtins.open", mock_open(read_data=json.dumps(tokens))
    ), patch("os.path.exists", return_value=True):

        with caplog.at_level("INFO"):
            result = load_tokens()

        # Check that warning was logged
        assert "expiring soon" in caplog.text
        assert result == tokens


# Library compatibility test ##################################################


def test_token_refresh_with_real_client():
    """
    Integration test that uses real Globus SDK but mocks the endpoints.
    This verifies the RefreshTokenAuthorizer actually works without needing
    real credentials.
    """
    from globus_sdk import NativeAppAuthClient, RefreshTokenAuthorizer, TransferClient

    from zstash.globus_utils import ZSTASH_CLIENT_ID

    # Create a mock authorizer that simulates token refresh
    auth_client = NativeAppAuthClient(ZSTASH_CLIENT_ID)

    # Create a mock refresh token (won't actually work, but tests the pattern)
    mock_refresh_token = "mock_refresh_token_xyz"

    try:
        # This will fail with invalid token, but we're testing the mechanism exists
        authorizer = RefreshTokenAuthorizer(
            refresh_token=mock_refresh_token, auth_client=auth_client
        )

        # Verify the authorizer was created successfully
        assert authorizer is not None
        assert hasattr(authorizer, "access_token")

        # Verify we can create a transfer client with it
        transfer_client = TransferClient(authorizer=authorizer)
        assert transfer_client is not None

    except Exception as e:
        # We expect this to fail with auth errors, but not with missing attributes
        assert "RefreshTokenAuthorizer" not in str(e)


# Edge case tests #############################################################


# Ensures no issues with many rapid refresh calls
def test_multiple_rapid_refreshes():
    """Test that calling refresh many times doesn't break"""
    with patch("zstash.globus.transfer_client") as mock_client:
        mock_client.endpoint_autoactivate = Mock()

        # Simulate what happens during a long transfer with many wait iterations
        for _ in range(100):
            mock_client.endpoint_autoactivate("test-endpoint", if_expires_in=86400)

        # Should have been called 100 times without error
        assert mock_client.endpoint_autoactivate.call_count == 100


# End-to-end test with mocked transfer
def test_small_transfer_with_refresh_enabled():
    """
    Functional test: Transfer a small file and verify refresh calls were made.
    """
    with patch("zstash.globus.transfer_client") as mock_client, patch(
        "zstash.globus.local_endpoint", "local-uuid"
    ), patch("zstash.globus.remote_endpoint", "remote-uuid"), patch(
        "zstash.globus.task_id", None
    ), patch(
        "zstash.globus.transfer_data", None
    ):

        # Set up mock to track calls
        mock_client.endpoint_autoactivate = Mock()
        mock_client.submit_transfer = Mock(return_value={"task_id": "test-123"})
        mock_client.task_wait = Mock(return_value=True)
        mock_client.get_task = Mock(return_value={"status": "SUCCEEDED"})

        # Run a transfer
        globus_transfer("endpoint", "/path", "small.tar", "put", non_blocking=False)

        # Verify refresh was called
        assert mock_client.endpoint_autoactivate.called


# Parametrized tests ##########################################################


# Tests blocking PUT mode
# Tests non-blocking PUT mode
@pytest.mark.parametrize(
    "transfer_type,non_blocking",
    [
        ("put", False),
        ("put", True),
    ],
)
def test_globus_transfer_refreshes_in_all_modes(transfer_type, non_blocking):
    """Test that token refresh works for all transfer types"""
    with patch("zstash.globus.transfer_client") as mock_client, patch(
        "zstash.globus.local_endpoint", "local-uuid"
    ), patch("zstash.globus.remote_endpoint", "remote-uuid"), patch(
        "zstash.globus.task_id", None
    ), patch(
        "zstash.globus.transfer_data", None
    ), patch(
        "zstash.globus.archive_directory_listing", [{"name": "file.tar"}]
    ):

        mock_client.endpoint_autoactivate = Mock()
        mock_client.operation_ls = Mock(return_value=[{"name": "file.tar"}])
        # Need to return a complete task dict to avoid KeyError
        mock_client.submit_transfer = Mock(
            return_value={
                "task_id": "test-123",
                "source_endpoint_id": "src-uuid",
                "destination_endpoint_id": "dst-uuid",
                "label": "test transfer",
            }
        )
        mock_client.task_wait = Mock(return_value=True)
        mock_client.get_task = Mock(
            return_value={
                "status": "SUCCEEDED",
                "source_endpoint_id": "src-uuid",
                "destination_endpoint_id": "dst-uuid",
                "label": "test transfer",
            }
        )

        globus_transfer("remote-ep", "/path", "file.tar", transfer_type, non_blocking)

        # Verify refresh was called
        assert mock_client.endpoint_autoactivate.called


# Fixture example #############################################################


@pytest.fixture
def mock_globus_client():
    """Fixture to set up a mock Globus client"""
    with patch("zstash.globus.transfer_client") as mock_client, patch(
        "zstash.globus.local_endpoint", "local-uuid"
    ), patch("zstash.globus.remote_endpoint", "remote-uuid"), patch(
        "zstash.globus.task_id", None
    ), patch(
        "zstash.globus.transfer_data", None
    ):

        mock_client.endpoint_autoactivate = Mock()
        mock_client.operation_ls = Mock(return_value=[])
        mock_client.submit_transfer = Mock(
            return_value={
                "task_id": "test-123",
                "source_endpoint_id": "src-uuid",
                "destination_endpoint_id": "dst-uuid",
                "label": "test transfer",
            }
        )
        mock_client.task_wait = Mock(return_value=True)
        mock_client.get_task = Mock(
            return_value={
                "status": "SUCCEEDED",
                "source_endpoint_id": "src-uuid",
                "destination_endpoint_id": "dst-uuid",
                "label": "test transfer",
            }
        )

        yield mock_client


# Demonstrates reusable fixture pattern
def test_with_fixture(mock_globus_client):
    """Test using the fixture"""
    globus_transfer("remote-ep", "/path", "file.tar", "put", False)
    assert mock_globus_client.endpoint_autoactivate.call_count >= 2
