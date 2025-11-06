"""Pytest fixtures for tap-persona tests."""

import pytest
import responses


@pytest.fixture
def mock_config():
    """Return mock configuration for testing.

    Returns:
        Dictionary with test configuration values.
    """
    return {
        "api_key": "test_api_key_12345",
        "base_url": "https://withpersona.com/api/v1",
        "start_date": "2025-01-01T00:00:00Z",
        "page_size": 100,
    }


@pytest.fixture
def mock_inquiries_response():
    """Return mock response for inquiries endpoint.

    Returns:
        Dictionary with sample inquiries data.
    """
    return {
        "data": [
            {
                "type": "inquiry",
                "id": "inq_123456",
                "attributes": {
                    "status": "completed",
                    "reference_id": "ref_001",
                    "created_at": "2025-01-15T10:30:00Z",
                    "updated_at": "2025-01-15T11:00:00Z",
                    "completed_at": "2025-01-15T11:00:00Z",
                    "name_first": "John",
                    "name_last": "Doe",
                    "email_address": "john.doe@example.com",
                    "inquiry_template_id": "itmpl_abc123",
                },
                "relationships": {
                    "verifications": {
                        "data": [
                            {"type": "verification", "id": "ver_123"}
                        ]
                    }
                }
            }
        ],
        "links": {
            "next": None
        }
    }


@pytest.fixture
def mock_cases_response():
    """Return mock response for cases endpoint.

    Returns:
        Dictionary with sample cases data.
    """
    return {
        "data": [
            {
                "type": "case",
                "id": "case_789012",
                "attributes": {
                    "status": "open",
                    "name": "Review Required",
                    "created_at": "2025-01-20T09:00:00Z",
                    "updated_at": "2025-01-20T14:30:00Z",
                    "resolved_at": None,
                    "assignee_id": "usr_456",
                    "case_template_id": "ctmpl_xyz789",
                },
                "relationships": {
                    "inquiries": {
                        "data": [
                            {"type": "inquiry", "id": "inq_123456"}
                        ]
                    }
                }
            }
        ],
        "links": {
            "next": None
        }
    }


@pytest.fixture
def mock_paginated_response():
    """Return mock paginated responses.

    Returns:
        List of response dictionaries for testing pagination.
    """
    page1 = {
        "data": [
            {
                "type": "inquiry",
                "id": "inq_001",
                "attributes": {
                    "status": "pending",
                    "created_at": "2025-01-01T00:00:00Z",
                    "updated_at": "2025-01-01T00:00:00Z",
                }
            }
        ],
        "links": {
            "next": "https://withpersona.com/api/v1/inquiries?page[after]=cursor_123"
        }
    }

    page2 = {
        "data": [
            {
                "type": "inquiry",
                "id": "inq_002",
                "attributes": {
                    "status": "completed",
                    "created_at": "2025-01-02T00:00:00Z",
                    "updated_at": "2025-01-02T00:00:00Z",
                }
            }
        ],
        "links": {
            "next": None
        }
    }

    return [page1, page2]


@pytest.fixture(autouse=True)
def mock_sleep(monkeypatch):
    """Disable sleep in all tests to speed up execution.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    monkeypatch.setattr("time.sleep", lambda x: None)
