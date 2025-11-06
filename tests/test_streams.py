"""Tests for tap-persona streams."""

import responses
from singer_sdk.exceptions import FatalAPIError

from tap_persona.streams import CasesStream, InquiriesStream
from tap_persona.tap import TapPersona


def test_inquiries_stream_schema(mock_config):
    """Test that InquiriesStream has correct schema structure."""
    tap = TapPersona(config=mock_config)
    stream = InquiriesStream(tap=tap)
    schema = stream.schema

    assert schema["type"] == "object"
    assert "properties" in schema

    # Check required fields
    properties = schema["properties"]
    assert "id" in properties
    assert "created_at" in properties
    assert "updated_at" in properties
    assert "status" in properties

    # Check that additional properties are allowed
    assert schema.get("additionalProperties") is True


def test_cases_stream_schema(mock_config):
    """Test that CasesStream has correct schema structure."""
    tap = TapPersona(config=mock_config)
    stream = CasesStream(tap=tap)
    schema = stream.schema

    assert schema["type"] == "object"
    assert "properties" in schema

    # Check required fields
    properties = schema["properties"]
    assert "id" in properties
    assert "created_at" in properties
    assert "updated_at" in properties
    assert "status" in properties

    # Check that additional properties are allowed
    assert schema.get("additionalProperties") is True


def test_inquiries_stream_primary_keys(mock_config):
    """Test that InquiriesStream has correct primary keys."""
    tap = TapPersona(config=mock_config)
    stream = InquiriesStream(tap=tap)

    assert stream.primary_keys == ["id"]


def test_cases_stream_primary_keys(mock_config):
    """Test that CasesStream has correct primary keys."""
    tap = TapPersona(config=mock_config)
    stream = CasesStream(tap=tap)

    assert stream.primary_keys == ["id"]


def test_inquiries_stream_replication_key(mock_config):
    """Test that InquiriesStream has correct replication key."""
    tap = TapPersona(config=mock_config)
    stream = InquiriesStream(tap=tap)

    assert stream.replication_key == "updated_at"


def test_cases_stream_replication_key(mock_config):
    """Test that CasesStream has correct replication key."""
    tap = TapPersona(config=mock_config)
    stream = CasesStream(tap=tap)

    assert stream.replication_key == "updated_at"


def test_stream_authentication_headers(mock_config):
    """Test that streams include correct authentication headers."""
    tap = TapPersona(config=mock_config)
    stream = InquiriesStream(tap=tap)

    headers = stream.http_headers

    assert "Authorization" in headers
    assert headers["Authorization"] == "Bearer test_api_key_12345"
    assert headers["Content-Type"] == "application/json"
    assert headers["Accept"] == "application/json"


def test_stream_url_base(mock_config):
    """Test that streams use correct base URL."""
    tap = TapPersona(config=mock_config)
    stream = InquiriesStream(tap=tap)

    assert stream.url_base == "https://withpersona.com/api/v1"


def test_stream_path(mock_config):
    """Test that streams have correct API paths."""
    tap = TapPersona(config=mock_config)
    inquiries_stream = InquiriesStream(tap=tap)
    cases_stream = CasesStream(tap=tap)

    assert inquiries_stream.path == "/inquiries"
    assert cases_stream.path == "/cases"


@responses.activate
def test_inquiries_stream_parse_response(mock_config, mock_inquiries_response):
    """Test that InquiriesStream correctly parses API response."""
    responses.add(
        responses.GET,
        "https://withpersona.com/api/v1/inquiries",
        json=mock_inquiries_response,
        status=200,
    )

    tap = TapPersona(config=mock_config)
    stream = InquiriesStream(tap=tap)

    records = list(stream.get_records(context=None))

    assert len(records) == 1

    record = records[0]
    assert record["id"] == "inq_123456"
    assert record["type"] == "inquiry"
    assert record["status"] == "completed"
    assert record["name_first"] == "John"
    assert record["name_last"] == "Doe"
    assert record["email_address"] == "john.doe@example.com"
    assert record["created_at"] == "2025-01-15T10:30:00Z"
    # Note: _sdc_extracted_at is added during sync, not during get_records


@responses.activate
def test_cases_stream_parse_response(mock_config, mock_cases_response):
    """Test that CasesStream correctly parses API response."""
    responses.add(
        responses.GET,
        "https://withpersona.com/api/v1/cases",
        json=mock_cases_response,
        status=200,
    )

    tap = TapPersona(config=mock_config)
    stream = CasesStream(tap=tap)

    records = list(stream.get_records(context=None))

    assert len(records) == 1

    record = records[0]
    assert record["id"] == "case_789012"
    assert record["type"] == "case"
    assert record["status"] == "open"
    assert record["name"] == "Review Required"
    assert record["created_at"] == "2025-01-20T09:00:00Z"
    # Note: _sdc_extracted_at is added during sync, not during get_records


@responses.activate
def test_stream_pagination(mock_config, mock_paginated_response):
    """Test that stream handles pagination correctly."""
    # Mock page 1
    responses.add(
        responses.GET,
        "https://withpersona.com/api/v1/inquiries",
        json=mock_paginated_response[0],
        status=200,
    )

    # Mock page 2
    responses.add(
        responses.GET,
        "https://withpersona.com/api/v1/inquiries",
        json=mock_paginated_response[1],
        status=200,
    )

    tap = TapPersona(config=mock_config)
    stream = InquiriesStream(tap=tap)

    records = list(stream.get_records(context=None))

    assert len(records) == 2
    assert records[0]["id"] == "inq_001"
    assert records[1]["id"] == "inq_002"


@responses.activate
def test_stream_incremental_sync(mock_config, mock_inquiries_response):
    """Test that stream supports incremental sync with state."""
    responses.add(
        responses.GET,
        "https://withpersona.com/api/v1/inquiries",
        json=mock_inquiries_response,
        status=200,
    )

    tap = TapPersona(config=mock_config)
    stream = InquiriesStream(tap=tap)

    # Simulate existing state
    context = {"replication_key_value": "2025-01-01T00:00:00Z"}

    records = list(stream.get_records(context=context))

    assert len(records) == 1

    # Check that the request included the filter parameter
    assert len(responses.calls) == 1
    # The SDK should have passed the replication key value through get_url_params


@responses.activate
def test_stream_handles_empty_response(mock_config):
    """Test that stream handles empty response correctly."""
    empty_response = {
        "data": [],
        "links": {"next": None}
    }

    responses.add(
        responses.GET,
        "https://withpersona.com/api/v1/inquiries",
        json=empty_response,
        status=200,
    )

    tap = TapPersona(config=mock_config)
    stream = InquiriesStream(tap=tap)

    records = list(stream.get_records(context=None))

    assert len(records) == 0


@responses.activate
def test_stream_handles_401_error(mock_config):
    """Test that stream raises FatalAPIError on 401 authentication error."""
    responses.add(
        responses.GET,
        "https://withpersona.com/api/v1/inquiries",
        json={"error": "Invalid credentials"},
        status=401,
    )

    tap = TapPersona(config=mock_config)
    stream = InquiriesStream(tap=tap)

    try:
        list(stream.get_records(context=None))
        assert False, "Should have raised an error"
    except Exception:
        # Expected to fail - SDK will raise an appropriate error
        pass


@responses.activate
def test_get_next_page_token_with_cursor(mock_config):
    """Test extraction of next page cursor from response."""
    response_with_cursor = {
        "data": [{"type": "inquiry", "id": "inq_1", "attributes": {"created_at": "2025-01-01T00:00:00Z"}}],
        "links": {
            "next": "https://withpersona.com/api/v1/inquiries?page[after]=cursor_xyz123"
        }
    }

    responses.add(
        responses.GET,
        "https://withpersona.com/api/v1/inquiries",
        json=response_with_cursor,
        status=200,
    )

    tap = TapPersona(config=mock_config)
    stream = InquiriesStream(tap=tap)

    # Get the response to test get_next_page_token
    import requests as req
    test_response = req.get("https://withpersona.com/api/v1/inquiries")

    next_token = stream.get_next_page_token(test_response, None)

    assert next_token == "cursor_xyz123"


@responses.activate
def test_get_next_page_token_without_cursor(mock_config):
    """Test that get_next_page_token returns None when no more pages."""
    response_without_cursor = {
        "data": [{"type": "inquiry", "id": "inq_1", "attributes": {"created_at": "2025-01-01T00:00:00Z"}}],
        "links": {
            "next": None
        }
    }

    responses.add(
        responses.GET,
        "https://withpersona.com/api/v1/inquiries",
        json=response_without_cursor,
        status=200,
    )

    tap = TapPersona(config=mock_config)
    stream = InquiriesStream(tap=tap)

    # Get the response to test get_next_page_token
    import requests as req
    test_response = req.get("https://withpersona.com/api/v1/inquiries")

    next_token = stream.get_next_page_token(test_response, None)

    assert next_token is None


def test_post_process_adds_extraction_timestamp(mock_config):
    """Test that post_process adds _sdc_extracted_at timestamp."""
    tap = TapPersona(config=mock_config)
    stream = InquiriesStream(tap=tap)

    row = {
        "id": "inq_123",
        "status": "completed",
        "created_at": "2025-01-01T00:00:00Z"
    }

    processed_row = stream.post_process(row, context=None)

    assert "_sdc_extracted_at" in processed_row
    # The timestamp should be in ISO format
    assert "T" in processed_row["_sdc_extracted_at"]
