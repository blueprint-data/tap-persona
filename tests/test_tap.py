"""Tests for tap-persona Tap class."""

import pytest

from tap_persona.tap import TapPersona


def test_tap_initialization_with_valid_config(mock_config):
    """Test that tap initializes correctly with valid config."""
    tap = TapPersona(config=mock_config)

    assert tap.config["api_key"] == "test_api_key_12345"
    assert tap.config["base_url"] == "https://withpersona.com/api/v1"
    assert tap.config["page_size"] == 100


def test_tap_initialization_missing_api_key():
    """Test that tap fails when API key is missing."""
    invalid_config = {
        "base_url": "https://withpersona.com/api/v1",
    }

    with pytest.raises(Exception):
        TapPersona(config=invalid_config)


def test_tap_uses_default_base_url():
    """Test that tap uses default base URL when not provided."""
    minimal_config = {
        "api_key": "test_api_key",
    }

    tap = TapPersona(config=minimal_config)

    assert tap.config.get("base_url", "https://withpersona.com/api/v1") == "https://withpersona.com/api/v1"


def test_stream_discovery(mock_config):
    """Test that tap discovers all expected streams."""
    tap = TapPersona(config=mock_config)
    streams = tap.discover_streams()

    assert len(streams) == 2

    stream_names = [s.name for s in streams]
    assert "inquiries" in stream_names
    assert "cases" in stream_names


def test_stream_discovery_returns_correct_types(mock_config):
    """Test that discovered streams have correct types."""
    from tap_persona.streams import CasesStream, InquiriesStream

    tap = TapPersona(config=mock_config)
    streams = tap.discover_streams()

    inquiries_stream = next((s for s in streams if s.name == "inquiries"), None)
    cases_stream = next((s for s in streams if s.name == "cases"), None)

    assert inquiries_stream is not None
    assert isinstance(inquiries_stream, InquiriesStream)

    assert cases_stream is not None
    assert isinstance(cases_stream, CasesStream)


def test_config_schema_includes_required_fields():
    """Test that config schema includes all required fields."""
    schema = TapPersona.config_jsonschema

    assert "properties" in schema
    properties = schema["properties"]

    # Check required field
    assert "api_key" in properties
    # Type can be a string or list of strings in JSONSchema
    api_key_type = properties["api_key"]["type"]
    assert api_key_type == "string" or api_key_type == ["string"]
    assert properties["api_key"].get("secret") is True

    # Check optional fields
    assert "base_url" in properties
    assert "start_date" in properties
    assert "page_size" in properties


def test_config_schema_default_values():
    """Test that config schema has correct default values."""
    schema = TapPersona.config_jsonschema
    properties = schema["properties"]

    assert properties["base_url"]["default"] == "https://withpersona.com/api/v1"
    assert properties["page_size"]["default"] == 100
