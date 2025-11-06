# tap-persona

Singer tap for Persona, built with the Meltano Singer SDK.

## Features

- Extracts data from Persona API (https://withpersona.com)
- Supports incremental replication on `inquiries` and `cases` streams
- Cursor-based pagination for efficient data extraction
- Configurable page sizes and start dates
- Bearer token authentication

## Installation

```bash
pip install tap-persona
```

Or with pipx:

```bash
pipx install tap-persona
```

## Configuration

### Required Settings

- `api_key` (string, secret): API key for Persona authentication

### Optional Settings

- `base_url` (string): Base API URL (default: https://withpersona.com/api/v1)
- `start_date` (datetime): Earliest record date to sync (for incremental replication)
- `page_size` (integer): Records per page (default: 100)

### Authentication

This tap uses Bearer token authentication. You'll need to obtain an API key from your Persona dashboard.

Required permissions:
- Read access to Inquiries
- Read access to Cases

### Configuration Example

```json
{
    "api_key": "your_api_key_here",
    "start_date": "2025-01-01T00:00:00Z",
    "page_size": 100
}
```

## Streams

| Stream | Replication Method | Primary Key | Replication Key |
|--------|-------------------|-------------|-----------------|
| inquiries | Incremental | id | updated_at |
| cases | Incremental | id | updated_at |

### Inquiries Stream

Extracts inquiry records from Persona. An inquiry represents a single instance of an individual attempting to verify their identity.

**Incremental Replication:** Uses `updated_at` as the replication key, ensuring that any modifications to existing inquiries are captured in subsequent syncs.

**Key fields:**
- `id`: Unique inquiry identifier
- `status`: Inquiry status (pending, completed, etc.)
- `created_at`: Timestamp when inquiry was created
- `updated_at`: Timestamp when inquiry was last updated (replication key)
- `name_first`, `name_last`: Individual's name
- `email_address`: Individual's email
- `inquiry_template_id`: Template used for the inquiry

### Cases Stream

Extracts case records from Persona. Cases are used for review and decision-making workflows.

**Incremental Replication:** Uses `updated_at` as the replication key, ensuring that any modifications to existing cases are captured in subsequent syncs.

**Key fields:**
- `id`: Unique case identifier
- `status`: Case status (open, resolved, etc.)
- `name`: Case name
- `created_at`: Timestamp when case was created
- `updated_at`: Timestamp when case was last updated (replication key)
- `assignee_id`: ID of user assigned to the case
- `case_template_id`: Template used for the case

## Usage

### With Meltano

Add to your `meltano.yml`:

```yaml
plugins:
  extractors:
    - name: tap-persona
      pip_url: tap-persona
      config:
        api_key: ${TAP_PERSONA_API_KEY}
        start_date: "2025-01-01T00:00:00Z"
```

Run extraction:

```bash
meltano run tap-persona target-jsonl
```

### Standalone

```bash
tap-persona --config config.json | target-jsonl
```

Or using environment variables:

```bash
export TAP_PERSONA_API_KEY="your_api_key"
tap-persona --config config.json --discover > catalog.json
tap-persona --config config.json --catalog catalog.json | target-jsonl
```

## Development

### Prerequisites

- Python 3.9+
- Poetry

### Setup

```bash
# Clone repository
cd tap-persona

# Install dependencies
poetry install

# Run tests
poetry run pytest

# Run with coverage
poetry run pytest --cov=tap_persona
```

### Testing

```bash
# Run all tests
poetry run pytest

# Run with coverage report
poetry run pytest --cov=tap_persona --cov-report=html

# Run specific test file
poetry run pytest tests/test_streams.py

# Run specific test
poetry run pytest tests/test_streams.py::test_inquiries_stream_schema
```

### Code Quality

```bash
# Format code with black
poetry run black tap_persona tests

# Lint with flake8
poetry run flake8 tap_persona tests
```

## Known Limitations

- API rate limits may apply (check Persona documentation for current limits)
- The tap uses cursor-based pagination as provided by the Persona API
- Incremental replication is based on `updated_at` timestamp (captures both new and modified records)
- Historical data availability depends on your Persona account settings

## Architecture

This tap follows the Blueprint Data tap development guidelines:

- Built with Meltano Singer SDK (v0.48+)
- RESTStream base class for API interactions
- Cursor-based pagination support
- Incremental replication with state management
- Comprehensive test coverage with pytest and responses
- Type-safe configuration with PropertiesList

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for version history.

## License

Apache 2.0 - See [LICENSE](LICENSE) for details.

## Support

- Report issues: https://github.com/blueprintdata/tap-persona/issues
- Persona API documentation: https://docs.withpersona.com

## Contributing

Contributions are welcome! Please follow these guidelines:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add/update tests as needed
5. Ensure all tests pass
6. Submit a pull request

## Acknowledgments

Built with the [Meltano Singer SDK](https://sdk.meltano.com).

Developed by Blueprint Data following our [tap development guidelines](../TAP_DEVELOPMENT_GUIDELINES.md).
