"""Stream definitions for tap-persona."""

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional
from urllib.parse import parse_qs, urlparse

import requests
from singer_sdk import typing as th
from singer_sdk.pagination import BaseAPIPaginator
from singer_sdk.streams import RESTStream


class PersonaPaginator(BaseAPIPaginator):
    """Paginator for Persona API cursor-based pagination.

    Persona API uses cursor-based pagination with a 'next' link in the response
    that contains a 'page[after]' query parameter with the cursor token.
    """

    def __init__(self, *args, **kwargs):
        """Initialize paginator."""
        super().__init__(None, *args, **kwargs)

    def get_next(self, response: requests.Response) -> Optional[str]:
        """Extract next page cursor from response.

        Args:
            response: HTTP response from API.

        Returns:
            Next page cursor string, or None if no more pages.
        """
        data = response.json()

        # Persona uses cursor-based pagination in the 'links' object
        links = data.get("links", {})
        next_url = links.get("next")

        if next_url:
            # Extract the cursor from the next URL
            # The cursor is in the 'page[after]' query parameter
            parsed = urlparse(next_url)
            query_params = parse_qs(parsed.query)

            # Get the 'page[after]' parameter
            after_param = query_params.get("page[after]", [None])[0]
            return after_param

        return None


class PersonaStream(RESTStream):
    """Base stream for Persona API."""

    @property
    def url_base(self) -> str:
        """Return base URL from config."""
        return self.config.get("base_url", "https://withpersona.com/api/v1")

    @property
    def http_headers(self) -> dict:
        """Return authentication headers.

        Returns:
            Dictionary with Bearer token authentication header.
        """
        return {
            "Authorization": f"Bearer {self.config['api_key']}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def get_new_paginator(self) -> PersonaPaginator:
        """Return a new paginator instance.

        Returns:
            PersonaPaginator instance for handling cursor-based pagination.
        """
        return PersonaPaginator()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[str]
    ) -> Dict[str, Any]:
        """Build query parameters for API request.

        Persona API uses bracket notation for nested parameters (JSON:API standard).
        For example: page[size]=100, page[after]=cursor123

        Args:
            context: Stream context (includes state for incremental sync).
            next_page_token: Cursor token from previous response for pagination.

        Returns:
            Dictionary of query parameters for the API request.
        """
        params: Dict[str, Any] = {}

        # Add page size using bracket notation (JSON:API standard)
        params["page[size]"] = self.config.get("page_size", 100)

        # Add cursor for pagination using bracket notation
        if next_page_token:
            params["page[after]"] = next_page_token

        # Add sort order for proper incremental replication
        # Sort by replication key in ascending order (oldest to newest)
        # This ensures the bookmark is always the most recent timestamp processed
        if self.replication_key:
            # Convert underscore to hyphen for API (updated_at → updated-at)
            api_field_name = self.replication_key.replace("_", "-")
            params["sort"] = api_field_name

        # Add incremental sync filter using bracket notation
        if self.replication_key:
            start_value = self.get_starting_replication_key_value(context)
            if start_value:
                # Filter for records created/updated after the bookmark
                # Convert underscore to hyphen for API (created_at → created-at)
                api_field_name = self.replication_key.replace("_", "-")
                params[f"filter[{api_field_name}][start]"] = start_value

        return params

    def _normalize_field_name(self, field_name: str) -> str:
        """Convert hyphenated field names to underscored (Persona API convention).

        Args:
            field_name: Field name from API (e.g., 'created-at')

        Returns:
            Normalized field name (e.g., 'created_at')
        """
        return field_name.replace("-", "_")

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse API response and yield records.

        Persona API uses hyphenated field names (JSON:API convention).
        We normalize them to underscored names for consistency.

        Args:
            response: HTTP response from API.

        Yields:
            Individual record dictionaries from the response.
        """
        data = response.json()

        # Persona API returns data in a 'data' array
        records = data.get("data", [])

        for record in records:
            # Flatten the structure: combine id, type, and attributes
            flattened_record = {
                "id": record.get("id"),
                "type": record.get("type"),
            }

            # Add all attributes to the root level, normalizing field names
            attributes = record.get("attributes", {})
            for key, value in attributes.items():
                normalized_key = self._normalize_field_name(key)
                flattened_record[normalized_key] = value

            # Include relationships if present
            if "relationships" in record:
                flattened_record["relationships"] = record["relationships"]

            yield flattened_record

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        """Transform record before output.

        Args:
            row: Record dictionary.
            context: Stream context.

        Returns:
            Processed record dictionary.
        """
        # Call parent post_process to ensure SDK's default behavior
        row = super().post_process(row, context)

        # Add extraction timestamp
        if row is not None:
            row["_sdc_extracted_at"] = datetime.now(timezone.utc).isoformat()

        return row


class InquiriesStream(PersonaStream):
    """Stream for Persona Inquiries.

    Extracts inquiry records with support for incremental replication
    based on the updated_at timestamp field.

    API Endpoint: GET /inquiries
    """

    name = "inquiries"
    path = "/inquiries"
    primary_keys = ["id"]
    replication_key = "updated_at"

    schema = th.PropertiesList(
        # Core fields
        th.Property("id", th.StringType, required=True, description="Inquiry ID"),
        th.Property("type", th.StringType, description="Resource type"),

        # Timestamps for incremental sync
        th.Property(
            "created_at",
            th.DateTimeType,
            description="Timestamp when the inquiry was created",
        ),
        th.Property(
            "updated_at",
            th.DateTimeType,
            description="Timestamp when the inquiry was last updated",
        ),
        th.Property(
            "completed_at",
            th.DateTimeType,
            description="Timestamp when the inquiry was completed",
        ),

        # Inquiry attributes
        th.Property("status", th.StringType, description="Inquiry status"),
        th.Property("reference_id", th.StringType, description="Reference ID"),
        th.Property(
            "inquiry_template_id",
            th.StringType,
            description="ID of the inquiry template",
        ),
        th.Property(
            "inquiry_template_version_id",
            th.StringType,
            description="Version ID of the inquiry template",
        ),
        th.Property("name_first", th.StringType, description="First name"),
        th.Property("name_middle", th.StringType, description="Middle name"),
        th.Property("name_last", th.StringType, description="Last name"),
        th.Property("email_address", th.StringType, description="Email address"),
        th.Property("phone_number", th.StringType, description="Phone number"),
        th.Property("address_street_1", th.StringType, description="Street address line 1"),
        th.Property("address_street_2", th.StringType, description="Street address line 2"),
        th.Property("address_city", th.StringType, description="City"),
        th.Property("address_subdivision", th.StringType, description="State/Province"),
        th.Property("address_postal_code", th.StringType, description="Postal code"),
        th.Property("birthdate", th.DateType, description="Date of birth"),

        # Relationships and metadata
        th.Property(
            "relationships",
            th.ObjectType(),
            description="Related resources (verifications, reports, etc.)",
        ),

        # Extraction metadata
        th.Property(
            "_sdc_extracted_at",
            th.DateTimeType,
            description="Timestamp when the record was extracted",
        ),
    ).to_dict()

    # Allow additional properties since the API may return extra fields
    schema["additionalProperties"] = True


class CasesStream(PersonaStream):
    """Stream for Persona Cases.

    Extracts case records with support for incremental replication
    based on the updated_at timestamp field.

    API Endpoint: GET /cases
    """

    name = "cases"
    path = "/cases"
    primary_keys = ["id"]
    replication_key = "updated_at"

    schema = th.PropertiesList(
        # Core fields
        th.Property("id", th.StringType, required=True, description="Case ID"),
        th.Property("type", th.StringType, description="Resource type"),

        # Timestamps for incremental sync
        th.Property(
            "created_at",
            th.DateTimeType,
            description="Timestamp when the case was created",
        ),
        th.Property(
            "updated_at",
            th.DateTimeType,
            description="Timestamp when the case was last updated",
        ),
        th.Property(
            "resolved_at",
            th.DateTimeType,
            description="Timestamp when the case was resolved",
        ),

        # Case attributes
        th.Property("status", th.StringType, description="Case status"),
        th.Property("name", th.StringType, description="Case name"),
        th.Property("assignee_id", th.StringType, description="ID of assigned user"),
        th.Property("resolution", th.StringType, description="Case resolution"),
        th.Property(
            "case_template_id",
            th.StringType,
            description="ID of the case template",
        ),
        th.Property(
            "case_template_version_id",
            th.StringType,
            description="Version ID of the case template",
        ),

        # Relationships and metadata
        th.Property(
            "relationships",
            th.ObjectType(),
            description="Related resources",
        ),

        # Extraction metadata
        th.Property(
            "_sdc_extracted_at",
            th.DateTimeType,
            description="Timestamp when the record was extracted",
        ),
    ).to_dict()

    # Allow additional properties since the API may return extra fields
    schema["additionalProperties"] = True
