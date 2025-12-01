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
    _boundary_reached = False

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
        # If boundary was reached, stop pagination
        if self._boundary_reached:
            return None

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
    

    # Set the boundary reached flag to stop pagination
    def set_boundary_reached(self) -> None:
        """Set the boundary reached flag."""
        self._boundary_reached = True


class PersonaStream(RESTStream):
    """Base stream for Persona API."""

    # Subclasses should override these to define incomplete statuses
    incomplete_statuses = []
    # Track the boundary ID to stop pagination when encountered
    _pagination_boundary_id = None
    # Store paginator instance to control pagination
    _paginator_instance = None

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
        self._paginator_instance = PersonaPaginator()
        return self._paginator_instance

    def get_starting_incomplete_id(self, context: Optional[dict]) -> Optional[str]:
        """Get the ID boundary for incremental sync.

        Checks in order:
        1. State: earliest_incomplete_id from previous run
        2. Config: stream-specific start ID from nested config (e.g., inquiries.start_id)

        Args:
            context: Stream context containing state information.

        Returns:
            ID to use as pagination boundary, or None for full sync.
        """
        state = self.get_context_state(context)
        state_id = state.get("earliest_incomplete_id")

        if state_id:
            return state_id

        # On initial run, check config for starting ID in nested structure
        # Look for config like: {"inquiries": {"start_id": "inq_123"}}
        stream_config = self.config.get(self.name, {})
        if isinstance(stream_config, dict):
            return stream_config.get("start_id")

        return None

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[str]
    ) -> Dict[str, Any]:
        """Build query parameters for API request.

        Persona API uses bracket notation for nested parameters (JSON:API standard).
        For example: page[size]=100, page[after]=cursor123

        Implements ID-based incremental sync strategy:
        - Tracks the earliest incomplete record ID in state
        - Uses page[before] to fetch records appearing before (newer than) that boundary
        - Continues pagination until the boundary ID is encountered in results
        - Avoids full refreshes while ensuring incomplete records are re-checked

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
        else:
            # On initial page, use ID boundary for incremental sync
            # Store boundary ID to stop pagination when we encounter it
            boundary_id = self.get_starting_incomplete_id(context)
            if boundary_id:
                self._pagination_boundary_id = boundary_id
                # Use page[before] to fetch records before this ID in reverse-chronological list
                # (i.e., newer records since API returns newest first)
                params["page[before]"] = boundary_id
                self.logger.info(
                    f"Using ID boundary for incremental sync: {boundary_id}"
                )

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

        Since Persona API returns records in reverse chronological order
        and doesn't support ascending sort, we collect and sort records
        locally to ensure proper incremental replication.

        During incremental syncs, includes records up to and including the
        pagination boundary ID, then stops to prevent infinite pagination.
        The boundary record is included to capture any updates to it.

        Note: The 'include' parameter is not supported on list endpoints.
        Related resources are available via the 'relationships' field (IDs only).

        Args:
            response: HTTP response from API.

        Yields:
            Individual record dictionaries from the response, sorted by replication key.
        """
        data = response.json()

        # Persona API returns data in a 'data' array
        records = data.get("data", [])

        flattened_records = []
        boundary_reached = False

        for record in records:
            record_id = record.get("id")

            # Flatten the structure: combine id, type, and attributes
            flattened_record = {
                "id": record_id,
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

            flattened_records.append(flattened_record)

            # Check if we've reached the pagination boundary AFTER adding the record
            # This ensures we include the boundary record itself (to get its latest state)
            if self._pagination_boundary_id and record_id == self._pagination_boundary_id:
                self.logger.info(
                    f"Reached pagination boundary: {record_id}. Including boundary record and stopping pagination."
                )
                boundary_reached = True
                break

        # If boundary reached, signal the paginator to stop
        if boundary_reached and self._paginator_instance:
            self.logger.info("Boundary reached, signaling paginator to stop")
            self.logger.info(f"Boundary ID: {self._pagination_boundary_id}")
            self._paginator_instance.set_boundary_reached()

        # Sort records by replication key in ascending order (oldest to newest)
        # This ensures proper bookmark progression for incremental sync
        if self.replication_key and flattened_records:
            flattened_records.sort(
                key=lambda x: x.get(self.replication_key) or "",
                reverse=False
            )

        # Yield sorted records
        for flattened_record in flattened_records:
            yield flattened_record

    def _finalize_state(self, state: Optional[dict] = None) -> None:
        """Finalize state while preserving custom fields.

        The SDK's default finalization only keeps replication_key and replication_key_value.
        We need to preserve our custom earliest_incomplete_* fields.

        Args:
            state: State object to finalize.
        """
        # Preserve custom fields before parent finalization
        if state:
            custom_fields = {
                k: v for k, v in state.items()
                if k.startswith("earliest_incomplete_")
            }
        else:
            custom_fields = {}

        # Let parent finalize (this will keep only replication_key fields)
        super()._finalize_state(state)

        # Restore custom fields after finalization
        if state and custom_fields:
            state.update(custom_fields)

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        """Transform record before output.

        Also tracks the earliest incomplete record for ID-based incremental sync.

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

            # Track earliest (oldest) incomplete record for ID-based incremental sync
            if row.get("status") in self.incomplete_statuses:
                state = self.get_context_state(context)
                current_earliest_id = state.get("earliest_incomplete_id")
                current_earliest_timestamp = state.get("earliest_incomplete_created_at")

                # Use created_at to determine which record is older
                # Store the ID for pagination purposes
                row_timestamp = row.get("created_at")

                if row_timestamp:
                    # Update if this is the first incomplete record or if it's older
                    should_update = (
                        not current_earliest_timestamp or
                        row_timestamp < current_earliest_timestamp
                    )

                    if should_update:
                        state["earliest_incomplete_id"] = row["id"]
                        state["earliest_incomplete_status"] = row.get("status")
                        state["earliest_incomplete_created_at"] = row_timestamp
                        state["earliest_incomplete_updated_at"] = row.get("updated_at")
                        self.logger.info(
                            f"Updating earliest incomplete: {row['id']} "
                            f"(created: {row_timestamp}, status: {row.get('status')})"
                        )

        return row


class InquiriesStream(PersonaStream):
    """Stream for Persona Inquiries.

    Extracts inquiry records with support for incremental replication
    based on the updated_at timestamp field and earliest incomplete inquiry tracking.

    For incremental sync:
    - Tracks the earliest inquiry with "created" status
    - Subsequent runs fetch only records newer than this boundary
    - Ensures we always re-check incomplete inquiries while avoiding full refreshes

    API Endpoint: GET /inquiries
    """

    name = "inquiries"
    path = "/inquiries"
    primary_keys = ["id"]
    replication_key = "updated_at"

    # Incomplete statuses that should be re-checked on each sync
    # "created" = inquiry started but not yet completed
    incomplete_statuses = ["created", "pending", "needs_review"]

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
        th.Property(
            "tags",
            th.ArrayType(th.StringType),
            description="Custom string labels applied to the inquiry",
        ),

        # Relationships and metadata
        th.Property(
            "relationships",
            th.ObjectType(
                # Verifications relationship (array)
                th.Property("verifications", th.ObjectType(
                    th.Property("data", th.ArrayType(th.ObjectType(
                        th.Property("type", th.StringType),
                        th.Property("id", th.StringType),
                    )))
                )),
                # Reports relationship (array)
                th.Property("reports", th.ObjectType(
                    th.Property("data", th.ArrayType(th.ObjectType(
                        th.Property("type", th.StringType),
                        th.Property("id", th.StringType),
                    )))
                )),
                # Sessions relationship (array)
                th.Property("sessions", th.ObjectType(
                    th.Property("data", th.ArrayType(th.ObjectType(
                        th.Property("type", th.StringType),
                        th.Property("id", th.StringType),
                    )))
                )),
                # Documents relationship (array)
                th.Property("documents", th.ObjectType(
                    th.Property("data", th.ArrayType(th.ObjectType(
                        th.Property("type", th.StringType),
                        th.Property("id", th.StringType),
                    )))
                )),
                # Selfies relationship (array)
                th.Property("selfies", th.ObjectType(
                    th.Property("data", th.ArrayType(th.ObjectType(
                        th.Property("type", th.StringType),
                        th.Property("id", th.StringType),
                    )))
                )),
                # Inquiry Template relationship
                th.Property("inquiry-template", th.ObjectType(
                    th.Property("data", th.ObjectType(
                        th.Property("type", th.StringType),
                        th.Property("id", th.StringType),
                    ))
                )),
                # Inquiry Template Version relationship
                th.Property("inquiry-template-version", th.ObjectType(
                    th.Property("data", th.ObjectType(
                        th.Property("type", th.StringType),
                        th.Property("id", th.StringType),
                    ))
                )),
                # Account relationship
                th.Property("account", th.ObjectType(
                    th.Property("data", th.ObjectType(
                        th.Property("type", th.StringType),
                        th.Property("id", th.StringType),
                    ))
                )),
                # Reviewer relationship
                th.Property("reviewer", th.ObjectType(
                    th.Property("data", th.ObjectType(
                        th.Property("type", th.StringType),
                        th.Property("id", th.StringType),
                    ))
                )),
                # Creator relationship
                th.Property("creator", th.ObjectType(
                    th.Property("data", th.ObjectType(
                        th.Property("type", th.StringType),
                        th.Property("id", th.StringType),
                    ))
                )),
            ),
            description="Related resources with their IDs and types (JSON:API format). "
            "Each relationship contains a 'data' field with type and id information.",
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
    based on the updated_at timestamp field and earliest open case tracking.

    For incremental sync:
    - Tracks the earliest case with "open" status
    - Subsequent runs fetch only records newer than this boundary
    - Ensures we always re-check open cases while avoiding full refreshes

    API Endpoint: GET /cases
    """

    name = "cases"
    path = "/cases"
    primary_keys = ["id"]
    replication_key = "updated_at"

    # Incomplete statuses that should be re-checked on each sync
    # "Open" = case is still under review and not yet resolved
    incomplete_statuses = ["Open"]

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
        th.Property(
            "tags",
            th.ArrayType(th.StringType),
            description="Custom string labels applied to the case",
        ),

        # Relationships and metadata
        th.Property(
            "relationships",
            th.ObjectType(
                # Case Template relationship
                th.Property("case-template", th.ObjectType(
                    th.Property("data", th.ObjectType(
                        th.Property("type", th.StringType),
                        th.Property("id", th.StringType),
                    ))
                )),
                # Case Queue relationship
                th.Property("case-queue", th.ObjectType(
                    th.Property("data", th.ObjectType(
                        th.Property("type", th.StringType),
                        th.Property("id", th.StringType),
                    ))
                )),
                # Case Comments relationship (array)
                th.Property("case-comments", th.ObjectType(
                    th.Property("data", th.ArrayType(th.ObjectType(
                        th.Property("type", th.StringType),
                        th.Property("id", th.StringType),
                    )))
                )),
                # Accounts relationship (array)
                th.Property("accounts", th.ObjectType(
                    th.Property("data", th.ArrayType(th.ObjectType(
                        th.Property("type", th.StringType),
                        th.Property("id", th.StringType),
                    )))
                )),
                # Inquiries relationship (array)
                th.Property("inquiries", th.ObjectType(
                    th.Property("data", th.ArrayType(th.ObjectType(
                        th.Property("type", th.StringType),
                        th.Property("id", th.StringType),
                    )))
                )),
                # Reports relationship (array)
                th.Property("reports", th.ObjectType(
                    th.Property("data", th.ArrayType(th.ObjectType(
                        th.Property("type", th.StringType),
                        th.Property("id", th.StringType),
                    )))
                )),
                # Verifications relationship (array)
                th.Property("verifications", th.ObjectType(
                    th.Property("data", th.ArrayType(th.ObjectType(
                        th.Property("type", th.StringType),
                        th.Property("id", th.StringType),
                    )))
                )),
                # Transactions relationship (array)
                th.Property("txns", th.ObjectType(
                    th.Property("data", th.ArrayType(th.ObjectType(
                        th.Property("type", th.StringType),
                        th.Property("id", th.StringType),
                    )))
                )),
                # Creator relationship
                th.Property("creator", th.ObjectType(
                    th.Property("data", th.ObjectType(
                        th.Property("type", th.StringType),
                        th.Property("id", th.StringType),
                    ))
                )),
                # Assignee relationship
                th.Property("assignee", th.ObjectType(
                    th.Property("data", th.ObjectType(
                        th.Property("type", th.StringType),
                        th.Property("id", th.StringType),
                    ))
                )),
                # Reviewer relationship
                th.Property("reviewer", th.ObjectType(
                    th.Property("data", th.ObjectType(
                        th.Property("type", th.StringType),
                        th.Property("id", th.StringType),
                    ))
                )),
            ),
            description="Related resources with their IDs and types (JSON:API format). "
            "Each relationship contains a 'data' field with type and id information.",
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
