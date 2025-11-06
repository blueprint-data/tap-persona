"""Persona tap class."""

from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th

from tap_persona.streams import CasesStream, InquiriesStream


class TapPersona(Tap):
    """Singer tap for Persona."""

    name = "tap-persona"

    config_jsonschema = th.PropertiesList(
        # Required authentication
        th.Property(
            "api_key",
            th.StringType,
            required=True,
            secret=True,
            description="API key for Persona authentication",
        ),
        # Optional configuration
        th.Property(
            "base_url",
            th.StringType,
            default="https://withpersona.com/api/v1",
            description="Base API URL for Persona",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="Earliest record date to sync (for incremental replication)",
        ),
        th.Property(
            "page_size",
            th.IntegerType,
            default=100,
            description="Number of records to fetch per page (default: 100)",
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams.

        Returns:
            A list of Persona streams (Inquiries and Cases).
        """
        return [
            InquiriesStream(self),
            CasesStream(self),
        ]


if __name__ == "__main__":
    TapPersona.cli()
