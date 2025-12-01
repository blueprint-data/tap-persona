#!/usr/bin/env python3
"""Simple script to verify state emission locally.

This script runs the tap with mocked API responses and verifies that:
1. The state includes standard replication_key_value
2. The state includes custom earliest_incomplete_* fields
3. Both inquiries and cases streams work correctly

Run this anytime to verify state persistence works as expected.
"""
import json
import sys
from io import StringIO
import responses
from tap_persona.tap import TapPersona


@responses.activate
def verify_state():
    """Run the tap and verify state emission."""
    # Mock inquiries API
    responses.add(
        responses.GET,
        "https://withpersona.com/api/v1/inquiries",
        json={
            "data": [
                {
                    "type": "inquiry",
                    "id": "inq_created_1",
                    "attributes": {
                        "status": "created",
                        "created-at": "2024-12-15T08:00:00Z",
                        "updated-at": "2025-01-15T12:00:00Z",
                    }
                },
                {
                    "type": "inquiry",
                    "id": "inq_completed_1",
                    "attributes": {
                        "status": "completed",
                        "created-at": "2025-01-01T10:00:00Z",
                        "updated-at": "2025-01-10T15:00:00Z",
                    }
                }
            ],
            "links": {"next": None}
        },
        status=200,
    )

    # Mock cases API
    responses.add(
        responses.GET,
        "https://withpersona.com/api/v1/cases",
        json={
            "data": [
                {
                    "type": "case",
                    "id": "case_open_1",
                    "attributes": {
                        "status": "Open",
                        "created-at": "2024-11-10T09:00:00Z",
                        "updated-at": "2025-01-12T14:00:00Z",
                    }
                },
                {
                    "type": "case",
                    "id": "case_resolved_1",
                    "attributes": {
                        "status": "Resolved",
                        "created-at": "2025-01-05T11:00:00Z",
                        "updated-at": "2025-01-18T16:00:00Z",
                    }
                }
            ],
            "links": {"next": None}
        },
        status=200,
    )

    # Capture stdout
    captured_output = StringIO()
    original_stdout = sys.stdout
    sys.stdout = captured_output

    try:
        tap = TapPersona(config={"api_key": "test_key"})
        tap.sync_all()
    finally:
        sys.stdout = original_stdout

    # Parse Singer messages
    output = captured_output.getvalue()
    messages = [json.loads(line) for line in output.strip().split('\n') if line]

    # Get final state
    state_messages = [msg for msg in messages if msg.get('type') == 'STATE']
    if not state_messages:
        print("❌ ERROR: No STATE messages found!")
        return False

    final_state = state_messages[-1].get('value', {})
    bookmarks = final_state.get('bookmarks', {})

    # Verify state
    print("\n" + "="*80)
    print("STATE VERIFICATION")
    print("="*80)

    success = True
    for stream_name in ['inquiries', 'cases']:
        stream_state = bookmarks.get(stream_name, {})
        has_rk = 'replication_key_value' in stream_state
        has_custom = 'earliest_incomplete_id' in stream_state

        print(f"\n{stream_name.upper()}:")
        print(f"  ✓ replication_key_value: {has_rk}")
        if has_rk:
            print(f"    → {stream_state['replication_key_value']}")

        print(f"  ✓ earliest_incomplete_id: {has_custom}")
        if has_custom:
            print(f"    → ID: {stream_state['earliest_incomplete_id']}")
            print(f"    → Status: {stream_state['earliest_incomplete_status']}")
            print(f"    → Created: {stream_state['earliest_incomplete_created_at']}")

        if not (has_rk and has_custom):
            success = False

    print("\n" + "="*80)
    if success:
        print("✅ SUCCESS: State includes all required fields!")
    else:
        print("❌ FAILED: State missing required fields")
    print("="*80)
    print("\nFull state:")
    print(json.dumps(final_state, indent=2))
    print()

    return success


if __name__ == "__main__":
    success = verify_state()
    sys.exit(0 if success else 1)
