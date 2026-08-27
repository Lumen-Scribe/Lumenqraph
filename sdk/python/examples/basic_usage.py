#!/usr/bin/env python3
"""Basic usage examples for the Lumenqraph Python SDK."""

from lumenqraph import LumenqraphClient, LumenqraphError


def example_list_contracts():
    """Example: List all contracts."""
    lq = LumenqraphClient(base_url="http://localhost:8080")

    # Get contracts with pagination
    response = lq.list_contracts(limit=10)
    contracts = response['data']

    print(f"Found {len(contracts)} contracts")
    for contract in contracts:
        print(f"  - {contract['contract_id']}: {contract['event_count']} events")

    if response['has_more']:
        print("  ... and more")


def example_paginate_events():
    """Example: Iterate through all events for a contract."""
    lq = LumenqraphClient(base_url="http://localhost:8080")

    # This would need a real contract ID
    contract_id = "CABC..."

    try:
        count = 0
        for event in lq.paginate_events(contract_id):
            print(f"Event: {event['event_name']}")
            count += 1
            if count >= 10:  # Just show first 10
                print("  ...")
                break
    except LumenqraphError as e:
        print(f"API error: {e}")


def example_call_contract():
    """Example: Call a contract's view function."""
    lq = LumenqraphClient(base_url="http://localhost:8080")

    contract_id = "CABC..."

    try:
        # Get the contract interface
        interface = lq.get_interface(contract_id)
        print(f"Contract functions: {list(interface['interface']['functions'].keys())}")

        # Call a function
        result = lq.call(contract_id, function="get_balance", args=["GXYZ..."])
        print(f"Result: {result['result']}")
    except LumenqraphError as e:
        print(f"API error: {e.status}: {e.body}")


def example_webhook_verification():
    """Example: Verify webhook signature."""
    from lumenqraph import verify_webhook_signature
    import json

    # Simulate receiving a webhook
    body_str = json.dumps({"event": "transfer", "amount": "1000"})
    secret = "your-webhook-secret"

    # In a real app, this would come from the X-Webhook-Signature header
    import hmac
    import hashlib
    signature = hmac.new(
        secret.encode("utf-8"),
        body_str.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()

    # Verify the signature
    if verify_webhook_signature(body_str, signature, secret):
        print("Webhook signature is valid")
        data = json.loads(body_str)
        print(f"Event: {data['event']}")
    else:
        print("Invalid webhook signature!")


if __name__ == "__main__":
    print("=== List Contracts ===")
    example_list_contracts()

    print("\n=== Paginate Events ===")
    example_paginate_events()

    print("\n=== Call Contract ===")
    example_call_contract()

    print("\n=== Webhook Verification ===")
    example_webhook_verification()
