"""Basic usage examples for the Lumenqraph Python SDK."""

import asyncio
from lumenqraph import LumenqraphClient


async def main():
    """Demonstrate basic SDK usage."""
    
    # Initialize client
    async with LumenqraphClient(base_url="http://localhost:8080") as client:
        
        # 1. List all contracts
        print("Fetching contracts...")
        contracts = await client.list_contracts()
        print(f"Found {len(contracts)} contracts")
        
        if not contracts:
            print("No contracts found. Make sure the indexer is running and has indexed some contracts.")
            return
        
        contract_id = contracts[0]["contract_id"]
        print(f"\nWorking with contract: {contract_id}")
        
        # 2. Get contract interface
        print("\nFetching contract interface...")
        interface = await client.get_interface(contract_id)
        print(f"Interface: {interface}")
        
        # 3. Get recent events
        print("\nFetching recent events...")
        events = await client.list_events(contract_id, limit=5)
        print(f"Recent events: {len(events)}")
        for event in events:
            print(f"  - {event['event_name']}: {event.get('enriched', event['decoded_value'])}")
        
        # 4. Paginate through all events
        print("\nPaginating through first 10 events...")
        count = 0
        async for event in client.paginate_events(contract_id, page_size=5):
            print(f"  Event {count + 1}: {event['event_name']}")
            count += 1
            if count >= 10:
                break
        
        # 5. Get contract state
        print("\nFetching contract state...")
        state = await client.get_state(contract_id, limit=1)
        print(f"State versions: {state['count']}")
        if state['versions']:
            latest = state['versions'][0]
            print(f"Latest state at ledger {latest['ledger']}")
        
        # 6. Get contract data keys
        print("\nFetching contract data keys...")
        data = await client.get_data(contract_id, limit=5)
        print(f"Data keys: {data['count']}")
        for key in data['keys'][:3]:
            print(f"  - {key.get('label', 'unlabeled')}: {key['value']}")
        
        # 7. List transfers
        print("\nFetching transfers...")
        transfers = await client.list_transfers(contract_id, limit=5)
        print(f"Recent transfers: {len(transfers)}")
        for transfer in transfers:
            print(f"  - {transfer['amount']} from {transfer.get('from_addr', 'null')} to {transfer.get('to_addr', 'null')}")
        
        # 8. Health check
        print("\nChecking API health...")
        health = await client.health()
        print(f"Health: {health}")


if __name__ == "__main__":
    asyncio.run(main())
