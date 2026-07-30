"""Example webhook server with signature verification."""

import os
from fastapi import FastAPI, Request, HTTPException
from lumenqraph import verify_webhook


app = FastAPI()


@app.post("/webhook")
async def handle_webhook(request: Request):
    """
    Handle incoming Lumenqraph webhooks with signature verification.
    
    Environment variables:
        WEBHOOK_SECRET: The webhook secret from Lumenqraph subscription
    """
    # Get the raw body (important: must be raw for signature verification)
    raw_body = await request.body()
    
    # Get the signature header
    signature = request.headers.get("x-lumenqraph-signature", "")
    if not signature:
        raise HTTPException(status_code=401, detail="Missing signature header")
    
    # Get the webhook secret from environment
    secret = os.environ.get("WEBHOOK_SECRET")
    if not secret:
        raise HTTPException(
            status_code=500,
            detail="Webhook secret not configured"
        )
    
    # Verify the signature
    is_valid = await verify_webhook(raw_body, signature, secret)
    if not is_valid:
        raise HTTPException(status_code=401, detail="Invalid signature")
    
    # Parse and process the webhook payload
    import json
    payload = json.loads(raw_body)
    
    # Process the webhook based on event type
    event_type = payload.get("event_type")
    print(f"Received webhook: {event_type}")
    
    if event_type == "contract.event":
        # Handle contract event
        contract_id = payload.get("contract_id")
        event_name = payload.get("event_name")
        print(f"Contract {contract_id} emitted event: {event_name}")
    
    elif event_type == "contract.state_change":
        # Handle state change
        contract_id = payload.get("contract_id")
        ledger = payload.get("ledger")
        print(f"Contract {contract_id} state changed at ledger {ledger}")
    
    # Return success
    return {"status": "received", "event_type": event_type}


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn
    
    # Make sure webhook secret is set
    if not os.environ.get("WEBHOOK_SECRET"):
        print("Warning: WEBHOOK_SECRET environment variable not set!")
        print("Set it before running: export WEBHOOK_SECRET=your-secret")
    
    # Run the server
    uvicorn.run(app, host="0.0.0.0", port=8000)
