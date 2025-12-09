import requests
import json

# Test 1: Normal transaction
print("\n=== TEST 1: Normal Transaction ===")
data1 = {
    "transaction_id": "TXN001",
    "customer_id": "106149964",
    "date_of_birth": "1985-05-15",
    "gender": "F",
    "transaction_amount": "2500",
    "transaction_date": "2025-12-08T14:30:00",
    "transaction_type": "PAY_TO_COOPTILL",
    "channelId": "2E08D0409BE26BA3E064020820E70A68",
    "relationship_opening_date": "2018-01-15",
    "risk_rating": "LOW",
    "customer_status": "ACTVE"
}

response = requests.post("http://localhost:8000/fraud/predict", json=data1)
print(json.dumps(response.json(), indent=2))

# Test 2: Suspicious transaction
print("\n=== TEST 2: Suspicious - Large Amount, Night ===")
data2 = {
    "transaction_id": "TXN002",
    "customer_id": "NEW123",
    "date_of_birth": "2005-01-01",
    "gender": "M",
    "transaction_amount": "250000",
    "transaction_date": "2025-12-08T02:30:00",
    "transaction_type": "PAY_TO_COOPTILL",
    "channelId": "2E08D0409BE16BA3E064020820E70A68",
    "relationship_opening_date": "2025-11-01",
    "risk_rating": "HIGH",
    "customer_status": "ACTVE"
}

response = requests.post("http://localhost:8000/fraud/predict", json=data2)
print(json.dumps(response.json(), indent=2))

# Test 3: Balance check (should be safe)
print("\n=== TEST 3: Balance Inquiry ===")
data3 = {
    "transaction_id": "TXN003",
    "customer_id": "106149964",
    "date_of_birth": "1990-06-15",
    "gender": "F",
    "transaction_amount": "0",
    "transaction_date": "2025-12-08T10:15:00",
    "transaction_type": "BALANCEENQUIRY",
    "channelId": "2E08D0409BE26BA3E064020820E70A68",
    "relationship_opening_date": "2019-03-20",
    "risk_rating": "LOW",
    "customer_status": "ACTVE"
}

response = requests.post("http://localhost:8000/fraud/predict", json=data3)
print(json.dumps(response.json(), indent=2))