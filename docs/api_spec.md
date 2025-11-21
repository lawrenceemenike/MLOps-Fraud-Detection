# Scorer Service API

Base URL: `http://localhost:8000`

## Endpoints

### `POST /predict`

Score a single transaction.

**Request Body**
```json
{
  "transaction_id": "string",
  "user_id": "string",
  "amount": 0,
  "merchant_id": "string",
  "timestamp": "string"
}
```

**Response**
```json
{
  "is_fraud": boolean,
  "score": float
}
```

### `GET /metrics`

Prometheus metrics endpoint.

### `GET /health`

Health check endpoint (if implemented).
