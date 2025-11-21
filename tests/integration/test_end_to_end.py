import json
import time
import uuid
import pytest
from kafka import KafkaProducer, KafkaConsumer

# Configuration
KAFKA_BROKER = 'localhost:9092' # External port
INPUT_TOPIC = 'transactions'
ALERTS_TOPIC = 'alerts'

@pytest.fixture(scope="module")
def kafka_producer():
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

@pytest.fixture(scope="module")
def kafka_consumer():
    consumer = KafkaConsumer(
        ALERTS_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        consumer_timeout_ms=10000 # Wait up to 10s
    )
    return consumer

def test_end_to_end_fraud_alert(kafka_producer, kafka_consumer):
    """
    Test that a high-value transaction triggers a fraud alert.
    """
    # 1. Generate a fraudulent transaction
    transaction_id = str(uuid.uuid4())
    transaction = {
        "transaction_id": transaction_id,
        "user_id": "test_user",
        "amount": 5000.0, # Should trigger dummy rule (> 3000)
        "merchant_id": "test_merchant",
        "timestamp": "2023-01-01T00:00:00"
    }
    
    # 2. Send to Kafka
    kafka_producer.send(INPUT_TOPIC, value=transaction)
    kafka_producer.flush()
    print(f"Sent transaction: {transaction_id}")
    
    # 3. Listen for alert
    alert_received = False
    for message in kafka_consumer:
        alert = message.value
        if alert['transaction_id'] == transaction_id:
            assert alert['score'] > 0.5
            print(f"Received alert for {transaction_id}: {alert}")
            alert_received = True
            break
            
    assert alert_received, "Did not receive fraud alert for high-value transaction"
