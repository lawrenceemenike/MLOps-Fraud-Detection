import time
import json
import random
import uuid
from datetime import datetime
from kafka import KafkaProducer
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka Configuration
KAFKA_BROKER = 'kafka:29092'
TOPIC = 'transactions'

def get_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
            logger.info("Connected to Kafka")
            return producer
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}. Retrying in 5 seconds...")
            time.sleep(5)

def generate_transaction():
    """Generate a synthetic transaction."""
    transaction_id = str(uuid.uuid4())
    user_id = f"user_{random.randint(1, 1000)}"
    merchant_id = f"merchant_{random.randint(1, 100)}"
    amount = round(random.uniform(1.0, 1000.0), 2)
    timestamp = datetime.now().isoformat()
    
    # Simulate fraud pattern: High amount or specific merchant
    is_fraud = False
    if random.random() < 0.05: # 5% fraud rate
        is_fraud = True
        amount = round(random.uniform(2000.0, 5000.0), 2) # High amount fraud
    
    transaction = {
        "transaction_id": transaction_id,
        "timestamp": timestamp,
        "user_id": user_id,
        "amount": amount,
        "currency": "USD",
        "merchant_id": merchant_id,
        "payment_method": random.choice(["card", "wallet", "bank_transfer"]),
        "ip_address": f"192.168.1.{random.randint(1, 255)}",
        "is_fraud_simulated": is_fraud # Label for testing/training later
    }
    return transaction

def main():
    producer = get_producer()
    
    logger.info(f"Starting producer. Target topic: {TOPIC}")
    
    try:
        while True:
            transaction = generate_transaction()
            producer.send(TOPIC, value=transaction)
            logger.info(f"Sent: {transaction['transaction_id']} (Fraud: {transaction['is_fraud_simulated']})")
            time.sleep(random.uniform(0.1, 1.0)) # Simulate variable throughput
    except KeyboardInterrupt:
        logger.info("Stopping producer...")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
