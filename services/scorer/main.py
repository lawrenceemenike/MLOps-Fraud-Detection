import json
import logging
import threading
import time
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from kafka import KafkaConsumer, KafkaProducer
from pydantic import BaseModel

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BROKER = 'kafka:29092'
INPUT_TOPIC = 'transactions'
ALERTS_TOPIC = 'alerts'
MODEL_URI = None # Placeholder for MLflow model URI

# Global variables
producer = None
consumer = None
model = None
running = True

class Transaction(BaseModel):
    transaction_id: str
    user_id: str
    amount: float
    merchant_id: str
    timestamp: str
    # ... other fields optional for now

def load_model():
    """Load model from MLflow or use a dummy fallback."""
    global model
    # TODO: Implement MLflow loading logic
    # model = mlflow.pyfunc.load_model(model_uri)
    logger.info("Loading dummy model...")
    model = "DUMMY_MODEL"

def score_transaction(transaction: dict):
    """Score a transaction using the loaded model."""
    # Dummy logic: Flag if amount > 3000
    amount = float(transaction.get("amount", 0))
    is_fraud = amount > 3000
    score = 0.9 if is_fraud else 0.1
    return is_fraud, score

import psycopg2
import os

# Database Configuration
DB_HOST = "postgres"
DB_NAME = "airflow" # Using the existing airflow DB for simplicity, ideally separate
DB_USER = "airflow"
DB_PASS = "airflow_password"

def get_db_connection():
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
    return conn

def init_db():
    """Initialize the transactions table."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                transaction_id VARCHAR(255) PRIMARY KEY,
                user_id VARCHAR(255),
                amount FLOAT,
                merchant_id VARCHAR(255),
                timestamp TIMESTAMP,
                is_fraud BOOLEAN,
                score FLOAT
            );
        """)
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Database initialized.")
    except Exception as e:
        logger.error(f"DB Init Error: {e}")

def save_to_db(transaction: dict, is_fraud: bool, score: float):
    """Save transaction and score to Postgres."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO transactions (transaction_id, user_id, amount, merchant_id, timestamp, is_fraud, score)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (transaction_id) DO NOTHING;
        """, (
            transaction.get("transaction_id"),
            transaction.get("user_id"),
            transaction.get("amount"),
            transaction.get("merchant_id"),
            transaction.get("timestamp"),
            is_fraud,
            score
        ))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"DB Save Error: {e}")

def kafka_consumer_loop():
    """Background loop to consume transactions and score them."""
    global consumer, producer, running
    
    # Initialize DB
    init_db()
    
    logger.info("Starting Kafka consumer loop...")
    while running:
        try:
            if not consumer:
                consumer = KafkaConsumer(
                    INPUT_TOPIC,
                    bootstrap_servers=[KAFKA_BROKER],
                    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                    auto_offset_reset='latest',
                    group_id='scorer-group'
                )
                logger.info("Connected to Kafka Consumer")

            if not producer:
                producer = KafkaProducer(
                    bootstrap_servers=[KAFKA_BROKER],
                    value_serializer=lambda x: json.dumps(x).encode('utf-8')
                )
                logger.info("Connected to Kafka Producer")

            for message in consumer:
                if not running:
                    break
                
                transaction = message.value
                is_fraud, score = score_transaction(transaction)
                
                # Save to DB
                save_to_db(transaction, is_fraud, score)
                
                if is_fraud:
                    alert = {
                        "transaction_id": transaction.get("transaction_id"),
                        "score": score,
                        "reason": "High fraud score",
                        "original_transaction": transaction
                    }
                    producer.send(ALERTS_TOPIC, value=alert)
                    logger.info(f"FRAUD DETECTED: {transaction.get('transaction_id')} Score: {score}")
                else:
                    # Optional: Log benign transactions or send to a scored topic
                    pass
                    
        except Exception as e:
            logger.error(f"Error in consumer loop: {e}. Retrying in 5s...")
            time.sleep(5)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    load_model()
    consumer_thread = threading.Thread(target=kafka_consumer_loop, daemon=True)
    consumer_thread.start()
    yield
    # Shutdown
    global running
    running = False
    if consumer:
        consumer.close()
    if producer:
        producer.close()

app = FastAPI(lifespan=lifespan)

from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

# Prometheus Metrics
REQUEST_COUNT = Counter('scorer_request_count', 'Total scoring requests', ['status'])
FRAUD_DETECTED_COUNT = Counter('scorer_fraud_detected_count', 'Total fraud detected')
SCORING_LATENCY = Histogram('scorer_latency_seconds', 'Time spent scoring')

# ... (existing code)

def score_transaction(transaction: dict):
    """Score a transaction using the loaded model."""
    with SCORING_LATENCY.time():
        # Dummy logic: Flag if amount > 3000
        amount = float(transaction.get("amount", 0))
        is_fraud = amount > 3000
        score = 0.9 if is_fraud else 0.1
        
        REQUEST_COUNT.labels(status='success').inc()
        if is_fraud:
            FRAUD_DETECTED_COUNT.inc()
            
        return is_fraud, score

# ... (existing code)

@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.get("/")
def read_root():
    return {"status": "running", "model": "dummy"}

@app.post("/predict")
def predict(transaction: Transaction):
    is_fraud, score = score_transaction(transaction.dict())
    return {"is_fraud": is_fraud, "score": score}
