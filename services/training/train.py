import pandas as pd
import mlflow
from pycaret.classification import *
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# MLflow Configuration
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
mlflow.set_experiment("fraud_detection_experiment")

def get_data(n_samples=1000):
    """Generate synthetic training data."""
    logger.info(f"Generating {n_samples} samples...")
    data = []
    for _ in range(n_samples):
        # Similar logic to producer but simpler for dataframe creation
        import random
        is_fraud = random.random() < 0.05
        amount = random.uniform(10, 1000)
        if is_fraud:
            amount = random.uniform(2000, 5000)
            
        data.append({
            "amount": amount,
            "payment_method_card": random.choice([0, 1]),
            "payment_method_wallet": random.choice([0, 1]),
            "is_fraud": 1 if is_fraud else 0
        })
    return pd.DataFrame(data)

def train_model():
    """Train and register model."""
    data = get_data(5000)
    
    logger.info("Initializing PyCaret setup...")
    s = setup(data, target='is_fraud', session_id=123, log_experiment=True, experiment_name='fraud_detection_experiment', verbose=False)
    
    logger.info("Comparing models...")
    best_model = compare_models(fold=3, sort='AUC', n_select=1)
    
    logger.info(f"Best model found: {best_model}")
    
    # Finalize model
    final_model = finalize_model(best_model)
    
    # Log to MLflow manually if needed, but PyCaret does it automatically if log_experiment=True
    # However, we want to register it explicitly to the model registry
    
    logger.info("Registering model...")
    # PyCaret's save_model saves to a pickle. We want to log it as an MLflow artifact.
    # We can use mlflow.sklearn.log_model
    
    with mlflow.start_run() as run:
        mlflow.sklearn.log_model(final_model, "model", registered_model_name="FraudDetectionModel")
        logger.info(f"Model registered with Run ID: {run.info.run_id}")

if __name__ == "__main__":
    train_model()
