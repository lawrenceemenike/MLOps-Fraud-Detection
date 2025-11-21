import pandas as pd
import numpy as np

def engineer_features(transaction: dict) -> dict:
    """
    Transform a single transaction dictionary into a feature dictionary.
    This is used for real-time scoring.
    """
    # Example features
    features = {}
    features['amount'] = float(transaction.get('amount', 0))
    
    # One-hot encoding simulation (in production, load encoder)
    payment_method = transaction.get('payment_method', 'unknown')
    features['payment_method_card'] = 1 if payment_method == 'card' else 0
    features['payment_method_wallet'] = 1 if payment_method == 'wallet' else 0
    
    # Time features
    # timestamp = pd.to_datetime(transaction.get('timestamp'))
    # features['hour'] = timestamp.hour
    
    return features

def preprocess_batch(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess a batch of transactions for training.
    """
    # Ensure types
    df['amount'] = df['amount'].astype(float)
    
    # Simple feature engineering
    # In a real scenario, this would be more complex and align with 'engineer_features'
    return df
