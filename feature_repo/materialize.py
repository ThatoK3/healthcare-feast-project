"""
Materialize features from Offline Store (PostgreSQL) 
to Online Store (Redis) for low-latency serving
"""

from feast import FeatureStore
from datetime import datetime, timedelta
import time

def materialize_features():
    store = FeatureStore(repo_path=".")
    
    print("Materializing features from PostgreSQL → Redis...")
    print(f"Start time: {datetime.now()}")
    
    # Materialize last 7 days of data (incremental)
    # In production, run this on a schedule (Airflow/Dagster/Cron)
    start_date = datetime.now() - timedelta(days=7)
    end_date = datetime.now()
    
    store.materialize(
        feature_views=[
            "clinical_statistics",
            "lifestyle_factors", 
            "medical_conditions",
            "computed_risk_metrics",
            "patient_demographics",
            "hospitalization_metrics"
        ],
        start_date=start_date,
        end_date=end_date
    )
    
    print(f"Materialization complete at {datetime.now()}")
    
    # Verify online store
    print("\nVerifying online store connectivity...")
    features = store.get_online_features(
        features=[
            "patient_demographics:age",
            "clinical_statistics:bmi",
            "computed_risk_metrics:risk_score"
        ],
        entity_rows=[{"patient_id": "P00000001"}]
    ).to_dict()
    
    print(f"Sample fetch from Redis: {features}")

if __name__ == "__main__":
    materialize_features()
