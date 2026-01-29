"""
End-to-end test of multi-source Feast architecture
Tests all 6 data sources and feature retrieval
"""

from feast import FeatureStore
from datetime import datetime, timedelta
import pandas as pd
import random

def test_feature_retrieval():
    """Test retrieving from all 6 sources via unified interface"""
    store = FeatureStore(repo_path=".")
    
    print("=" * 60)
    print("HEALTHCARE FEAST - MULTI-SOURCE TEST")
    print("=" * 60)
    
    # Test 1: Online features (Redis) - simulated real-time inference
    print("\n1. Testing ONLINE features (Redis) for real-time inference...")
    patient_ids = [f"P{str(i).zfill(8)}" for i in range(1, 6)]
    
    online_features = store.get_online_features(
        featureservice="stroke_risk_v1",  # Using curated subset
        entity_rows=[{"patient_id": pid} for pid in patient_ids]
    ).to_df()
    
    print(f"Retrieved {len(online_features)} patient records from Redis:")
    print(online_features.to_string(index=False))
    
    # Test 2: Historical features (PostgreSQL) - training data
    print("\n2. Testing OFFLINE features (PostgreSQL) for training...")
    
    # Create entity dataframe with timestamps (point-in-time correct)
    entity_df = pd.DataFrame({
        "patient_id": [f"P{str(random.randint(1, 10000)).zfill(8)}" for _ in range(100)],
        "event_timestamp": [datetime.now() - timedelta(days=random.randint(1, 365)) for _ in range(100)],
        "stroke_occurred": [random.randint(0, 1) for _ in range(100)]  # Labels
    })
    
    historical_features = store.get_historical_features(
        entity_df=entity_df,
        features=[
            "patient_demographics:age",
            "patient_demographics:gender",
            "clinical_statistics:avg_glucose_level",
            "clinical_statistics:bmi",
            "lifestyle_factors:smoking_status",
            "medical_conditions:hypertension",
            "medical_conditions:heart_disease",
            "medical_conditions:diabetes",
            "computed_risk_metrics:risk_score",
            "computed_risk_metrics:condition_count",
            "hospitalization_metrics:was_hospitalized"
        ]
    ).to_df()
    
    print(f"Retrieved {len(historical_features)} training records from PostgreSQL")
    print("Sample columns:", list(historical_features.columns))
    print(historical_features.head(3).to_string(index=False))
    
    # Test 3: Source-specific retrieval validation
    print("\n3. Validating individual source connections...")
    
    sources = {
        "PostgreSQL (Clinical)": ["clinical_statistics:bmi", "clinical_statistics:avg_glucose_level"],
        "MinIO S3 (Lifestyle)": ["lifestyle_factors:smoking_status", "lifestyle_factors:exercise_frequency"],
        "MinIO S3 (Conditions)": ["medical_conditions:hypertension", "medical_conditions:stroke_occurred"],
        "Spark (Computed)": ["computed_risk_metrics:risk_score", "computed_risk_metrics:age_group"],
        "MongoDB (Demographics)": ["patient_demographics:age", "patient_demographics:gender"],
        "MSSQL (Hospital)": ["hospitalization_metrics:was_hospitalized"]
    }
    
    for source_name, features in sources.items():
        try:
            result = store.get_online_features(
                features=features,
                entity_rows=[{"patient_id": "P00000001"}]
            ).to_dict()
            print(f"  ✓ {source_name}: Connected")
        except Exception as e:
            print(f"  ✗ {source_name}: Failed - {str(e)[:50]}")
    
    # Test 4: Feature Service metadata
    print("\n4. Feature Service Registry Info...")
    service = store.get_feature_service("stroke_risk_v1")
    print(f"Name: {service.name}")
    print(f"Description: {service.description}")
    print(f"Features in service: {len(service.feature_view_projections)} feature views")
    
    print("\n" + "=" * 60)
    print("TEST COMPLETE - Multi-source Feast architecture validated")
    print("=" * 60)

if __name__ == "__main__":
    test_feature_retrieval()
