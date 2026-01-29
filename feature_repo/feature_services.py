from feast import FeatureService
from feature_views import (
    clinical_stats_fv,
    lifestyle_fv,
    medical_conditions_fv,
    computed_risk_fv,
    demographics_fv,
    hospital_fv
)

# Curated feature service for stroke prediction
# Only includes selected features, not everything from all sources
stroke_risk_v1 = FeatureService(
    name="stroke_risk_v1",
    features=[
        # Demographics (subset)
        demographics_fv[["age", "gender"]],  # Excluding: blood_type, insurance_id, etc.
        
        # Clinical (subset)
        clinical_stats_fv[["avg_glucose_level", "bmi", "blood_pressure_systolic"]],
        # Excluding: heart_rate, temperature from training
        
        # Lifestyle (subset)
        lifestyle_fv[["smoking_status", "exercise_frequency"]],  
        # Excluding: diet_type, stress_level for this model version
        
        # Conditions (subset - all relevant)
        medical_conditions_fv[["hypertension", "heart_disease", "diabetes"]],
        # Target separate: stroke_occurred
        
        # Computed (all relevant)
        computed_risk_fv[["risk_score", "age_group", "condition_count", "has_multiple_conditions"]],
        
        # Hospital (subset)
        hospital_fv[["was_hospitalized"]],  
        # Excluding: physician names, specific departments for privacy
    ],
    description="Stroke risk prediction features v1. "
                "Selected 12 features from 6 different data sources. "
                "PostgreSQL, MinIO S3, Spark, MongoDB, MSSQL aggregated via Feast.",
    tags={"version": "1.0", "model": "xgboost", "status": "production"},
    owner="ml-team@healthcare.com"
)

# Alternative service for high-risk patients (different feature set)
stroke_risk_high_conf = FeatureService(
    name="stroke_risk_high_confidence",
    features=[
        demographics_fv[["age", "gender", "blood_type"]],  # Include blood type for high-risk
        clinical_stats_fv,
        computed_risk_fv,
        medical_conditions_fv,  # Include all condition details
    ],
    description="High-confidence model using more features for critical cases",
    tags={"version": "2.0-beta", "model": "ensemble", "status": "experimental"}
)
