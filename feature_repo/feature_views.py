from feast import FeatureView, Field
from feast.types import Int64, String, Float64, UnixTimestamp
from datetime import timedelta

from entities import patient
from data_sources import (
    clinical_measurements_source,
    lifestyle_source,
    stroke_outcomes_source,
    demographics_push_source,
    hospital_push_source,
    computed_push_source,
)

# Feature View 1: PostgreSQL Clinical
clinical_stats_fv = FeatureView(
    name="clinical_statistics",
    entities=[patient],
    ttl=timedelta(days=365),
    schema=[
        Field(name="avg_glucose_level", dtype=Float64),
        Field(name="bmi", dtype=Float64),
        Field(name="blood_pressure_systolic", dtype=Int64),
    ],
    online=True,
    source=clinical_measurements_source,
    tags={"source": "postgresql"},
)

# Feature View 2: MinIO S3 Lifestyle
lifestyle_fv = FeatureView(
    name="lifestyle_factors",
    entities=[patient],
    ttl=timedelta(days=180),
    schema=[
        Field(name="smoking_status", dtype=String),
        Field(name="exercise_frequency", dtype=String),
        Field(name="alcohol_consumption", dtype=String),
        Field(name="diet_type", dtype=String),
        Field(name="stress_level", dtype=String),
    ],
    online=True,
    source=lifestyle_source,
    tags={"source": "minio-s3"},
)

# Feature View 3: MinIO S3 Conditions
medical_conditions_fv = FeatureView(
    name="medical_conditions",
    entities=[patient],
    ttl=timedelta(days=3650),
    schema=[
        Field(name="hypertension", dtype=Int64),
        Field(name="heart_disease", dtype=Int64),
        Field(name="diabetes", dtype=Int64),
        Field(name="stroke_occurred", dtype=Int64),
        Field(name="stroke_probability", dtype=Float64),
        Field(name="stroke_severity", dtype=String),
        Field(name="recovery_status", dtype=String),
    ],
    online=True,
    source=stroke_outcomes_source,
    tags={"source": "minio-s3", "is_target": "true"},
)

# Feature View 4: MongoDB via Push - schema defined HERE
demographics_fv = FeatureView(
    name="patient_demographics",
    entities=[patient],
    ttl=timedelta(days=3650),
    schema=[
        Field(name="age", dtype=Int64),
        Field(name="gender", dtype=String),
        Field(name="blood_type", dtype=String),
        Field(name="healthcare_region", dtype=String),
        Field(name="insurance_type", dtype=String),
        Field(name="event_timestamp", dtype=UnixTimestamp),
    ],
    online=True,
    source=demographics_push_source,
    tags={"source": "mongodb"},
)

# Feature View 5: MSSQL via Push
hospital_fv = FeatureView(
    name="hospitalization_metrics",
    entities=[patient],
    ttl=timedelta(days=730),
    schema=[
        Field(name="was_hospitalized", dtype=Int64),
        Field(name="days_since_admission", dtype=Int64),
        Field(name="is_currently_admitted", dtype=Int64),
        Field(name="department", dtype=String),
        Field(name="discharge_status", dtype=String),
        Field(name="admission_date", dtype=UnixTimestamp),
        Field(name="event_timestamp", dtype=UnixTimestamp),
    ],
    online=True,
    source=hospital_push_source,
    tags={"source": "mssql"},
)

# Feature View 6: Spark Computed via Push
computed_risk_fv = FeatureView(
    name="computed_risk_metrics",
    entities=[patient],
    ttl=timedelta(days=30),
    schema=[
        Field(name="risk_score", dtype=Int64),
        Field(name="age_group", dtype=String),
        Field(name="condition_count", dtype=Int64),
        Field(name="has_multiple_conditions", dtype=Int64),
        Field(name="days_since_stroke", dtype=Int64),
        Field(name="event_timestamp", dtype=UnixTimestamp),
    ],
    online=True,
    source=computed_push_source,
    tags={"source": "spark"},
)    
