from feast import FileSource, PushSource
from feast.data_format import ParquetFormat
from feast.types import Int64, String, Float64, UnixTimestamp

# PostgreSQLSource import with fallback
try:
    from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import PostgreSQLSource
except ImportError:
    from feast import PostgreSQLSource

# Source 1: PostgreSQL (Clinical Data)
clinical_measurements_source = PostgreSQLSource(
    name="clinical_measurements",
    query="SELECT patient_id, visit_date, avg_glucose_level, bmi, blood_pressure_systolic FROM clinical_measurements",
    timestamp_field="visit_date",
    description="Clinical measurements from PostgreSQL",
)

# Source 2: MinIO S3 (Lifestyle Data)
lifestyle_source = FileSource(
    name="lifestyle_data",
    path="s3://healthcare-lifestyle/lifestyle_data.parquet",
    s3_endpoint_override="http://healthcare-minio:9000",
    file_format=ParquetFormat(),
    timestamp_field="event_timestamp",
    description="Lifestyle factors from MinIO S3",
)

# Source 3: MinIO S3 (Conditions & Target)
stroke_outcomes_source = FileSource(
    name="stroke_outcomes",
    path="s3://healthcare-batch/stroke_outcomes.parquet",
    s3_endpoint_override="http://healthcare-minio:9000",
    file_format=ParquetFormat(),
    timestamp_field="stroke_date",
    description="Stroke conditions and outcomes",
)

# Sources 4, 5, 6: PushSources with underlying batch storage in MinIO
# Data pushed via Spark will be stored in these Parquet locations

demographics_batch_source = FileSource(
    name="patient_demographics_batch",
    path="s3://healthcare-feast/demographics.parquet",
    s3_endpoint_override="http://healthcare-minio:9000",
    file_format=ParquetFormat(),
    timestamp_field="event_timestamp",
)

demographics_push_source = PushSource(
    name="patient_demographics",
    batch_source=demographics_batch_source,
    description="Patient demographics pushed from MongoDB via Spark",
)

hospital_batch_source = FileSource(
    name="hospital_administration_batch",
    path="s3://healthcare-feast/hospital.parquet",
    s3_endpoint_override="http://healthcare-minio:9000",
    file_format=ParquetFormat(),
    timestamp_field="event_timestamp",
)

hospital_push_source = PushSource(
    name="hospital_administration",
    batch_source=hospital_batch_source,
    description="Hospital data pushed from MSSQL via Spark",
)

computed_batch_source = FileSource(
    name="computed_risk_features_batch",
    path="s3://healthcare-feast/computed.parquet",
    s3_endpoint_override="http://healthcare-minio:9000",
    file_format=ParquetFormat(),
    timestamp_field="event_timestamp",
)

computed_push_source = PushSource(
    name="computed_risk_features",
    batch_source=computed_batch_source,
    description="Computed features from Spark",
)
