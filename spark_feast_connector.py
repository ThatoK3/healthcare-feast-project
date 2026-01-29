"""
Spark → Feast Connector
Ingests data from MongoDB and MSSQL into Feast via PushSource
(Required because Feast doesn't have native MongoDB/MSSQL offline store support)
"""

from pyspark.sql import SparkSession
from feast import FeatureStore, PushMode
from feast.data_source import PushSource
import os
from datetime import datetime

def get_spark_session():
    """Connect to existing Spark cluster"""
    return (SparkSession.builder
        .appName("FeastDataIngestion")
        .master("spark://spark-master:7077")
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.2,com.microsoft.sqlserver:mssql-jdbc:12.4.0.jre11")
        .getOrCreate())

def ingest_mongodb_demographics(spark, store):
    """Read from MongoDB, push to Feast demographics PushSource"""
    print("Ingesting MongoDB demographics...")
    
    mongo_uri = "mongodb://healthcare_admin:HealthCare2024!@healthcare-mongodb:27017/healthcare.patient_demographics?authSource=admin"
    
    df = (spark.read.format("mongo")
        .option("uri", mongo_uri)
        .load())
    
    # Calculate age and select relevant columns
    from pyspark.sql.functions import datediff, current_date, col, lit, current_timestamp
    
    df = df.withColumn("age", (datediff(current_date(), col("date_of_birth")) / 365.25).cast("int"))
    df = df.withColumn("event_timestamp", current_timestamp())  # Required for Feast
    
    # Select only columns defined in the schema
    df = df.select("patient_id", "age", "gender", "blood_type", "healthcare_region", "event_timestamp")
    
    # Push to Feast online and offline stores
    store.push("patient_demographics", df, to=PushMode.ONLINE_AND_OFFLINE)
    print(f"Pushed {df.count()} demographic records to Feast")

def ingest_mssql_hospital(spark, store):
    """Read from MS SQL, push to Feast hospital PushSource"""
    print("Ingesting MSSQL hospital data...")
    
    jdbc_url = "jdbc:sqlserver://healthcare-mssql:1433;databaseName=healthcare;encrypt=true;trustServerCertificate=true"
    
    df = (spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "hospital_administration")
        .option("user", "sa")
        .option("password", "HealthCare2024!")
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .load())
    
    from pyspark.sql.functions import col, lit, current_timestamp, datediff, current_date, when
    
    # Create hospitalization metrics
    df = df.withColumn("event_timestamp", current_timestamp())
    df = df.withColumn("was_hospitalized", lit(1))
    df = df.withColumn("days_since_admission", 
        datediff(current_date(), col("admission_date")))
    df = df.withColumn("is_currently_admitted", 
        when(col("discharge_status").isNull(), 1).otherwise(0))
    
    # Select columns matching the schema
    df = df.select("patient_id", "admission_date", "discharge_status", "department", 
                   "was_hospitalized", "days_since_admission", "is_currently_admitted", 
                   "event_timestamp")
    
    store.push("hospital_administration", df, to=PushMode.ONLINE_AND_OFFLINE)
    print(f"Pushed {df.count()} hospital records to Feast")

def ingest_computed_features(spark, store):
    """Compute advanced features in Spark and push to Feast"""
    print("Computing and ingesting Spark features...")
    
    # Read base data from MinIO/PostgreSQL via Spark
    # This demonstrates SparkSource usage
    # In practice, you'd read from your existing Spark DataFrames
    
    # Example: Create computed risk features
    # This would connect to your existing AnalyticsFeatureEngineer logic
    
    print("Spark features would be pushed here via computed_features_source")

def main():
    store = FeatureStore(repo_path="feature_repo")
    spark = get_spark_session()
    
    try:
        print(f"Starting data ingestion at {datetime.now()}")
        
        # Ingest from sources not natively supported by Feast offline stores
        ingest_mongodb_demographics(spark, store)
        ingest_mssql_hospital(spark, store)
        
        # Trigger materialization to Redis
        print("Materializing features to Redis online store...")
        store.materialize_incremental(end_date=datetime.now())
        
        print("Ingestion complete!")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
