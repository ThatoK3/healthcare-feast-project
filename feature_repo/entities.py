from feast import Entity, ValueType

# Core entity across all data sources
patient = Entity(
    name="patient",
    join_keys=["patient_id"],
    value_type=ValueType.STRING,
    description="Healthcare patient identifier linking all data sources",
    tags={"team": "healthcare-analytics", "domain": "patient"}
)
