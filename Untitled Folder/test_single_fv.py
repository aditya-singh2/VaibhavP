from datetime import timedelta

import pandas as pd
import yaml

from feast import (
    Entity,
    FeatureService,
    FeatureView,
    Field,
    PushSource,
    RequestSource,
    SnowflakeSource,
    batch_feature_view,
    aggregation
)

from feast.types import Float32, Float64, Int64, String, UnixTimestamp
import numpy as np
from datetime import datetime, timedelta

customer = Entity(name="customer", join_keys=["CUSTOMER_ID"])


order = Entity(name="order", join_keys=["ORDER_ID"])

product = Entity(name="product", join_keys=["PRODUCT_ID"])

# project_name = yaml.safe_load(open("feature_store.yaml"))["project"]
project_name = yaml.safe_load(open("feature_store.yaml"))["project"]

# OLIST_CUSTOMERS data source
olist_customers_source = SnowflakeSource(
    database=yaml.safe_load(open("feature_store.yaml"))["offline_store"]["database"],
    schema=yaml.safe_load(open("feature_store.yaml"))["offline_store"]["schema"],
    timestamp_field="EVENT_TIMESTAMP",
    created_timestamp_column="CREATED_TIMESTAMP",
    table="RETAIL_AGGREGATION_MASTERTABLE"
)

olist_customers_fv_3 = FeatureView(
    name="olist_customers_fv_3",
    entities=[customer],
    ttl=timedelta(weeks=52 * 10),
    schema=[
        Field(name="LIST_PRICE", dtype=Float64),
        Field(name="SALE_PRICE", dtype=Float64),
    ],
    source=olist_customers_source,
    tags={"team": "customers"},
)

test_fv = FeatureView(
    name="test_fv",
    entities=[customer],
    ttl=timedelta(weeks=52 * 10),
    schema=[
        Field(name="LIST_PRICE", dtype=Float64),
        Field(name="SALE_PRICE", dtype=Float64),
    ],
    source=olist_customers_source,
    tags={"team": "customers"},
)

# # OLIST_GEOLOCATION data source
# retail_stats_source = SnowflakeSource(
#     database=yaml.safe_load(open("feature_store.yaml"))["offline_store"]["database"],
#     schema=yaml.safe_load(open("feature_store.yaml"))["offline_store"]["schema"],
#     timestamp_field="EVENT_TIMESTAMP",
#     created_timestamp_column="CREATED_TIMESTAMP",
#     table="Aggregate_Sales_Summary_Insights"
# )
#
customer_orders_fs = FeatureService(
    name="customer_orders_fs",
    features=[olist_customers_fv_3]
)

