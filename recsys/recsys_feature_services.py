from tecton import FileConfig, BatchSource, Entity, batch_feature_view, FeatureService, RedisConfig, on_demand_feature_view
from pyspark.sql.functions import from_unixtime, col

# def convert_long_timestamp(df):

#     df_with_date = df.withColumn(
#         "timestamp",
#         from_unixtime((col("timestamp") / 1e9).cast("double")).cast("timestamp")
#     )
#     return df_with_date

# def get_seller_df(df):

from tecton import spark_batch_config, BatchSource

@spark_batch_config()
def product_df(spark):
    from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, TimestampType, StringType, LongType
    schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("seller_id", LongType(), True),
        StructField("feature_1", DoubleType(), True),
        StructField("feature_2", DoubleType(), True),
        StructField("feature_3", DoubleType(), True),
        StructField("feature_4", DoubleType(), True),
        StructField("feature_5", DoubleType(), True),
        StructField("feature_6", DoubleType(), True),
        StructField("feature_7", DoubleType(), True),
        StructField("feature_8", DoubleType(), True),
        StructField("feature_9", DoubleType(), True),
        StructField("feature_10", DoubleType(), True),
        StructField("timestamp", LongType(), True),
    ])

    df = spark.read.schema(schema).parquet("s3://tecton-spark/data/recsys_sellers_112.pq")
    df_with_date = df.withColumn(
        "timestamp",
        from_unixtime((col("timestamp") / 1e9).cast("double")).cast("timestamp")
    )
    return df_with_date

@spark_batch_config()
def buyer_df(spark):
    from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, TimestampType, StringType, LongType
    schema = StructType([
        StructField("buyer_id", StringType(), True),
        StructField("buyer_feature_1", DoubleType(), True),
        StructField("buyer_feature_2", DoubleType(), True),
        StructField("buyer_feature_3", DoubleType(), True),
        StructField("timestamp", LongType(), True),
    ])

    df = spark.read.schema(schema).parquet("s3://tecton-spark/data/recsys_buyers_112.pq")
    df_with_date = df.withColumn(
        "timestamp",
        from_unixtime((col("timestamp") / 1e9).cast("double")).cast("timestamp")
    )
    return df_with_date

seller_datasource = BatchSource(
    name="seller_d",
    batch_config=product_df,
    owner='mihir@tecton.ai',
    tags={'release': 'production'}
)

buyer_datasource = BatchSource(
    name="buyer_d",
    batch_config=buyer_df,
    owner='mihir@tecton.ai',
    tags={'release': 'production'}
#   name='buyer_d',
#   batch_config=FileConfig(
#     uri='s3://tecton-spark/data/recsys_buyers_112.pq',
#     # uri='s3://tecton-spark-edge/data/recsys_buyers_919.pq',
#     file_format='parquet',
#     timestamp_field='timestamp',
#     # post_processor=convert_long_timestamp,
#   ),
#   owner='kzhang@tecton.ai',
#   tags={'release': 'test'},
)

# seller_datasource = BatchSource(
#   name='seller_d',
#   batch_config=FileConfig(
#     uri='s3://tecton-spark/data/recsys_sellers_112.pq',
#     # uri='s3://tecton-spark-edge/data/recsys_sellers_919.pq',

#     # uri='s3://tecton.ai/data/recsys_sellers_109.pq',
#     file_format='parquet',
#     timestamp_field='timestamp',
#     # post_processor=convert_long_timestamp,
#   ),
#   owner='kzhang@tecton.ai',
#   tags={'release': 'test'},
# )



# buyer_datasource = BatchSource(
#   name='buyer_d',
#   batch_config=FileConfig(
#     uri='s3://tecton-spark/data/recsys_buyers_112.pq',
#     # uri='s3://tecton-spark-edge/data/recsys_buyers_919.pq',
#     file_format='parquet',
#     timestamp_field='timestamp',
#     # post_processor=convert_long_timestamp,
#   ),
#   owner='kzhang@tecton.ai',
#   tags={'release': 'test'},
# )

buyer = Entity(name='buyer', join_keys=['buyer_id'])
seller = Entity(name='seller', join_keys=['seller_id'])
product = Entity(name='product', join_keys=['product_id'])

from tecton import Aggregation, FilteredSource
from datetime import datetime, timedelta
from tecton import FeatureService

@batch_feature_view(
    options={"DEV_CACHE_MAX_AGE_SECONDS": "86400"},
    sources=[seller_datasource],
    entities=[seller, product],
    mode='spark_sql',
    online=True,
    offline=False,
    feature_start_time=datetime(2022, 1, 1),
    aggregation_interval=timedelta(days=1),
    aggregations=[
        Aggregation(column='feature_1', function='sum', time_window=timedelta(days=30)),
        Aggregation(column='feature_1', function='count', time_window=timedelta(days=30)),
        Aggregation(column='feature_1', function='mean', time_window=timedelta(days=30)),
        ],
    #online_store=redis_config,
)
def seller_features1(data):
    return f'''
        SELECT
            seller_id,
            product_id,
            feature_1,
            timestamp
        FROM
            {data}
        '''

@batch_feature_view(
    options={"DEV_CACHE_MAX_AGE_SECONDS": "86400"},
    sources=[seller_datasource],
    entities=[seller, product],
    mode='spark_sql',
    online=True,
    offline=False,
    feature_start_time=datetime(2022, 1, 1),
    aggregation_interval=timedelta(days=1),
    aggregations=[
        Aggregation(column='feature_2', function='sum', time_window=timedelta(days=28)),
        Aggregation(column='feature_2', function='count', time_window=timedelta(days=28)),
        Aggregation(column='feature_2', function='mean', time_window=timedelta(days=28)),
    ],
    #online_store=redis_config,
)
def seller_features2(data):
    return f'''
        SELECT
            seller_id,
            product_id,
            feature_2,
            timestamp
        FROM
            {data}
        '''

@batch_feature_view(
    options={"DEV_CACHE_MAX_AGE_SECONDS": "86400"},
    sources=[seller_datasource],
    entities=[seller, product],
    mode='spark_sql',
    online=True,
    offline=False,
    feature_start_time=datetime(2022, 1, 1),
    aggregation_interval=timedelta(days=1),
    aggregations=[
        Aggregation(column='feature_3', function='sum', time_window=timedelta(days=28)),
        Aggregation(column='feature_3', function='count', time_window=timedelta(days=28)),
        Aggregation(column='feature_3', function='mean', time_window=timedelta(days=28)),
        ],
    #online_store=redis_config,
)
def seller_features3(data):
    return f'''
        SELECT
            seller_id,
            product_id,
            feature_3,
            timestamp
        FROM
            {data}
        '''

@batch_feature_view(
    options={"DEV_CACHE_MAX_AGE_SECONDS": "86400"},
    sources=[seller_datasource],
    entities=[seller, product],
    mode='spark_sql',
    online=True,
    offline=False,
    feature_start_time=datetime(2022, 1, 1),
    aggregation_interval=timedelta(days=1),
    aggregations=[
        Aggregation(column='feature_4', function='sum', time_window=timedelta(days=28)),
        Aggregation(column='feature_4', function='count', time_window=timedelta(days=28)),
        Aggregation(column='feature_4', function='mean', time_window=timedelta(days=28)),
        ],
    #online_store=redis_config,
)
def seller_features4(data):
    return f'''
        SELECT
            seller_id,
            product_id,
            feature_4,
            timestamp
        FROM
            {data}
        '''

@batch_feature_view(
    options={"DEV_CACHE_MAX_AGE_SECONDS": "86400"},
    sources=[seller_datasource],
    entities=[seller, product],
    mode='spark_sql',
    online=True,
    offline=False,
    feature_start_time=datetime(2022, 1, 1),
    aggregation_interval=timedelta(days=1),
    aggregations=[
        Aggregation(column='feature_5', function='sum', time_window=timedelta(days=28)),
        Aggregation(column='feature_5', function='count', time_window=timedelta(days=28)),
        Aggregation(column='feature_5', function='mean', time_window=timedelta(days=28)),
    ],
    #online_store=redis_config,
)
def product_features1(data):
    return f'''
        SELECT
            seller_id,
            product_id,
            feature_5,
            timestamp
        FROM
            {data}
        '''

@batch_feature_view(
    options={"DEV_CACHE_MAX_AGE_SECONDS": "86400"},
    sources=[seller_datasource],
    entities=[seller, product],
    mode='spark_sql',
    online=True,
    offline=False,
    feature_start_time=datetime(2022, 1, 1),
    aggregation_interval=timedelta(days=1),
    aggregations=[
        Aggregation(column='feature_6', function='sum', time_window=timedelta(days=28)),
        Aggregation(column='feature_6', function='count', time_window=timedelta(days=28)),
        Aggregation(column='feature_6', function='mean', time_window=timedelta(days=28)),
    ],
    #online_store=redis_config,
)
def product_features2(data):
    return f'''
        SELECT
            seller_id,
            product_id,
            feature_6,
            timestamp
        FROM
            {data}
        '''

@batch_feature_view(
    options={"DEV_CACHE_MAX_AGE_SECONDS": "86400"},
    sources=[seller_datasource],
    entities=[seller, product],
    mode='spark_sql',
    online=True,
    offline=False,
    feature_start_time=datetime(2022, 1, 1),
    aggregation_interval=timedelta(days=1),
    aggregations=[
        Aggregation(column='feature_7', function='sum', time_window=timedelta(days=28)),
        Aggregation(column='feature_7', function='count', time_window=timedelta(days=28)),
        Aggregation(column='feature_7', function='mean', time_window=timedelta(days=28)),
    ],
    #online_store=redis_config,
)
def product_features3(data):
    return f'''
        SELECT
            seller_id,
            product_id,
            feature_7,
            timestamp
        FROM
            {data}
        '''

@batch_feature_view(
    options={"DEV_CACHE_MAX_AGE_SECONDS": "86400"},
    sources=[buyer_datasource],
    entities=[buyer],
    mode='spark_sql',
    online=True,
    offline=False,
    aggregations=[
        Aggregation(column='buyer_feature_2', function='sum', time_window=timedelta(days=28)),
        Aggregation(column='buyer_feature_2', function='count', time_window=timedelta(days=28)),
        Aggregation(column='buyer_feature_2', function='mean', time_window=timedelta(days=28)),
    ],
    aggregation_interval=timedelta(days=7),
    feature_start_time=datetime(2022, 1, 1),
    #online_store=redis_config,
)
def buyer_features2(data):
    return f'''
        SELECT
            buyer_id,
            buyer_feature_2,
            timestamp
        FROM
            {data}
        '''

@batch_feature_view(
    options={"DEV_CACHE_MAX_AGE_SECONDS": "86400"},
    sources=[buyer_datasource],
    entities=[buyer],
    mode='spark_sql',
    online=True,
    offline=False,
    aggregations=[
        Aggregation(column='buyer_feature_1', function='sum', time_window=timedelta(days=28)),
        Aggregation(column='buyer_feature_1', function='count', time_window=timedelta(days=28)),
        Aggregation(column='buyer_feature_1', function='mean', time_window=timedelta(days=28)),
        ],
    aggregation_interval=timedelta(days=7),
    feature_start_time=datetime(2022, 1, 1),
)
def buyer_features1(data):
    return f'''
        SELECT
            buyer_id,
            buyer_feature_1,
            timestamp
        FROM
            {data}
        '''
@batch_feature_view(
    options={"DEV_CACHE_MAX_AGE_SECONDS": "86400"},
    sources=[buyer_datasource],
    entities=[buyer],
    mode='spark_sql',
    online=True,
    offline=False,
    aggregations=[
        Aggregation(column='buyer_feature_3', function='sum', time_window=timedelta(days=28)),
        Aggregation(column='buyer_feature_3', function='count', time_window=timedelta(days=28)),
       Aggregation(column='buyer_feature_3', function='mean', time_window=timedelta(days=28)),
    ],
    aggregation_interval=timedelta(days=7),
    feature_start_time=datetime(2022, 1, 1),
)
def buyer_features3(data):
    return f'''
        SELECT
            buyer_id,
            buyer_feature_3,
            timestamp
        FROM
            {data}
        '''

from tecton.types import Field
from tecton.types import Float64
from tecton.types import Int64
from tecton.types import String
from tecton.types import Timestamp

@on_demand_feature_view(
    mode="python",
    sources=[seller_features1, buyer_features3],
    schema=[
        Field("meanx1000", Float64),
        Field("feature_2", String),
    ],
)
def odfv_with_bfv_input(seller_features1, buyer_features3):
    return {
        "meanx1000": seller_features1["feature_1_mean_30d_1d"] * 1000,
        "feature_2": str(buyer_features3["buyer_feature_3_count_28d_7d"]) + "+odfvTransformation",
    }

odfv_fs = FeatureService(
    name='odfv_fs',
    features=[odfv_with_bfv_input],
    online_serving_enabled=True,
    options={"DEV_USE_CACHED_FEATURES": "true"},
)


fs1 = FeatureService(
    name='fs1',
    features=[buyer_features1, buyer_features2, buyer_features3, seller_features1, seller_features2, seller_features3, seller_features4, product_features1, product_features2, product_features3],
    online_serving_enabled=True,
    options={"DEV_USE_CACHED_FEATURES": "true"},
)

fs2 = FeatureService(
    name='fs2',
    features=[seller_features1, seller_features2, seller_features3, seller_features4, product_features1, product_features2, product_features3],
    online_serving_enabled=True,
    options={"DEV_USE_CACHED_FEATURES": "true"},
)

fs3 = FeatureService(
    name='fs3',
    features=[buyer_features1, buyer_features2, buyer_features3, seller_features1, seller_features2, seller_features3, seller_features4],
    online_serving_enabled=True,
    options={"DEV_USE_CACHED_FEATURES": "true"},
)