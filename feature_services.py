from tecton import FileConfig, BatchSource, Entity, batch_feature_view
from tecton.types import Field, String, Int64

test_datasource = BatchSource(
  name='test_datasource',
  batch_config=FileConfig(
    uri='s3://tecton.ai.public/benchmark/kevinz/loadtesting_data_final.pq',
    file_format='parquet',
    timestamp_field='timestamp',
  ),
  owner='kzhang@tecton.ai',
  tags={'release': 'test'},
)

customer = Entity(name='customer', join_keys=[Field('cust_id', Int64)])
merchant = Entity(name='merchant', join_keys=[Field('merchant_id', Int64)])

from tecton import Aggregate
from datetime import datetime, timedelta
from tecton import FeatureService, BatchTriggerType
from tecton.aggregation_functions import approx_count_distinct, approx_percentile
from tecton.aggregation_functions import last, last_distinct


@batch_feature_view(
    sources=[test_datasource],
    entities=[merchant, customer],
    mode='spark_sql',
    aggregation_interval=timedelta(hours=4),
    features=[
        Aggregate(input_column=Field('test_0', Int64), function='max', time_window=timedelta(hours=400)),
    ],
    tecton_materialization_runtime="1.0.20",
    online=True,
    offline=False,
    feature_start_time=datetime(2020, 10, 10),
    timestamp_field="timestamp",
    batch_trigger=BatchTriggerType.MANUAL,  # Use manual triggers
)
def fv_max(data):
    return f"""
        SELECT
            merchant_id,
            cust_id,
            col4 as test_0,
            timestamp
        FROM
            {data}
        """

fv_max_400h_fs = FeatureService(
    name='fv_max_400h_fs',
    features=[fv_max]
)


@batch_feature_view(
    sources=[test_datasource],
    entities=[merchant, customer],
    mode='spark_sql',
    aggregation_interval=timedelta(hours=4),
    features=[
        Aggregate(input_column=Field('test_0', Int64), function='mean', time_window=timedelta(hours=400)),
    ],
    tecton_materialization_runtime="1.0.20",
    online=True,
    offline=False,
    feature_start_time=datetime(2020, 10, 10),
    timestamp_field="timestamp",
    batch_trigger=BatchTriggerType.MANUAL,  # Use manual triggers
)
def fv_mean(data):
    return f"""
        SELECT
            merchant_id,
            cust_id,
            col4 as test_0,
            timestamp
        FROM
            {data}
        """

fv_mean_400h_fs = FeatureService(
    name='fv_mean_400h_fs',
    features=[fv_mean]
)


@batch_feature_view(
    sources=[test_datasource],
    entities=[merchant, customer],
    mode='spark_sql',
    aggregation_interval=timedelta(hours=4),
    features=[
        Aggregate(input_column=Field('test_0', Int64), function='var_samp', time_window=timedelta(hours=400)),
    ],
    tecton_materialization_runtime="1.0.20",
    online=True,
    offline=False,
    feature_start_time=datetime(2020, 10, 10),
    timestamp_field="timestamp",
    batch_trigger=BatchTriggerType.MANUAL,  # Use manual triggers
)
def fv_var_samp(data):
    return f"""
        SELECT
            merchant_id,
            cust_id,
            col4 as test_0,
            timestamp
        FROM
            {data}
        """

fv_var_samp_400h_fs = FeatureService(
    name='fv_var_samp_400h_fs',
    features=[fv_var_samp]
)


@batch_feature_view(
    sources=[test_datasource],
    entities=[merchant, customer],
    mode='spark_sql',
    aggregation_interval=timedelta(hours=4),
    features=[
        Aggregate(input_column=Field('test_0', Int64), function='var_pop', time_window=timedelta(hours=400)),
    ],
    tecton_materialization_runtime="1.0.20",
    online=True,
    offline=False,
    feature_start_time=datetime(2020, 10, 10),
    timestamp_field="timestamp",
    batch_trigger=BatchTriggerType.MANUAL,  # Use manual triggers
)
def fv_var_pop(data):
    return f"""
        SELECT
            merchant_id,
            cust_id,
            col4 as test_0,
            timestamp
        FROM
            {data}
        """

fv_var_pop_400h_fs = FeatureService(
    name='fv_var_pop_400h_fs',
    features=[fv_var_pop]
)


@batch_feature_view(
    sources=[test_datasource],
    entities=[merchant, customer],
    mode='spark_sql',
    aggregation_interval=timedelta(hours=4),
    features=[
        Aggregate(input_column=Field('test_0', Int64), function=approx_count_distinct(precision=12), time_window=timedelta(hours=400)),
    ],
    tecton_materialization_runtime="1.0.20",
    online=True,
    offline=False,
    feature_start_time=datetime(2020, 10, 10),
    timestamp_field="timestamp",
    batch_trigger=BatchTriggerType.MANUAL,  # Use manual triggers
)
def fv_approx_count(data):
    return f"""
        SELECT
            merchant_id,
            cust_id,
            col5 as test_0,
            timestamp
        FROM
            {data}
        """

fv_approx_count_400h_fs = FeatureService(
    name='fv_approx_count_400h_fs',
    features=[fv_approx_count]
)


@batch_feature_view(
    sources=[test_datasource],
    entities=[merchant, customer],
    mode='spark_sql',
    aggregation_interval=timedelta(hours=4),
    features=[
        Aggregate(input_column=Field('test_0', Int64), function=approx_percentile(percentile=0.5, precision=100), time_window=timedelta(hours=400)),
    ],
    tecton_materialization_runtime="1.0.20",
    online=True,
    offline=False,
    feature_start_time=datetime(2020, 10, 10),
    timestamp_field="timestamp",
    batch_trigger=BatchTriggerType.MANUAL,  # Use manual triggers
)
def fv_approx_percentile(data):
    return f"""
        SELECT
            merchant_id,
            cust_id,
            col4 as test_0,
            timestamp
        FROM
            {data}
        """

fv_approx_percentile_400h_fs = FeatureService(
    name='fv_approx_percentile_400h_fs',
    features=[fv_approx_percentile]
)


@batch_feature_view(
    sources=[test_datasource],
    entities=[merchant, customer],
    mode='spark_sql',
    aggregation_interval=timedelta(hours=4),
    features=[
        Aggregate(input_column=Field('last_col', String), function=last(5), time_window=timedelta(hours=400)),
    ],
    tecton_materialization_runtime="1.0.20",
    online=True,
    offline=False,
    feature_start_time=datetime(2020, 10, 10),
    timestamp_field="timestamp",
    batch_trigger=BatchTriggerType.MANUAL,  # Use manual triggers
)
def last5_fv(data):
    return f"""
        SELECT
            merchant_id,
            cust_id,
            CAST(col3 as STRING) as last_col,
            timestamp
        FROM
            {data}
        """

last5_fv_400h_fs = FeatureService(
    name='last5_fv_400h_fs',
    features=[last5_fv]
)


@batch_feature_view(
    sources=[test_datasource],
    entities=[merchant, customer],
    mode='spark_sql',
    aggregation_interval=timedelta(hours=4),
    features=[
        Aggregate(input_column=Field('last_col', String), function=last_distinct(5), time_window=timedelta(hours=400)),
    ],
    tecton_materialization_runtime="1.0.20",
    online=True,
    offline=False,
    feature_start_time=datetime(2020, 10, 10),
    timestamp_field="timestamp",
    batch_trigger=BatchTriggerType.MANUAL,  # Use manual triggers
)
def last_distinct5_fv(data):
    return f"""
        SELECT
            merchant_id,
            cust_id,
            CAST(col3 as STRING) as last_col,
            timestamp
        FROM
            {data}
        """

last_distinct5_fv_400h_fs = FeatureService(
    name='last_distinct5_fv_400h_fs',
    features=[last_distinct5_fv]
)


@batch_feature_view(
    sources=[test_datasource],
    entities=[merchant, customer],
    mode='spark_sql',
    aggregation_interval=timedelta(hours=4),
    features=[
        Aggregate(input_column=Field('last_col', String), function=last(100), time_window=timedelta(hours=400)),
    ],
    tecton_materialization_runtime="1.0.20",
    online=True,
    offline=False,
    feature_start_time=datetime(2020, 10, 10),
    timestamp_field="timestamp",
    batch_trigger=BatchTriggerType.MANUAL,  # Use manual triggers
)
def last100_fv(data):
    return f"""
        SELECT
            merchant_id,
            cust_id,
            CAST(col3 as STRING) as last_col,
            timestamp
        FROM
            {data}
        """

last100_fv_400h_fs = FeatureService(
    name='last100_fv_400h_fs',
    features=[last100_fv]
)


@batch_feature_view(
    sources=[test_datasource],
    entities=[merchant, customer],
    mode='spark_sql',
    aggregation_interval=timedelta(hours=4),
    features=[
        Aggregate(input_column=Field('last_col', String), function=last_distinct(100), time_window=timedelta(hours=400)),
    ],
    tecton_materialization_runtime="1.0.20",
    online=True,
    offline=False,
    feature_start_time=datetime(2020, 10, 10),
    timestamp_field="timestamp",
    batch_trigger=BatchTriggerType.MANUAL,  # Use manual triggers
)
def last_distinct100_fv(data):
    return f"""
        SELECT
            merchant_id,
            cust_id,
            CAST(col3 as STRING) as last_col,
            timestamp
        FROM
            {data}
        """

last_distinct100_fv_400h_fs = FeatureService(
    name='last_distinct100_fv_400h_fs',
    features=[last_distinct100_fv]
)


@batch_feature_view(
    sources=[test_datasource],
    entities=[merchant, customer],
    mode='spark_sql',
    aggregation_interval=timedelta(hours=4),
    features=[
        Aggregate(input_column=Field('last_col', String), function='last', time_window=timedelta(hours=400)),
    ],
    tecton_materialization_runtime="1.0.20",
    online=True,
    offline=False,
    feature_start_time=datetime(2020, 10, 10),
    timestamp_field="timestamp",
    batch_trigger=BatchTriggerType.MANUAL,  # Use manual triggers
)
def last_fv(data):
    return f"""
        SELECT
            merchant_id,
            cust_id,
            CAST(col3 as STRING) as last_col,
            timestamp
        FROM
            {data}
        """

last_fv_400h_fs = FeatureService(
    name='last_fv_400h_fs',
    features=[last_fv]
)


