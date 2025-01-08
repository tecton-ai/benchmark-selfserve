#!/usr/bin/env python3
import hashlib
import os
from pathlib import Path
import stat
from datetime import datetime

OUT_FILE = Path(__file__).parent / 'feature_services.py'
RUN_SCRIPT = Path(__file__).parent / 'run_vegeta_all.sh'

N_MIXED_FS = 3


header =  """from tecton import FileConfig, BatchSource, Entity, batch_feature_view
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
"""

def generate_last_fv(column_name, function_name, aggregation_interval_hours=5, num_tiles=100, agg_func="last", n=5):
    func = f"{agg_func}({n})"
    if n == 1:
        func = f"'last'"
    code = f"""@batch_feature_view(
    sources=[test_datasource],
    entities=[merchant, customer],
    mode='spark_sql',
    aggregation_interval=timedelta(hours={aggregation_interval_hours}),
    features=[
        Aggregate(input_column=Field('{column_name}', String), function={func}, time_window=timedelta(hours={aggregation_interval_hours * num_tiles})),
    ],
    tecton_materialization_runtime="1.0.20",
    online=True,
    offline=False,
    feature_start_time=datetime(2020, 10, 10),
    timestamp_field="timestamp",
    batch_trigger=BatchTriggerType.MANUAL,  # Use manual triggers
)
def {function_name}(data):
    return f\"\"\"
        SELECT
            merchant_id,
            cust_id,
            CAST(col3 as STRING) as {column_name},
            timestamp
        FROM
            {{data}}
        \"\"\"

{function_name}_{aggregation_interval_hours*num_tiles}h_fs = FeatureService(
    name='{function_name}_{aggregation_interval_hours*num_tiles}h_fs',
    features=[{function_name}]
)
"""
    return code


def generate_feature_view_code(
    function_name: str,
    column_name: str = 'test_0',
    agg_function: str = 'sum',
    aggregation_interval_hours=5,
    num_tiles=100,
    agg_col: str = "col4",
):
    """
    Generate a code snippet of a batch feature view definition and corresponding feature service.

    Parameters:
    -----------
    function_name: str
        The name of the feature view function (e.g., 'single_sum_7d').
    column_name: str
        The column to be aggregated.
    agg_function: str
        The aggregation function to apply (e.g., 'sum', 'avg', etc.).
    time_window_days: int
        The number of days for the aggregation time window.
    start_date: datetime
        The feature start time.

    Returns:
    --------
    str
        A string containing the code snippet for the batch feature view and feature service.
    """
    code = f"""@batch_feature_view(
    sources=[test_datasource],
    entities=[merchant, customer],
    mode='spark_sql',
    aggregation_interval=timedelta(hours={aggregation_interval_hours}),
    features=[
        Aggregate(input_column=Field('{column_name}', Int64), function={agg_function}, time_window=timedelta(hours={aggregation_interval_hours * num_tiles})),
    ],
    tecton_materialization_runtime="1.0.20",
    online=True,
    offline=False,
    feature_start_time=datetime(2020, 10, 10),
    timestamp_field="timestamp",
    batch_trigger=BatchTriggerType.MANUAL,  # Use manual triggers
)
def {function_name}(data):
    return f\"\"\"
        SELECT
            merchant_id,
            cust_id,
            {agg_col} as {column_name},
            timestamp
        FROM
            {{data}}
        \"\"\"

{function_name}_{aggregation_interval_hours*num_tiles}h_fs = FeatureService(
    name='{function_name}_{aggregation_interval_hours*num_tiles}h_fs',
    features=[{function_name}]
)
"""
    return code
agg_functions = [
    ("'max'","max", "col4"),
    ("'mean'","mean", "col4"),
    ("'var_samp'", "var_samp", "col4"),
    ("'var_pop'", "var_pop", "col4"),
    ("approx_count_distinct(precision=12)", "approx_count", "col5"),
    ("approx_percentile(percentile=0.5, precision=100)", "approx_percentile", "col4"),
]


def write():
    code = ""
    code += header + "\n\n"
    for agg, name, agg_col in agg_functions:
        code += generate_feature_view_code(f"fv_{name}", aggregation_interval_hours=4, num_tiles=100, agg_function=agg, agg_col =agg_col)
        code += "\n\n"

    for n in [5, 100]:
        code += generate_last_fv("last_col", f"last{n}_fv", aggregation_interval_hours=4, num_tiles=100, agg_func="last", n=n)
        code += "\n\n"
        code += generate_last_fv("last_col", f"last_distinct{n}_fv", aggregation_interval_hours=4, num_tiles=100, agg_func="last_distinct", n=n)
        code += "\n\n"

    code += generate_last_fv("last_col", f"last_fv", aggregation_interval_hours=4, num_tiles=100, agg_func="last", n=1)
    code += "\n\n"

    try:
        os.remove(OUT_FILE)
    except:
        pass

    OUT_FILE.write_text(code)
write()