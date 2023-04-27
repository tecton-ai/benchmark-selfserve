#!/usr/bin/env python3
import hashlib
import os
from pathlib import Path
import stat

OUT_FILE = Path(__file__).parent / 'feature_services.py'
RUN_SCRIPT = Path(__file__).parent / 'run_vegeta_all.sh'

N_MIXED_FS = 3


header =  """from tecton import FileConfig, BatchSource, Entity, batch_feature_view


test_datasource = BatchSource(
  name='test_datasource',
  batch_config=FileConfig(
    uri='s3://tecton.ai.public/data/load_testing_data.pq',
    file_format='parquet',
    timestamp_field='timestamp',
  ),
  owner='rohit@tecton.ai',
  tags={'release': 'test'},
)

customer = Entity(name='customer', join_keys=['cust_id'])
merchant = Entity(name='merchant', join_keys=['merchant_id'])

from tecton import Aggregation, FilteredSource
from datetime import datetime, timedelta
from tecton import FeatureService
"""


def gen_lifetime_feature(
    num_features,
    feature_name,
    start_year,
    start_month,
    start_day,
):
    features = ""
    for i in range(num_features):
        features += f"            amount/{i} as test_{i},"
        if i != num_features-1:
            features += "\n"
    return f"""
@batch_feature_view(
    sources=[FilteredSource(test_datasource)],
    entities=[customer],
    mode='spark_sql',
    online=True,
    offline=False,
    feature_start_time=datetime({start_year}, {start_month}, {start_day}),
    batch_schedule=timedelta(days=1),
    ttl=timedelta(days=3650),
)
def {feature_name}(data):
    return f'''
        SELECT
            cust_id,
{features}
            timestamp
        FROM
            {{data}}
        '''
"""

def gen_agg_feature(
    num_features,
    feature_name,
    entity,
    join_key,
    time_window,
    slide_period,
    start_year,
    start_month,
    start_day,
    agg_function='sum',
):
    features = ""
    feature_names = []
    for i in range(num_features):
        name = f"test_{i}"
        features += f"            amount/{i} as {name},"
        if i != num_features-1:
            features += "\n"
        feature_names.append(name)

    aggregation_block = ""
    for name in feature_names:
        aggregation_block += f"        Aggregation(column='{name}', function='{agg_function}', time_window=timedelta(days={time_window})),\n"
    return f"""
@batch_feature_view(
    sources=[FilteredSource(test_datasource)],
    entities=[{entity}],
    mode='spark_sql',
    aggregation_interval=timedelta(days={slide_period}),
    aggregations=[
{aggregation_block}
    ],
    online=True,
    offline=False,
    feature_start_time=datetime({start_year}, {start_month}, {start_day}),
)
def {feature_name}(data):
    return f'''
        SELECT
            {join_key},
{features}
            timestamp
        FROM
            {{data}}
        '''
"""

def gen_feature_service(name, features):
    return f"""
{name} = FeatureService(
    name='{name}',
    features={str(features).replace("'", "")}
)
"""

def main():
    NUM_FEATURES = 5000
    max_features_per_feature_service = 100
    splits = [
        ("lifetime", None, (63, 62, 125, 1000)),
        ("28", "7", (62, 63, 125, 1000)),
        ("7", "1", (62, 63, 125, 1000)),
        ("1", "1", (50, 50, 100, 800)),
        ('336', "7", (13, 12, 25, 200))
    ]
    #
    # splits = [
    #     ("lifetime", None, (1,2, 3, 4)),
    #     ("28d", "7d", (1,2, 3, 4)),
    #     ("7d", "1d", (1,2, 3, 4)),
    #     ("1d", "1h", (1,2, 3, 4)),
    #     ('336d', "7d", (1,2, 3, 4))
    # ]
    start_year=2020
    start_month=10
    start_day=10

    feature_view_num = 0


    fs = {
        i: []
        for i in range(2*N_MIXED_FS)
    }

    code = header
    last = "merchant"

    for window, slide_period, counts in splits:
        for i, count in enumerate(counts):
            subcounts = []
            if count > max_features_per_feature_service:
                subcounts += [max_features_per_feature_service] * int(count / max_features_per_feature_service)
                if count % max_features_per_feature_service > 0:
                    subcounts += [count % max_features_per_feature_service]
            else:
                subcounts = [count]
            for num_features in subcounts:
                if window == "lifetime":
                    feature_name = "load_test_lifetime_" + hashlib.sha1(repr(feature_view_num).encode()).hexdigest()
                    feature_code = gen_lifetime_feature(
                        num_features,
                        feature_name,
                        start_year,
                        start_month,
                        start_day,
                    )
                else:
                    feature_name = "load_test_window_" + window + "_" + hashlib.sha1(repr(feature_view_num).encode()).hexdigest()

                    if last == "merchant":
                        entity = 'customer'
                        join_key = 'cust_id'
                        last = "customer"
                    else:
                        entity = 'merchant'
                        join_key = 'merchant_id'
                        last = 'merchant'
                    feature_code = gen_agg_feature(
                        num_features,
                        feature_name,
                        entity,
                        join_key,
                        window,
                        slide_period,
                        start_year,
                        start_month,
                        start_day
                    )
                code += feature_code
                fs[i].append(feature_name)
                feature_view_num += 1

    fs_prefix = "fs"
    all_feature_services = []
    for i in range(N_MIXED_FS):
        features = fs[i]
        if i > 0:
            fs[i] += fs[i-1]
        name = f"{fs_prefix}_mixed_{len(features)}_feature_views"
        all_feature_services.append(name)
        code += gen_feature_service(name, features)
    for i in range(N_MIXED_FS, N_MIXED_FS*2):
        features = fs[i-N_MIXED_FS]
        tfv_features = []
        for feature in features:
            if "load_test_lifetime" in feature:
                tfv_features.append(feature)
        name = f"{fs_prefix}_non_aggregate_{len(tfv_features)}_feature_views"
        all_feature_services.append(name)
        code += gen_feature_service(name, tfv_features)

    fs_names = "\n".join([f'    "{fs_name}",' for fs_name in all_feature_services])
    code += f"""
ALL_FEATURE_SERVICES = [
{fs_names}
]
    """

    try:
        os.remove(OUT_FILE)
    except:
        pass
    try:
        os.remove(RUN_SCRIPT)
    except:
        pass

    OUT_FILE.write_text(code)

    script_lines = "\n".join([
        f"./run_vegeta.py --service {fs_name} --file -r 5 -d 10 -t 5000 &" for fs_name in all_feature_services
    ])

    # Now codegen the run-all script
    script = f"""#!/bin/sh

#
# Use this as a "workspace" to loadtest multiple services as the same time,
# or just as a reference to paste line(s) into your console.
#

{script_lines}

"""
    RUN_SCRIPT.write_text(script)
    RUN_SCRIPT.chmod(RUN_SCRIPT.stat().st_mode | stat.S_IEXEC)


if __name__ == '__main__':
    main()
