# benchmark-selfserve
Clone this repository and follow the instructions to benchmark Tecton

## Quick Start Guide
`feature_services.py` already exists, but if you want to make changes to how it's generated,
you can modify `gen_test_features.py` then run it to re-generate `feature_services.py`

Once you're happy with the Feature Services in `feature_services.py`, do the following
to apply the changes to your cluster:
1. `tecton login <url>` to log into your cluster
2. `tecton workspace select <workspace>` to select your bench workspace
    - If you need to create the workspace, run `tecton workspace create --live <workspace>`
3. `cd` to this repo then run `tecton apply` to apply the Feature Services in `feature_services.py`

## What's In This Repo
`gen_test_features.py` generates `feature_services.py`, which defines a few things:

### Data Source
Parquet file on S3 that's available in a public bucket (any cluster/person can access the data)
```
test_datasource = BatchSource(
  name='test_datasource',
  batch_config=FileConfig(
    uri='s3://tecton.ai.public/data/load_testing_data.pq',
    file_format='parquet',
    timestamp_field='timestamp',
  ),
  owner='david@tecton.ai',
  tags={'release': 'test'},
)
```

### Feature Services
- `load_test_feature_service_non_aggregate_1_feature_views`
    - 1 non-aggregate feature view in the feature service
- `load_test_feature_service_non_aggregate_2_feature_views`
    - 2 non-aggregate feature view in the feature service
- `load_test_feature_service_non_aggregate_4_feature_views`
    - 4 non-aggregate feature view in the feature service
- `load_test_feature_service_non_aggregate_14_feature_views`
    - 14 non-aggregate feature view in the feature service
- `load_test_feature_service_mixed_5_feature_views`
    - 5 feature views in the feature service
    - N aggregate and M non-aggregate feature views
- `load_test_feature_service_mixed_10_feature_views`
    - 10 feature views in the feature service
    - N aggregate and M non-aggregate feature views
- `load_test_feature_service_mixed_18_feature_views`
    - 18 feature views in the feature service
    - N aggregate and M non-aggregate feature views
- `load_test_feature_service_mixed_58_feature_views`
    - 58 feature views in the feature service
    - N aggregate and M non-aggregate feature views

### Join Keys
* `customer_id` 1 through 50
* `merchant_id` 1 through 50
