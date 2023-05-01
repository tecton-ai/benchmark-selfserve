# benchmark-selfserve
Clone this repository and follow the instructions to benchmark Tecton

## What's In This Repo
The empty `.tecton` file is just so you don't have to run `tecton init`.

`gen_feature_services.py` generates `feature_services.py`, which defines a few things:

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
- `fs_non_aggregate_1_feature_views`
    - 1 non-aggregate feature view
- `fs_non_aggregate_2_feature_views`
    - 2 non-aggregate feature view
- `fs_non_aggregate_4_feature_views`
    - 4 non-aggregate feature view
- `fs_mixed_5_feature_views`
    - 5 feature views
    - 4 aggregate and 1 non-aggregate feature views
- `fs_mixed_10_feature_views`
    - 10 feature views
    - 8 aggregate and 2 non-aggregate feature views
- `fs_mixed_18_feature_views`
    - 18 feature views
    - 14 aggregate and 4 non-aggregate feature views

### Join Keys
* `cust_id` 1 through 50
* `merchant_id` 1 through 50

# How-to
`feature_services.py` already exists, but if you want to make changes to how it's generated,
you can modify `gen_feature_services.py` then run it to re-generate `feature_services.py`.

## Requirements
* Python 3
* [Vegeta](https://github.com/tsenart/vegeta) installed and in the path

## 1. Workspace Setup
Once you're happy with the Feature Services in `feature_services.py`, do the following
to apply the changes to your cluster:
1. `tecton login <domain>` to log into your cluster
    - e.g. `tecton login yourcluster.tecton.ai`
2. `tecton workspace select <workspace>` to select your bench workspace
    - If you need to create the workspace, run `tecton workspace create --live <workspace>`
3. `cd` to this repo then run `tecton apply` to apply the Feature Services in
`feature_services.py`

`tecton apply` starts materialization, which can take a while to finish. You can check the
status in the web UI:
1. Ensure your new workspace is selected near the top left via the drop-down
2. Click Services on the left
3. Click a service
4. Click the Materialization tab (to the right of the Overview tab that's selected by default)

**IMPORTANT**: Set the environment variable `TECTON_API_KEY` to the API Key of a Tecton
user (may be a service user), and add that user as a Consumer of your bench workspace:
1. Go to the Tecton web UI
2. Click "Accounts & Access" on the left
3. Find the user associated with the `TECTON_API_KEY` and click on them
4. Click "Assign Workspace Access"
5. Select your bench workspace and click the "Consumer" role
6. Click "Add" in the bottom right

## 2. Generate Requests
In order to generate the requests that will be sent to the get-features API, you need to
run the `gen_requests` script like so:
```
./gen_requests.py <domain> <workspace>
```
e.g.
```
./gen_requests.py yourcluster.tecton.ai benchmarking
```

This populates the `requests` directory where each file represents a feature service's
requests that will be sent by Vegeta.


## 3. Run Vegeta
To load a feature service, run:
```
./run_vegeta.py -s <feature_service>
```
Note that the `feature_service` must correspond to a file in the `requests` directory,
i.e. one of the feature services from [Step 1](#1-workspace-setup) that you generated
requests for in [Step 2](#2-generate-requests).

You can also specify the RPS (`-r`), duration (`-d`), and timeout (`-t`). The `--file`
flag tells it to output to a file whose name is the same as the service name, in the
`vegeta_out` directory.

You can optionally run the `run_vegeta_all.sh` script to test some or all of the feature
services at the same time, or just use it as a reference to copy and paste
`./run_vegeta.py` commands into your console.

Big red button to kill all vegeta load tests:
```
ps aux | grep vegeta | awk '{print $2}' | xargs kill
```

### Caveat
Load tests in production clusters **will affect your production traffic**, even if your
your feature services are in a separate workspace. If you are unsure if your cluster can
handle the load you'd like to test, please reach out to your Tecton points of contact to
discuss and provision appropriately. Also, as a rule of thumb, this should be good up to
10,000 RPS. Above that, we will likely need something more sophisticated, e.g. a
distributed load driver.

# Metrics
**DISCLAIMER**: You may see a small warmup period (usually lasts a few seconds max) of
higher latency if you go from 0 RPS to hundreds of RPS. The following numbers are measured
after that initial period. Moreover, these numbers were recorded from values that the
Feature Server itself exports, so Vegeta latency numbers will include things like HTTP
round-trip time, whereas these numbers represent only the `get-features` RPC duration.

Note that Dynamo is the online store. With 100 RPS, this is what we got (abbreviating the
feature server names):

## Response Size Per Query
All values are in KiB:
| Feature Service | p50 | p90 | p95 | p99 |
| --------------- | --- | --- | --- | --- |
| `mixed_5`  | 0.729 | 0.927 | 0.952 | 0.972 |
| `mixed_10` | 1.46 | 1.86 | 1.90 | 1.94 |
| `mixed_18` | 3.41 | 4.59 | 4.74 | 4.85 |
| `nonagg_1` | 0.732 | 0.928 | 0.952 | 0.972 |
| `nonagg_2` | 1.46 | 1.86 | 1.90 | 1.94 |
| `nonagg_4` | 3.42 | 4.59 | 4.74 | 4.85 |

## Latency
All values are in ms:
| Feature Service | p50 | p90 | p95 | p99 |
| --------------- | --- | --- | --- | --- |
| `mixed_5`  | 5.67 | 10.5 | 17.0 | 40.9 |
| `mixed_10` | 8.27 | 18.8 | 28.6 | 54.5 |
| `mixed_18` | 13.4 | 28.9 | 38.9 | 74.1 |
| `nonagg_1` | 4.09 | 6.17 | 7.94 | 18.8 |
| `nonagg_2` | 4.69 | 7.65 | 9.91 | 25.9 |
| `nonagg_4` | 5.84 | 11.3 | 16.8 | 37.4 |
