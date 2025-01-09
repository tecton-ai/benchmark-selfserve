import argparse
import json
from datetime import datetime
import pytz

import numpy as np
import requests


URL = "https://dev-serving.tecton.ai/api/v1/feature-service/get-features"

HEADERS = {"Authorization": "Tecton-key "}
dt = datetime(2025, 1, 1, 0, 0, 0)
SAMPLE_PARAMS = {
    "params": {
        "feature_service_name": "fv_mean_400h_fs",
        "join_key_map": {
        "cust_id": str(50),
        "merchant_id": str(50),
        },
        "workspace_name": "kz_test",
        "metadata_options": {"include_serving_status": True, "include_slo_info": True, "include_names": True, "includeEffectiveTimes": True},
        "requestOptions": {
            "highWatermarkOverride": dt.astimezone(pytz.UTC).isoformat(),
        }
    }
}
def get_feature():
    x = requests.post(URL, json=SAMPLE_PARAMS, headers=HEADERS)
    if x.status_code != 200:
        def pretty_print_json(s):
            js = json.loads(s)
            print(json.dumps(js, indent=2))
            print("\n")
        print(x.status_code)
    else:
        print(x.text)
if __name__ == "__main__":
    get_feature()