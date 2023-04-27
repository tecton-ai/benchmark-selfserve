#!/usr/bin/env python3
import argparse
from pathlib import Path
from typing import Any, Dict, List
from base64 import b64encode
import json
import os

from feature_services import ALL_FEATURE_SERVICES


REQS_DIR = Path(__file__).parent / "requests"


def req_params(fs_name: str, ws_name: str, jk_map: Dict[str, str]) -> Dict[str, Any]:
    return {
        "params": {
            "feature_service_name": fs_name,
            "join_key_map": jk_map,
            "workspace_name": ws_name,
        }
    }


def web_req_with_b64_body(api_url: str, fs_name: str, ws_name: str, jk_map: Dict[str, str]) -> str:
    params_json = json.dumps(req_params(fs_name, ws_name, jk_map))
    return json.dumps(
        {
            "method": "POST",
            "url": api_url,
            "body": b64encode(params_json.encode("utf-8")).decode("utf-8"),
        }
    )


def clean_reqs_dir():
    try:
        REQS_DIR.mkdir(parents=True, exist_ok=True)
        for req_file in REQS_DIR.iterdir():
            os.remove(req_file)
        os.rmdir(REQS_DIR)
    except:
        pass
    REQS_DIR.mkdir(parents=True, exist_ok=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("cluster_url", type=str, help="Tecton cluster URL")
    parser.add_argument("ws_name", type=str, help="Workspace name")
    args = parser.parse_args()
    api_url = f"https://{args.cluster_url}/api/v1/feature-service/get-features"

    values = list(range(1, 51))
    jk_maps = [
        {
            "cust_id": str(val1),
            "merchant_id": str(val2),
        }
       for val1 in values
       for val2 in values
    ]
    print(f"Generated {len(jk_maps)} distinct requests per feature service")

    clean_reqs_dir()
    for fs_name in ALL_FEATURE_SERVICES:
        b64_requests = [web_req_with_b64_body(api_url, fs_name, args.ws_name, jk_map) for jk_map in jk_maps]
        fs_file = REQS_DIR / fs_name
        fs_file.write_text("\n".join(b64_requests))


if __name__ == '__main__':
    main()
