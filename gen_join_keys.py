#!/usr/bin/env python3
import argparse
from pathlib import Path
from typing import Any, Dict, List
from base64 import b64encode
import json
import os

from feature_services import ALL_FEATURE_SERVICES


JK_DIR = Path(__file__).parent / "join_keys"


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


def get_b64_requests(api_url: str, ws_name: str, fs_name: str, jk_maps) -> List[str]:
    return


def clean_jk_dir():
    try:
        JK_DIR.mkdir(parents=True, exist_ok=True)
        for jk_file in JK_DIR.iterdir():
            os.remove(jk_file)
        os.rmdir(JK_DIR)
    except:
        pass
    JK_DIR.mkdir(parents=True, exist_ok=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("cluster_url", type=str, help="Tecton cluster URL")
    parser.add_argument("ws_name", type=str, help="Workspace name")
    args = parser.parse_args()
    api_url = f"https://{args.cluster_url}/api/v1/feature-service/get-features"

    values = list(range(1, 51))
    jk_maps = [
        {
            "customer_id": str(val1),
            "merchant_id": str(val2),
        }
       for val1 in values
       for val2 in values
    ]
    print(f"{len(jk_maps)} join-key maps")

    clean_jk_dir()

    for fs_name in ALL_FEATURE_SERVICES:
        b64_requests = [web_req_with_b64_body(api_url, fs_name, args.ws_name, jk_map) for jk_map in jk_maps]
        fs_file = JK_DIR / fs_name
        fs_file.write_text("\n".join(b64_requests))


if __name__ == '__main__':
    main()
