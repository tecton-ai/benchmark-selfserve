#!/usr/bin/env python3
import argparse
import json
import os
import random
import subprocess
import time
import traceback
from base64 import b64encode
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from pprint import pformat
from threading import Thread
from typing import Any, Dict, Final, List, Set

import numpy as np
import pandas as pd
import requests as re

import tecton
from tecton import conf
from tecton.cli.command import _cluster_url
from tecton.framework.feature_service import FeatureService
from tecton.framework.feature_view import FeatureReference, FeatureView
from tecton.framework.workspace import Workspace


class Util:
    @staticmethod
    def shell(cmd_parts: List[str], print_stdout=False) -> str:
        print(f"Running `{' '.join(cmd_parts)}`...")
        res = subprocess.run(cmd_parts, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")
        if res.returncode != 0:
            print(f"Return code {res.returncode}\nStdout:\n{res.stdout}\n\nStderr:\n{res.stderr}\n", flush=True)
        elif print_stdout:
            print(res.stdout)
        return res.stdout




# TODO @nocommit Put this back up
N_JOIN_KEYS = 1000

THIS_FOLDER: Final[Path] = Path(__file__).parent
JK_DIR: Final[Path] = THIS_FOLDER / "join_keys"
FILE_EXT: Final[str] = "csv"

if "TECTON_API_KEY" not in os.environ:
    print("Set the environment variable TECTON_API_KEY")
    exit(1)
TECTON_API_KEY: Final[str] = os.environ["TECTON_API_KEY"]

MD_METRICS: Set[str] = {
    "sloServerTimeSeconds",
    "dynamodbResponseSizeBytes",
    "serverTimeSeconds",
    "storeMaxLatency",
    "storeResponseSizeBytes",
}



# ----------------------------------------------------------------------------------------
#                              Dealing with Tecton stuff
# ----------------------------------------------------------------------------------------
def get_cluster() -> str:
    cmd_parts: List[str] = [
        "kubectl",
        "config",
        "current-context",
    ]
    res = subprocess.run(cmd_parts, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    cluster = res.stdout.decode("utf-8").strip()
    if "desktop" in cluster:
        raise Exception("Connected to a desktop cluster. " "Please `tectonctl login` beore running this script.")
    return cluster


def get_workspace(ws_name: str) -> Workspace:
    try:
        workspace: Workspace = tecton.get_workspace(ws_name)
        assert _cluster_url() is not None
    except Exception:
        raise Exception(
            f"Could not connect to workspace '{ws_name}'. Make sure to:\n"
            "1) Log into the cluster via `tectonctl login <cluster-name> --aws`,\n"
            "2) Log into Tecton via `tecton login <cluster-URL>`,\n"
            "3) Ensure the existence of the workspace, and\n"
            "4) Have `tectonctl vpn` running in the cluster"
        )
    return workspace


def get_join_keys(fs: FeatureService) -> pd.DataFrame:
    feature_refs: List[FeatureReference] = fs.features
    feature_views: List[FeatureView] = [fr.feature_definition for fr in feature_refs]
    print(f"Found {len(feature_views)} Feature Views")

    # The join keys of the FS is the union of all its FV's join keys
    fs_jks: Set[str] = set()
    for fv in feature_views:
        fs_jks.update(fv.join_keys)
    fs_jks_list = sorted(fs_jks)

    fv_with_all_jk: List[FeatureView] = []
    for fv in feature_views:
        # if fv.is_on_demand:
        #     print(f"\tFV {fv.name} is on-demand. Skipping!")
        #     continue
        fv_jks: Set[str] = set(fv.join_keys)
        if fv_jks != fs_jks:
            print(f"\tFV {fv.name} join keys {fv_jks} aren't the FS's join keys {fs_jks}. Skipping!")
            continue
        print(f"\tKeeping FV {fv.name}")
        fv_with_all_jk.append(fv)

    if len(fv_with_all_jk) == 0:
        raise Exception("No Feature Views with all of the Service's join keys. Terminating!")

    print(f"\n{len(fv_with_all_jk)} Feature View(s) had all join keys:")
    for fv in fv_with_all_jk:
        print(f"\t{fv.name}")

    print(f"\nRetrieving historical features for {fs.name}")
    df_all = pd.DataFrame([], columns=fs_jks_list)
    for fv in fv_with_all_jk:
        print(f"\n\tFeature view: {fv.name}\n")
        print(f"{fv.summary()}")

        today = datetime.now()
        yesterday = today - timedelta(days=1)
        features = fv.get_historical_features(
            start_time=yesterday,
            end_time=today,
        ).to_pandas()

        df_fv = features[fs_jks_list]
        df_all = pd.concat([df_all, df_fv])

    return df_all.drop_duplicates().head(N_JOIN_KEYS)


# ----------------------------------------------------------------------------------------
#                                  Program Logic
# ----------------------------------------------------------------------------------------
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


def get_b64_requests(jk_df: pd.DataFrame, api_url: str, ws_name: str, fs_name: str) -> List[str]:
    join_keys: List[str] = list(jk_df.columns.values)
    jk_maps: List[Dict[str, str]] = [{jk: str(row[jk]) for jk in join_keys} for _idx, row in jk_df.iterrows()]
    return [web_req_with_b64_body(api_url, fs_name, ws_name, jk_map) for jk_map in jk_maps]


def send_requests(
    jk_maps: List[Dict[str, Any]],
    result_set: List[Dict[str, Any]],
    rps: float,
    api_url: str,
    ws_name: str,
    fs_name: str,
):
    """
    Target of a new thread. Appends to `result_set`
    """

    def lerp(a: float, b: float, ratio: float) -> float:
        return (ratio * (b - a)) + a

    time.sleep(random.random() * 5.0)

    seconds_per_req = 1.0 / rps
    last_start = None
    for i, jk_map in enumerate(jk_maps):
        headers = {"Authorization": f"Tecton-key {TECTON_API_KEY}"}
        params = req_params(fs_name, ws_name, jk_map)
        params["params"]["metadata_options"] = {
            "include_slo_info": True,
        }

        if last_start is not None:
            since_last = time.time() - last_start
            time_to_spare = seconds_per_req - since_last
            wait_s = max(0.05, time_to_spare)
            real_wait_s = lerp(wait_s * 0.8, wait_s * 1.2, random.random())
            time.sleep(real_wait_s)

        last_start = time.time()

        resp = None
        n_retries = 3
        for _ in range(n_retries):
            try:
                resp = re.post(api_url, json=params, headers=headers)
                break
            except Exception:
                # Give it a second
                time.sleep(1.0)
        if resp is None:
            print(f"Failed to get a response after {n_retries} retries. Moving on.", flush=True)
            continue
        md = resp.json()["metadata"]
        if "sloInfo" not in md:
            raise ValueError(
                "Response metadata does not contain sloInfo. Please check your params "
                f"for the `include_slo_info` param. Metadata:\n{md}\n"
            )
        result_set.append(md["sloInfo"])


def send_all_requests(jk_df: pd.DataFrame, api_url: str, ws_name: str, fs_name: str) -> List[Dict[str, Any]]:
    n_requests = len(jk_df)
    max_rps = 150.0
    n_threads = 50
    expected_s = round(float(n_requests) / max_rps, 2)

    thread_rps = max_rps / float(n_threads)

    join_keys: List[str] = list(jk_df.columns.values)

    # Create the chunks and result sets
    chunks: List[List[Dict[str, Any]]] = []
    result_sets: List[List[Dict[str, Any]]] = []
    for _ in range(n_threads):
        chunks.append([])
        result_sets.append([])

    # Populate the chunks
    for i, row in jk_df.iterrows():
        jk_map = {jk: str(row[jk]) for jk in join_keys}
        chunks[i % n_threads].append(jk_map)

    # Run the threads!
    threads: List[Thread] = [
        Thread(target=send_requests, args=(chunks[i], result_sets[i], thread_rps, api_url, ws_name, fs_name))
        for i in range(n_threads)
    ]
    print(
        f"Kicking off {n_threads} threads to do {n_requests} requests ({len(chunks[0])} each), at a "
        f"target RPS of {round(max_rps, 2)} ({round(thread_rps, 2)} per-thread). "
        f"Expected time taken: {expected_s} seconds...",
        flush=True,
    )
    for t in threads:
        t.start()
    print(f"Joining {n_threads} threads...")
    start = time.time()
    for t in threads:
        t.join()
    t = round(time.time() - start, 2)
    print(f"Done joining threads! Took {t} seconds ({round(t * 100.0 / expected_s, 2)}% of the {expected_s}s estimate)")

    # Merge the results together
    all_results: List[Dict[str, Any]] = []
    for result_set in result_sets:
        all_results.extend(result_set)
    return all_results


def parse_request_metadata(results: List[Dict[str, Any]]) -> Dict[str, Dict[str, Dict[str, float]]]:
    metric_to_vals = {}
    for md in results:
        for metric in MD_METRICS:
            if metric in md and type(md[metric]) in [int, float]:
                if metric not in metric_to_vals:
                    metric_to_vals[metric] = []
                metric_to_vals[metric].append(md[metric])

    metric_to_npvals = {metric: np.array(vals) for metric, vals in metric_to_vals.items()}
    metric_to_stats = {
        metric: {
            "n": npvals.size,
            "n_zero": npvals.size - np.count_nonzero(npvals),
            "mean": np.mean(npvals),
            "min": np.min(npvals),
            "max": np.max(npvals),
        }
        for metric, npvals in metric_to_npvals.items()
    }
    return metric_to_stats


def handle_feature_service(cluster: str, ws: Workspace, fs_name: str, api_url: str) -> None:
    fs: FeatureService = ws.get_feature_service(fs_name)
    print(f"Feature Service: {fs.name}")

    jk_df = get_join_keys(fs)

    resp_md = send_all_requests(jk_df, api_url, ws.workspace, fs.name)
    stats = parse_request_metadata(resp_md)
    print(f"Stats:\n{pformat(stats, indent=2)}\n")

    cluster_dir = JK_DIR / cluster
    cluster_dir.mkdir(parents=True, exist_ok=True)
    fs_file = cluster_dir / f"{fs.name}.{FILE_EXT}"
    if input(f"Do these stats look good? If so, overwriting {fs_file} [y/n]").upper() != "Y":
        return
    jk_df.to_csv(fs_file, index=False)
    print(f"Done with Feature Service {fs.name}!")


def main():
    workspace_raw = Util.shell(["tecton", "workspace", "show"], print_stdout=False)
    if ' ' in workspace_raw:
        workspace = workspace_raw.split(' ')[0]
    else:
        workspace = workspace_raw

    cluster = get_cluster()
    print(f"Connected to cluster '{cluster}'")

    ws_obj: Workspace = get_workspace(workspace)
    assert _cluster_url() is not None
    api_url = f"{_cluster_url()}/api/v1/feature-service/get-features"
    print("Connected to workspace:")
    print(ws_obj.summary())

    # feature_services: List[str] = ws_obj.list_feature_services()
    # for fs in feature_services:
    #     handle_feature_service(cluster, ws_obj, fs, api_url)


def cleanup(good: bool = True):
    try:
        pass
    except Exception:
        print(traceback.format_exc())
    finally:
        exit(0 if good else 1)


if __name__ == "__main__":
    good = True
    try:
        main()
    except Exception:
        print(traceback.format_exc())
        good = False
    finally:
        cleanup(good)
