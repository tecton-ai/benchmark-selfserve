#!/usr/bin/env python3
import argparse
from functools import lru_cache
import os
from pathlib import Path
from typing import List, Tuple
import subprocess
import sys

from gen_requests import REQS_DIR

VEGET_OUT_FOLDERNAME = "vegeta_out"
VEGETA_OUT_DIR = Path(__file__).parent / VEGET_OUT_FOLDERNAME

class ReqUtil:
    @staticmethod
    @lru_cache(maxsize=None)
    def _tecton_api_key() -> str:
        var_name = "TECTON_API_KEY"
        if var_name not in os.environ:
            raise Exception(f"Environment variable '{var_name}' not set")
        return os.environ[var_name]

    @staticmethod
    def shell_capture(cmd_parts: List[str]) -> Tuple[int, str, str]:
        res = subprocess.run(cmd_parts, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")
        if res.returncode != 0:
            print(f"Return code {res.returncode}\nStdout:\n{res.stdout}\n\nStderr:\n{res.stderr}\n")
        return (res.returncode, res.stdout, res.stderr)

    @staticmethod
    def shell(cmd_parts: List[str]) -> int:
        res = subprocess.run(cmd_parts, encoding="utf-8")
        if res.returncode != 0:
            print(f"Return code {res.returncode}")
        return res.returncode

    @staticmethod
    def shell_pipe(cmd_parts_1: List[str], cmd_parts_2: List[str]) -> None:
        ps1 = subprocess.Popen(cmd_parts_1, stdout=subprocess.PIPE)
        ps2 = subprocess.Popen(cmd_parts_2, stdin=ps1.stdout, stdout=sys.stdout)
        ps1.wait()
        ps2.wait()

def main():
    # Check the requests directory
    REQS_DIR.mkdir(parents=True, exist_ok=True)
    req_filenames = [req_file.name for req_file in REQS_DIR.iterdir()]
    if len(req_filenames) == 0:
        raise Exception(f"No request files in {REQS_DIR}. Run `gen_requests.py` first.")

    parser = argparse.ArgumentParser()
    parser.add_argument("-r", "--rps", type=int, help="Requests per second", default=5)
    parser.add_argument("-d", "--duration", type=int, help="Duration (in seconds)", default=10)
    parser.add_argument("-t", "--timeout", type=int, help="Timeout (in miliseconds)", default=5000)
    parser.add_argument("-s", "--service", type=str, help="Feature Service", default=req_filenames[0], choices=req_filenames)
    parser.add_argument("-f", "--file", help=f"If set, output to a file in {VEGET_OUT_FOLDERNAME}", action="store_true", default=False)
    args = parser.parse_args()

    # Check for API key
    api_key = ReqUtil._tecton_api_key()

    # Check for vegeta
    returncode, _, stderr = ReqUtil.shell_capture(["vegeta", "--version"])
    if returncode != 0 or len(stderr) > 0:
        raise Exception("Make sure Vegeta is installed. See https://github.com/tsenart/vegeta")

    req_file = REQS_DIR / args.service

    cmd_attack = [
        "vegeta",
        "attack",
        "--format=json",
        f"--targets={req_file}",
        f"--timeout={args.timeout}ms",
        f"--rate={args.rps}/1s",
        "--max-workers=20000",
        f"--duration={args.duration}s",
        f"--header=Authorization: Tecton-key {api_key}",
    ]
    cmd_report = [
        "vegeta",
        "report",
        "--every=1s",
    ]
    if args.file:
        VEGETA_OUT_DIR.mkdir(parents=True, exist_ok=True)
        out_file = VEGETA_OUT_DIR / str(args.service)
        cmd_report.append(f"--output={out_file}")

    ReqUtil.shell_pipe(cmd_attack, cmd_report)


if __name__ == '__main__':
    main()
