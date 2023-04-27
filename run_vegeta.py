#!/usr/bin/env python3
import argparse
from functools import lru_cache
import os
from typing import List, Tuple
import subprocess
import sys

from gen_join_keys import JK_DIR

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
        print(f"Running `{' '.join(cmd_parts)}`...")
        res = subprocess.run(cmd_parts, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")
        if res.returncode != 0:
            print(f"Return code {res.returncode}\nStdout:\n{res.stdout}\n\nStderr:\n{res.stderr}\n")
        return (res.returncode, res.stdout, res.stderr)

    @staticmethod
    def shell(cmd_parts: List[str]) -> int:
        print(f"Running `{' '.join(cmd_parts)}`...")
        res = subprocess.run(cmd_parts, encoding="utf-8")
        if res.returncode != 0:
            print(f"Return code {res.returncode}")
        return res.returncode

    @staticmethod
    def shell_pipe(cmd_parts_1: List[str], cmd_parts_2: List[str]) -> None:
        print(f"Running `{' '.join(cmd_parts_1)} | {' '.join(cmd_parts_2)}`...")
        ps1 = subprocess.Popen(cmd_parts_1, stdout=subprocess.PIPE)
        ps2 = subprocess.Popen(cmd_parts_2, stdin=ps1.stdout, stdout=sys.stdout)
        ps1.wait()
        ps2.wait()

def main():
    JK_DIR.mkdir(parents=True, exist_ok=True)
    jk_filenames = [jk_file.name for jk_file in JK_DIR.iterdir()]
    if len(jk_filenames) == 0:
        raise Exception(f"No join-key files in {JK_DIR}. Run `gen_join_keys.py` first.")

    parser = argparse.ArgumentParser()
    parser.add_argument("-r", "--rps", type=int, help="Requests per second", default=100)
    parser.add_argument("-d", "--duration", type=int, help="Duration (in seconds)", default=60)
    parser.add_argument("-t", "--timeout", type=int, help="Timeout (in miliseconds)", default=5000)
    parser.add_argument("-f", "--feature-service", type=str, help="Feature Service", default=jk_filenames[0])
    args = parser.parse_args()

    # Check for API key
    api_key = ReqUtil._tecton_api_key()

    # Check for vegeta
    returncode, _, stderr = ReqUtil.shell_capture(["vegeta", "--version"])
    if returncode != 0 or len(stderr) > 0:
        raise Exception("Make sure Vegeta is installed. See https://github.com/tsenart/vegeta")

    # Just do the first one for now
    jk_filename = jk_filenames[0]
    jk_file = JK_DIR / jk_filename

    cmd_attack = [
        "vegeta",
        "attack",
        "--format=json",
        f"--targets={jk_file}",
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
    ReqUtil.shell_pipe(cmd_attack, cmd_report)



if __name__ == '__main__':
    main()
