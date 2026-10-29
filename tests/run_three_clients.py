"""Corre 3 clientes en paralelo sobre el set dividido en 3 y verifica Q1."""

import re
import subprocess
import sys
from pathlib import Path

import pyarrow.parquet as pq

SOURCE = "/app/src/client/test.parquet"
TMP = Path("/tmp/q1_clients")
CLIENTS = 3
EXPECTED = 1111


def split_dataset():
    TMP.mkdir(parents=True, exist_ok=True)
    table = pq.ParquetFile(SOURCE).read()
    total = table.num_rows
    paths = []
    for index in range(CLIENTS):
        start = index * total // CLIENTS
        end = (index + 1) * total // CLIENTS
        path = TMP / f"client_{index + 1}.parquet"
        pq.write_table(table.slice(start, end - start), path)
        paths.append(path)
    return paths


def start_client(path):
    cmd = [
        sys.executable,
        "-m",
        "client.main",
        "--input",
        str(path),
        "--host",
        "mock_ah",
        "--port",
        "8080",
        "--timeout",
        "240",
    ]
    return subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def main():
    paths = split_dataset()
    processes = [(index + 1, start_client(path)) for index, path in enumerate(paths)]

    totals = []
    failures = 0
    for index, proc in processes:
        stdout, stderr = proc.communicate()
        print(f"--- cliente {index}: returncode={proc.returncode} ---")
        print(stdout[-1500:])
        if proc.returncode != 0:
            print(stderr[-1500:], file=sys.stderr)
            failures += 1
            continue

        matches = re.findall(
            r"Total de resultados recolectados con éxito:\s*(\d+)", stdout
        )
        if not matches:
            print(f"cliente {index}: no encontré el total", file=sys.stderr)
            failures += 1
            continue
        totals.append(int(matches[-1]))

    if failures:
        raise SystemExit(f"{failures} clientes fallaron")

    results = sum(totals) - CLIENTS
    print("resultados por cliente (incluye el final):", totals)
    print("Q1 total:", results)
    if results != EXPECTED:
        raise SystemExit(f"esperado {EXPECTED}, obtenido {results}")
    print("3 clientes en paralelo OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
