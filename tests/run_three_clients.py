"""Corre 3 clientes en paralelo sobre los parquet cortados de /data y verifica Q1."""

import re
import subprocess
import sys

CLIENTS = 3
EXPECTED = 1111
PATHS = [f"/data/e2e_{index}.parquet" for index in range(1, CLIENTS + 1)]


def start_client(path):
    cmd = [
        sys.executable,
        "-m",
        "client.main",
        "--input",
        path,
        "--host",
        "article_handler",
        "--port",
        "5678",
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
    processes = [(index + 1, start_client(path)) for index, path in enumerate(PATHS)]

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
