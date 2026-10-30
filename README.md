# tp-final-distribuidos-grupo31
TP Final Distribuidos - Grupo 31

## Cómo correr

Todo desde **WSL o Git Bash** (el Makefile usa `/bin/bash`; en CMD no anda). Docker Desktop tiene que estar corriendo.

```bash
# 1) generar los parquet de prueba (data/ no se versiona)
make split-data

# 2) levantar el sistema: ArticleHandler real + Q1 ×3 + 1 cliente
make up
# Ctrl+C corta los logs; para bajar todo:
make down
```

`make up` procesa `data/input_1.parquet`; el cliente termina con **1.112** resultados.

Pruebas:

```bash
make e2e        # AH real + Q1 ×1 + 1 cliente          → 1.112
make e2e-3-1    # AH real + Q1 ×1 + 3 clientes         → 1.111
make e2e-3-2    # AH real + Q1 ×2 + 3 clientes         → 1.111
make e2e-3-3    # AH real + Q1 ×3 + 3 clientes         → 1.111
make q1-test    # Q1 con simulador (mock), sin AH real
```

Los e2e de 3 clientes usan `data/e2e_1.parquet`, `data/e2e_2.parquet` y `data/e2e_3.parquet` (los genera `make split-data`).

Si en WSL el build falla con `error getting credentials`, usá un config de Docker sin credential helper:

```bash
export DOCKER_CONFIG=/tmp/dockercfg
```
