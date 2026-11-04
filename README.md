# tp-final-distribuidos-grupo31
TP Final Distribuidos - Grupo 31

## Cómo correr

Todo desde **WSL o Git Bash** (el Makefile usa `/bin/bash`; en CMD no anda). Docker Desktop tiene que estar corriendo.

```bash
# Levantar el sistema
make up

# Ctrl+C corta los logs; para bajar todo:
make down

# Borrar todas las salidas (no toca los controles)
make clean
```

Los datos están organizados por cliente:

```text
data/client_1/input/
data/client_1/output/
data/client_1/output/img/
data/client_1/control/

data/client_2/input/
data/client_2/output/
data/client_2/output/img/
data/client_2/control/

data/client_3/input/
data/client_3/output/
data/client_3/output/img/
data/client_3/control/
```

`make clean` borra `q*.txt`, `summary.txt` e imágenes de `output/`, pero deja intactos `input/` y `control/`.

Si en WSL el build falla con `error getting credentials`, usá un config de Docker sin credential helper:

```bash
export DOCKER_CONFIG=/tmp/dockercfg
```
