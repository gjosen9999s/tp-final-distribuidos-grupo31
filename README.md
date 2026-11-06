# tp-final-distribuidos-grupo31
TP Final Distribuidos - Grupo 31

## Cómo correr

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

data/client_4/input/
data/client_4/output/
data/client_4/output/img/
data/client_4/control/
```

`make clean` borra `q*.txt`, `summary.txt` e imágenes de `output/`, pero deja intactos `input/` y `control/`.

```

## Analyzer desde Docker

El analyzer es una tool desarrollada en base al Notebook que recibimos como ejemplo. No es parte del sistema distribuido. Su función es generar archivos con formato simil csv para que luego podamos comparar los resultados esperados vs los obtenidos.

```bash
mkdir -p tools/analyzer/out

docker compose run --rm --no-deps \
  -v "$(pwd)/data:/data" \
  -v "$(pwd)/tools/analyzer.py:/app/tools/analyzer.py:ro" \
  -v "$(pwd)/tools/analyzer/out:/app/tools/analyzer/out" \
  article_handler python -u /app/tools/analyzer.py \
  /data/client_1/input/wiki_sample_1.parquet
```

Los controles quedan en:

```text
tools/analyzer/out/q1.txt
tools/analyzer/out/q2.txt
tools/analyzer/out/q3.txt
tools/analyzer/out/q4.txt
tools/analyzer/out/q5.txt
tools/analyzer/out/summary.txt
```

Para otro cliente, cambiar el path:

```text
/data/client_2/input/wiki_sample_2.parquet
/data/client_3/input/wiki_sample_3.parquet
/data/client_4/input/wiki_sample_4.parquet
```

El contenedor `article_handler` ya tiene pandas (solo para ANALYZER), pyarrow y NLTK.

## Diff controles vs salidas (Ej: client_1)

Q1, Q3, Q4 y Q5 se comparan ordenados porque el orden de salida no es estable:

```bash
diff -u <(sort data/client_1/control/q1.txt) <(sort data/client_1/output/q1.txt)
diff -u <(sort data/client_1/control/q3.txt) <(sort data/client_1/output/q3.txt)
diff -u <(sort data/client_1/control/q4.txt) <(sort data/client_1/output/q4.txt)
diff -u <(sort data/client_1/control/q5.txt) <(sort data/client_1/output/q5.txt)
```

Q2 no se compara con diff directo porque el control tiene `image_name` y `sha256`, y la salida tiene el path de la imagen. Para Q2 usar `tools/comparar_q2.py`.

## Comparador Q2

Compara el control Q2 contra las imágenes guardadas por el cliente:

```bash
python3 /mnt/c/DISTRIBUIDOS/tp-dev-actual/tools/comparar_q2.py \
  /mnt/c/DISTRIBUIDOS/tp-dev-actual/data/client_1/control/q2.txt \
  /mnt/c/DISTRIBUIDOS/tp-dev-actual/data/client_1/output/img
```

Usar paths absolutos evita problemas de WSL al ejecutar el comparador desde otra carpeta.

El primer argumento es el control Q2 generado por el analyzer.
El segundo es la carpeta de imágenes del cliente.

El control Q2 tiene este formato:

```text
"name","article_url","image_name",sha256
```

El comparador calcula el SHA-256 de los archivos del disco y compara por
`image_name`. Informa:

- cuántas coinciden
- cuáles faltan en disco
- cuáles tienen hash distinto
- cuáles sobran en disco
- cuáles no tienen hash en el control
