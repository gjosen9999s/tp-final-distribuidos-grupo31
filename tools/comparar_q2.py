import csv
import hashlib
import os
import sys

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

if len(sys.argv) != 3:
    print("Uso: python comparar_q2.py <q2_control.txt> <carpeta_img>")
    raise SystemExit(1)

control_path = sys.argv[1]
img_dir = sys.argv[2]


def sha256(path):
    with open(path, "rb") as archivo:
        return hashlib.sha256(archivo.read()).hexdigest()


control = {}
sin_hash = set()

with open(control_path, newline="", encoding="utf-8") as archivo:
    for fila in csv.reader(archivo):
        if len(fila) < 4:
            continue

        image_name = fila[2].strip()
        image_hash = fila[3].strip()

        if not image_name:
            continue

        if not image_hash:
            sin_hash.add(image_name)
            continue

        control[image_name] = image_hash

disco = {}
for nombre in os.listdir(img_dir):
    ruta = os.path.join(img_dir, nombre)
    if os.path.isfile(ruta):
        disco[nombre] = sha256(ruta)

ok = 0
faltan_en_disco = []
hash_distinto = []
faltan_en_control = []
sin_hash_presentes = []
sin_hash_faltan = []

for image_name, image_hash in control.items():
    if image_name not in disco:
        faltan_en_disco.append(image_name)
    elif disco[image_name] != image_hash:
        hash_distinto.append(image_name)
    else:
        ok += 1

for image_name in sin_hash:
    if image_name in disco:
        sin_hash_presentes.append(image_name)
    else:
        sin_hash_faltan.append(image_name)

for image_name in disco:
    if image_name not in control and image_name not in sin_hash:
        faltan_en_control.append(image_name)

print("control con hash:", len(control))
print("sin hash en control:", len(sin_hash))
print("imagenes en disco:", len(disco))
print("coinciden:", ok)
print("sin hash presentes:", len(sin_hash_presentes))
print("sin hash faltan en disco:", len(sin_hash_faltan))
print("faltan en disco:", len(faltan_en_disco))
print("hash distinto:", len(hash_distinto))
print("sobran en disco:", len(faltan_en_control))

for image_name in faltan_en_disco:
    print("FALTA EN DISCO:", image_name)

for image_name in hash_distinto:
    print("HASH DISTINTO:", image_name)

for image_name in faltan_en_control:
    print("SOBRA EN DISCO:", image_name)

for image_name in sin_hash_faltan:
    print("SIN HASH FALTA EN DISCO:", image_name)
