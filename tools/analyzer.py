import argparse
import collections
import json
from nltk.corpus import stopwords
import pandas as pd
from pathlib import Path
import re


parser = argparse.ArgumentParser()
parser.add_argument("input")

args = parser.parse_args()

FILENAME = args.input

#FILENAME = "input_1.parquet"
SUMMARY_FILE = FILENAME+".summ.txt"
CONTROL_FILE_Q1 = FILENAME+".ctrl.q1.txt"
CONTROL_FILE_Q2 = FILENAME+".ctrl.q2.txt"
CONTROL_FILE_Q3 = FILENAME+".ctrl.q3.txt"
CONTROL_FILE_Q4 = FILENAME+".ctrl.q4.txt"
CONTROL_FILE_Q5 = FILENAME+".ctrl.q5.txt"

df = pd.read_parquet("data/"+FILENAME)

print(df.columns.tolist())

#['abstract', 'additional_entities', 'date_created', 'date_modified', 
# 'description', 'event', 'identifier', 'image', 'in_language', 
# 'infoboxes', 'is_part_of', 'license', 'main_entity', 'name', 
# 'references', 'sections', 'tables', 'url', 'version']

COLUMNS_TO_DROP = ['additional_entities', 'date_created',
       'description', 'event', 'identifier',
       'infoboxes', 'is_part_of', 'license', 'main_entity',
       'references', 'tables', 'version']

df=df.drop(columns=COLUMNS_TO_DROP)

print(df.columns.tolist())
print(df.shape)
print(df[["in_language", "date_modified"]].head())

# Helper para modificar la columna in_language de {'identifier': 'en'} => en 
def in_language_parse(value):
    if not isinstance(value, dict):
        return None
    return value.get("identifier")

# Aplico el formateo a la columna
df["in_language"] = df["in_language"].apply(in_language_parse)

print(df["date_modified"].dtype)
print(df["in_language"].value_counts(dropna=False))

# Query Q1 sobre el data frame (language = en y date_modified <= 2020, ignorando los null)
q1 = df[
    (df["in_language"] == "en")
    & (df["date_modified"].notna()) # not null
    & (df["date_modified"].dt.year <= 2020)
    & (df["url"].notna()) #not null
]

print("Q1:", len(q1))
print(q1[["date_modified", "url"]].head())

# Escritura archivo sumario de Q1
with open("data/"+SUMMARY_FILE , "w", encoding="utf-8") as file:
    file.write(f"Q1: {len(q1)}\n")

# Escritura archivo de detalle de Q1 => "url",date_modified
with open("data/"+CONTROL_FILE_Q1, "w", encoding="utf-8") as file:
    for i in range (len(q1)):
        file.write('"'+q1.iloc[i]["url"]+'"'+","+str(q1.iloc[i]["date_modified"])+"\n")

# Helper para modificar la columna content_url {'content_url': 'url'} => url
def image_object_to_url(image):
    if not isinstance(image, dict):
        return None
    return image.get('content_url', None)

# Aplico el formateo a la columna
df["image"] = df["image"].apply(image_object_to_url)



# Helper para marcar los que tienen biografia / biographie
BIOGRAPHY_SECTION_NAMES = ['biographie', 'biography']
def has_biography_section(sections):
    if not isinstance(sections, str):
        return False
    section_names =  [section.get("name", "").lower() for section in json.loads(sections)]
    for biography_section_name in BIOGRAPHY_SECTION_NAMES:
        if biography_section_name in section_names:
            return True
    return False

# Agrego una columna al df original y que tiene solo el año
df['date_modified_year'] = df['date_modified'].dt.year

# Creo una copia con solo los que tienen año de modificacion <= 2020 
df_before_2020 = df[df['date_modified_year'] <= 2020].copy()

# Aplico la funcion de transformacion, creando la columna has_biography_section con True/False segun el caso
df_before_2020['has_biography_section'] = df_before_2020['sections'].apply(has_biography_section)

# Query Q2 sobre el data frame (date_modified <= 2020 y contiene la seccion biography/biographie)
q2 = df_before_2020[(df_before_2020['has_biography_section'] == True)]


# Escritura en archivo sumario de totales de Q2
with open("data/"+SUMMARY_FILE , "a", encoding="utf-8") as file:
    file.write(f"Q2: {len(q2)}\n")

# Escritura archivo de detalle de Q2 => name, date_modified, image(url)
with open("data/"+CONTROL_FILE_Q2, "w", encoding="utf-8") as file:
    for i in range (len(q2)):
        file.write(f'{q2.iloc[i]["name"]},{q2.iloc[i]["date_modified"]},'
                   f'{q2.iloc[i]["image"]}\n'
                   )


# Stop Words de ingles y frances (set)
stop_words = set(stopwords.words('english')) | set(stopwords.words('french'))

WORD_REGEX = r'\b\w+\b'
NUMBER_REGEX = r'[+-]?((\d+\.?\d*)|(\.\d+))'

# Helper para extraer las palabras que no son stopwords y que no son nros
def extract_non_stopwords_from_abstract(abstract):
    if not isinstance(abstract, str):
        return []
    words = re.findall(WORD_REGEX, abstract.lower())
    return [word for word in words if word not in stop_words and not re.fullmatch(NUMBER_REGEX, word)]

# Helper para crear la lista de palgbras unicas por lenguaje (que no sean stopwords ni nros)
def fill_language_word_set(df, language):
    language_words = set()
    for abstract in df[df["in_language"] == language]['abstract'].dropna():
        language_words |= set(extract_non_stopwords_from_abstract(abstract))
    return language_words

english_words = fill_language_word_set(df, "en")
french_words = fill_language_word_set(df, "fr")
shared_words = english_words & french_words

# Escritura en archivo sumario de totales de Q3
with open("data/"+SUMMARY_FILE , "a", encoding="utf-8") as file:
    file.write(f"Q3: {len(shared_words)}\n")

# Escritura archivo de detalle de Q3 => Palabras unicas compartidas entre todos los articulos en ingles/frances
with open("data/"+CONTROL_FILE_Q3, "w", encoding="utf-8") as file:
    for word in sorted(shared_words):
        file.write(word+'\n')

TOP_SIZE = 20
words_counter = collections.Counter()
for abstract in df['abstract'].dropna():
    non_stop_words = extract_non_stopwords_from_abstract(abstract)
    words_counter.update(set(non_stop_words)) # las palabras de un mismo abstract cuentan 1 vez deduplico con set

q4 = pd.DataFrame(words_counter.most_common(TOP_SIZE),
                  columns=["word", "count"]
                  )
# Escritura en archivo sumario de totales de Q4
with open("data/" + SUMMARY_FILE, "a", encoding="utf-8") as file:
    file.write(f"Q4: {len(q4)}\n")

# Escritura archivo de detalle de Q3 => TOP 20 de palabras, deduplicando palabras repetidas en un mismo abstract(articulo)
with open("data/" + CONTROL_FILE_Q4, "w", encoding="utf-8") as file:
    for word, count in q4.itertuples(index=False, name=None): #para que no devuelva nro de registro y titulo de campo(named tuple)
        file.write(f"{word},{count}\n")


# Creo una copia con solo los que tienen año de modificacion > 2020 
df_after_2020 = df[df['date_modified_year'] > 2020].copy()

# Helper para scar el promedio
def calc_abstract_word_count_average(df):
    count = 0
    total = 0
    for abstract in df['abstract'].dropna():
        total += len(set(extract_non_stopwords_from_abstract(abstract)))
        count += 1
    return total/count

df_avg_before_2020 = calc_abstract_word_count_average(df_before_2020)

df_after_2020 ["abstract_word_count"] = df_after_2020 ["abstract"].apply(lambda abstract: len(set(extract_non_stopwords_from_abstract(abstract))))
q5 = df_after_2020 [df_after_2020["abstract_word_count"] > df_avg_before_2020 ][["url","abstract_word_count"]]

# Escritura en archivo sumario de totales de Q5
with open("data/" + SUMMARY_FILE, "a", encoding="utf-8") as file:
    file.write(f"Q5: {len(q5)}\n")

with open("data/" + CONTROL_FILE_Q5, "w", encoding="utf-8") as file:
    for url, wordcount in q5.itertuples(index=False, name=None): #para que no devuelva nro de registro y titulo de campo(named tuple)
        file.write(f"{url},{wordcount}\n")















