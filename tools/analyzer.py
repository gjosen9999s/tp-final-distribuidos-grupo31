import pandas as pd

FILENAME = "input_1.parquet"
CONTROL_FILE_Q1 = FILENAME+".ctrl.q1.txt"
SUMMARY_FILE = FILENAME+".summ.txt"

df = pd.read_parquet("data/input_1.parquet")

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

def in_language_parse(value):
    if not isinstance(value, dict):
        return None
    return value.get("identifier")

df["in_language"] = df["in_language"].apply(in_language_parse)

print(df["date_modified"].dtype)
print(df["in_language"].value_counts(dropna=False))

q1 = df[
    (df["in_language"] == "en")
    & (df["date_modified"].notna()) # not null
    & (df["date_modified"].dt.year <= 2020)
    & (df["url"].notna()) #not null
]

print("Q1:", len(q1))
print(q1[["date_modified", "url"]].head())

with open("data/"+SUMMARY_FILE , "w", encoding="utf-8") as file:
    file.write(f"Q1: {len(q1)}\n")

with open("data/"+CONTROL_FILE_Q1, "w", encoding="utf-8") as file:
    for i in range (len(q1)):
        file.write(q1.iloc[i]["url"]+","+str(q1.iloc[i]["date_modified"])+"\n")
   







