# see exploremean model .ipynb
# need to run elasticToPandas.py before!
import numpy as np
import pandas as pd

df = pd.read_csv("data/output/autoscout-singlecars.csv.gz",compression="gzip")
#df = df[["Modell","Marke","Garantie","country","Zustand", "Zylinder","Kraftstoff","Erstzulassung", "Außenfarbe","Innenausstattung","Karosserieform","Anzahl Türen","Sitzplätze","Getriebeart","Gänge",'Hubraum', 'Kraftstoff', 'Schadstoffklasse', 'haendler', 'privat',"price"]]
#df = df[['url', 'country', 'date', 'Zustand', 'Garantie', 'Marke',
#       'Modell', 'Angebotsnummer', 'Außenfarbe', 'Lackierung',
#       'Farbe laut Hersteller', 'Innenausstattung', 'Karosserieform',
#       'Anzahl Türen', 'Sitzplätze', 'Schlüsselnummer', 'Getriebeart', 'Gänge',
#       'Hubraum', 'Kraftstoff', 'Schadstoffklasse', 'haendler', 'privat',
#       'ort', 'price', 'ausstattung_liste', 'Erstzulassung', 'Zylinder',
#       'Leergewicht']]

# for now drop description
#df = df.drop(["created","EZ erstzulassung","Beschreibung","Innenausstattung","URL","Ausstattung","Schluesselnummer",],axis=1)
df = df.fillna(0)
df = df.set_index(["Marke","Modell"])
meanDF = df.groupby(['Marke','Modell']).agg(lambda x:x.value_counts().index[0])
meanDF.to_csv("data/output/meanvalues.csv")
from elasticsearch import Elasticsearch
es = Elasticsearch(["elasticsearch:9200"])

mapping = {
  "mappings": {
      "properties": {
        "created" : {"type": "date"},
        "Antriebsart": {"type": "keyword" },
        "URL": {"type": "keyword" },
        "Preis": {"type": "integer" },
        "km": {"type": "integer" },
        "EZ erstzulassung": {"type": "date" },
        "kw": {"type": "short" },
        "ps": {"type": "short" },
        "Ausstattung": {"type": "keyword" },
        "Zustand": {"type": "keyword" },
        "Fahrzeughalter": {"type": "byte" },
        "Getriebeart": {"type": "keyword" },
        "Gaenge": {"type": "byte" },
        "Hubraum": {"type": "short" },
        "Zylinder": {"type": "byte" },
        "Leergewicht": {"type": "short" },
        "Kraftstoff": {"type": "keyword" },
        "Schadstoffklasse": {"type": "keyword" },
        "Feinstaubplakette": {"type": "keyword" },
        "Marke": {"type": "keyword" },
        "Modell": {"type": "keyword" },
        "Aussenfarbe": {"type": "keyword" },
        "Innenausstattung": {"type": "keyword" },
        "Karosserieform": {"type": "keyword" },
        "Anzahl Tueren": {"type": "byte" },
        "Sitzplaetze": {"type": "byte" },
        "Schluesselnummer": {"type": "keyword" },
        "Kraftstoffverbrauch (komb)": {"type": "half_float" },
        "Kraftstoffverbrauch (innerorts)": {"type": "half_float" },
        "Kraftstoffverbrauch (ausserorts)": {"type": "half_float" },
        "CO2-Emissionen": {"type": "short" },
        "Beschreibung": {"type": "text" },
          }}}
try:
    response = es.indices.create(
        index="autoscout-meanvalues",
        body=mapping
        )
except Exception as e:
    print("index mapping failed,maybe the index is already created?")
    print(str(e))

from tqdm import tqdm

for brand,model in tqdm(meanDF.index):
    data = meanDF.loc[(brand,model)]
    data["Modell"] = model
    data["Marke"] = brand
    idy = str(brand).replace(" ","-")+"_"+str(model).replace(" ","-")
    data = data.to_json()
    res = es.index(index="autoscout-meanvalues", id=idy, body=data)
