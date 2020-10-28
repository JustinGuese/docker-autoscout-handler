import pandas as pd
import elasticsearch
import time

df = pd.read_csv("data/input/largeDF.csv.gz",compression="gzip")
df = df[['url', 'country', 'date', 'Zustand', 'Garantie', 'Marke',
       'Modell', 'Angebotsnummer', 'Außenfarbe', 'Lackierung',
       'Farbe laut Hersteller', 'Innenausstattung', 'Karosserieform',
       'Anzahl Türen', 'Sitzplätze', 'Schlüsselnummer', 'Getriebeart', 'Gänge',
       'Hubraum', 'Kraftstoff', 'Schadstoffklasse', 'haendler', 'privat',
       'ort', 'price', 'ausstattung_liste', 'Erstzulassung', 'Zylinder',
       'Leergewicht']]


# clean
df = df.fillna(0)

es = elasticsearch.Elasticsearch(["elasticsearch:9200"]) # rem if not using docker

max_wait = 20
wait = 0
while not es.ping() and wait < max_wait:
    print("no connection yet...")
    time.sleep(5)
if wait == max_wait:
    raise Exception("No Connection to Elasticsearch...abort.")

dindawork = []
for i,entry in df.iterrows():
    try:
        entry = entry.to_dict()
        id = entry["url"]
        es.index(index="autoscout-singlecar", id=id, body=entry)
    except elasticsearch.exceptions.RequestError as e:
        dindawork.append([i])

print("DINDT WORK: ",dindawork)
print(len(dindawork)," of ",len(df)," items failed.")

# adding mean values
dindaworkmean = []
print(df.shape, " shape")
meanDF = df.groupby(["Marke","Modell"]).agg(lambda x:x.value_counts().index[0])
meanDF = meanDF.fillna(0)
for brand,model in meanDF.index:
    p1 = meanDF.loc[brand,model].to_dict()
    p1.update({"model":model})
    p1.update({"brand":brand})
    comb = str(brand)+"_"+str(model)
    p1.update({"brand_model":comb})
    # try elastic
    try:
        es.index(index="autoscout-mean", id=comb, body=p1)
    except elasticsearch.exceptions.RequestError as e:
        dindaworkmean.append([brand,model,str(e)])

meanDF.to_csv("data/output/autoscout-mean.csv")

print("DINDT WORK: ",dindaworkmean)
print(len(dindaworkmean)," of ",len(meanDF)," items failed.")