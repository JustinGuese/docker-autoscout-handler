import elasticsearch
elastic_client = elasticsearch.Elasticsearch()
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
response = elastic_client.indices.create(
    index="autoscout-singlecar",
    body=mapping
    )

# print out the response:
print ('response:', response)
