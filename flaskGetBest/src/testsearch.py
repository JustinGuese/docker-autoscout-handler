from elasticsearch import Elasticsearch
from elasticsearch_dsl import Q,Search
import pprint
import pandas as pd


client = Elasticsearch(["localhost:9200"])

marke = "Volkswagen"
modell = "Polo"
ez = "2012"
kraftstoff = "Benzin"

query_body = {
"query": {
"bool": {
  "should": [
      # one block
    {
      "match": {
        "Marke": {
          "query": marke,
          "operator": "and"
        }
      }
    },
      # end one block
      # one block
    {
      "match": {
        "Erstzulassung": {
          "query": ez,
          "operator": "and"
        }
      }
    },
      # end one block
      # one block
    {
      "match": {
        "Kraftstoff": {
          "query": kraftstoff,
          "operator": "and"
        }
      }
    },
      # end one block
    {
      "match": {
        "Modell": {
          "query": modell,
          "operator": "and"
          }
        }
      }
     ]
    }
  }
}

resp = client.search(index="autoscout-singlecar", body=query_body)
for hit in resp["hits"]["hits"][:1]:
    pprint.pprint(hit["_source"])
    d = hit["_source"]
    print(type(d))
    df = pd.Series(data=d)
    print(df.to_frame().to_html())