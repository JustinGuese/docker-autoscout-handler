from elasticsearch import Elasticsearch, exceptions
# declare globals for the Elasticsearch client host
DOMAIN = "elasticsearch"
PORT = 9200
host = str(DOMAIN) + ":" + str(PORT)
index = "autoscout-singlecar"
client = Elasticsearch(host)
data = []

# declare a filter query dict object
match_all = {
   "size": 50,
   "sort": { "created": "desc"},
   "query": {
      "match_all": {}
   }
}
# make a search() request to get all docs in the index
resp = client.search(
    index = index,
    body = match_all,
    scroll = '2s' # length of time to keep search context
)

# keep track of pass scroll _id
old_scroll_id = resp['_scroll_id']
while len(resp['hits']['hits']):
    resp = client.scroll(
        scroll_id = old_scroll_id,
        scroll = '2s' # length of time to keep search context
    )
    # keep track of pass scroll _id
    old_scroll_id = resp['_scroll_id']
    for doc in resp['hits']['hits']:
        data.append(doc["_source"])

# save
import pandas as pd
data = pd.DataFrame.from_dict(data)
data = data.set_index("URL")
data.to_csv("data/output/autoscout-singlecars.csv.gz",compression="gzip")