import elasticsearch

es = elasticsearch.Elasticsearch()
print(es.ping())