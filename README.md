COnda env
autoscoutd

## extract data from elastic

easiest way:

<!-- docker run --rm -ti -v $PWD/data/input:/tmp elasticdump/elasticsearch-dump \
  --input=http://192.168.0.242:9200/autoscout-singlecar \
  --output=/tmp/autoscout-singlecar.json \
  --type=data -->

  wrong, just run elasticToPandas.py

  output in autoscout-singlecar.py


# indices

autoscout-singlecar


# TODO

script running through all candidates and calculating the expected price