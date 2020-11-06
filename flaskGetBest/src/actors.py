from flask import Flask, render_template, redirect, url_for
from flask_bootstrap import Bootstrap
import elasticsearch
import pandas as pd


client = elasticsearch.Elasticsearch(["elasticsearch:9200"])

app = Flask(__name__)

# Flask-WTF requires an enryption key - the string can be anything
app.config['SECRET_KEY'] = 'C2HWGVoMGfNTBsrYQg8EcMrdTimkZfAb'

# Flask-Bootstrap requires this line
Bootstrap(app)

def prepareDF(df):
    df= df[["Marke","Modell","km","ps","Preis","twinPrice","meanPrice","URL"]]
    df["Vorhergesagter Profit"] = (df["twinPrice"]*2+df["meanPrice"])/3 - df["Preis"]
    df["Profit laut Twin"] = df["twinPrice"] - df["Preis"]
    # safety net, if profit too good delete it
    df["Vorhergesagter Profit"][(df["Profit laut Twin"]/df["Preis"]) > 2] = 0 # set profit 0 if profit is X times the price

    # sort by profit
    df = df.sort_values(by="Vorhergesagter Profit",ascending=False,ignore_index=True)
    return df

def elasticSearch():
    match_all = {
    "size": 50,
    "sort": { "created": "desc"},
    "query": {
        "match_all": {}
    }
    }
    # make a search() request to get all docs in the index
    res = client.search(
        index = "autoscout-candidates",
        body = match_all,
    )["hits"]["hits"]
    if len(res) > 0:
        tmp = []
        for entry in res:
            tmp.append(entry["_source"])
        df = pd.DataFrame(data=tmp)
        df = prepareDF(df)
        message = df.to_html(header=True,justify="center",render_links=True)
    else:
        message = "That car is not in our database."
    return message



@app.route('/', methods=['GET', 'POST'])
def index():
    message = elasticSearch()
    return render_template('index.html', message=message)




@app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html'), 404

@app.errorhandler(500)
def internal_server_error(e):
    return render_template('500.html'), 500


# keep this as is
if __name__ == '__main__':
    app.run(host='0.0.0.0',debug=True)
