from flask import Flask, render_template, redirect, url_for
from flask_bootstrap import Bootstrap
from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField
from wtforms.validators import DataRequired
import elasticsearch
import pandas as pd


client = elasticsearch.Elasticsearch(["elasticsearch:9200"])

app = Flask(__name__)

# Flask-WTF requires an enryption key - the string can be anything
app.config['SECRET_KEY'] = 'C2HWGVoMGfNTBsrYQg8EcMrdTimkZfAb'

class NameForm(FlaskForm):
    name = StringField('Was für ein Auto suchen Sie?', validators=[DataRequired()])
    submit = SubmitField('Suchen')

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

def base(size=50,term={}):
    match_all = {
    "size": 50,
    "sort": { "created": "desc"},
    "query": {
        "match_all": term
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
    return df

def specificSearch(searchterm):  
    try:
        query = {
        "from":10,
        "size":10,
        "query": {
            "query_string": {
            "query": searchterm
            }
        }
        }
        res = client.search(
        index = "autoscout-candidates",
        body = query,
        )["hits"]["hits"]
        if len(res) > 0:
            tmp = []
            for entry in res:
                tmp.append(entry["_source"])
            df = pd.DataFrame(data=tmp)
            df = prepareDF(df)
            message = df.to_html(header=True,justify="center",render_links=True)
    except Exception as e:
        message = "That car is not in our database." +  str(e)
    return message     

def elasticSearch():
    try:
        df = base()
        message = df.to_html(header=True,justify="center",render_links=True)
    except Exception:
        message = "That car is not in our database."
    return message

def elasticSearchFree():
    try:
        df = base(size=20)
        # free trial additions
        df["URL"][:10] = "Nur für Mitglieder sichtbar"
        # end free trial additions
        message = df.to_html(header=True,justify="center",render_links=True)
    except Exception:
        message = elasticSearch()
    return message




@app.route('/', methods=['GET', 'POST'])
def index():
    message = elasticSearch()
    return render_template('index.html', message=message)

@app.route('/free', methods=['GET', 'POST'])
def free():
    message = elasticSearchFree()
    return render_template('index.html', message=message)


@app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html'), 404

@app.errorhandler(500)
def internal_server_error(e):
    return render_template('500.html'), 500

@app.route('/spezialsuche', methods=['GET', 'POST'])
def spezialsuche():
    form = NameForm()
    message = ""
    if form.validate_on_submit():
        name = form.name.data
        if len(name) > 0:
            # empty the form field
            message = specificSearch(name)
        else:
            message = "That car is not in our database."
    return render_template('suche.html', form=form, message=message)

# keep this as is
if __name__ == '__main__':
    app.run(host='0.0.0.0',debug=True)
