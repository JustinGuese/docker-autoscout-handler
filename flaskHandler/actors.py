from flask import Flask, render_template, redirect, url_for
from flask_bootstrap import Bootstrap
from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField
from wtforms.validators import DataRequired
from data import ACTORS
from modules import get_names, get_actor, get_id
import elasticsearch
import pandas as pd


client = elasticsearch.Elasticsearch(["localhost:9200"])

app = Flask(__name__)

# Flask-WTF requires an enryption key - the string can be anything
app.config['SECRET_KEY'] = 'C2HWGVoMGfNTBsrYQg8EcMrdTimkZfAb'

# Flask-Bootstrap requires this line
Bootstrap(app)

# with Flask-WTF, each web form is represented by a class
# "NameForm" can change; "(FlaskForm)" cannot
# see the route for "/" and "index.html" to see how this is used
class NameForm(FlaskForm):
    brand = StringField('Brand', validators=[DataRequired()])
    model = StringField('Model', validators=[DataRequired()])
    ez = StringField('Erstzulassung', validators=[DataRequired()])
    ks = StringField('Kraftstoff', validators=[DataRequired()])
    submit = SubmitField('Suchen')


# all Flask routes below

def searchElastic(marke,modell,ez,kraftstoff):

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
    return client.search(index="autoscout-singlecar", body=query_body)["hits"]["hits"]

def filterResults(df): 
    baseurl = "https://www.autoscout24.de"
    print(df.columns)
    df = df[["similarity_score","price","Marke","Modell","Erstzulassung","Karosserieform","country","Zustand","url",'date',
       'Angebotsnummer', 'Außenfarbe', 'Lackierung', 'Farbe laut Hersteller',
       'Innenausstattung', 'Anzahl Türen', 'Sitzplätze',
       'Schlüsselnummer', 'Getriebeart', 'Gänge', 'Hubraum', 'Kraftstoff',
       'Schadstoffklasse', 'haendler', 'privat', 'ort',
       'ausstattung_liste', 'Zylinder', 'Leergewicht']]
    # rename some columns
    df.columns = ["similarity_score","Preis","Marke","Modell","Erstzulassung","Karosserieform","Land","Garantie?","url", 'Datum gesehen',
       'Angebotsnummer', 'Außenfarbe', 'Lackierung', 'Farbe laut Hersteller',
       'Innenausstattung', 'Anzahl Türen', 'Sitzplätze',
       'Schlüsselnummer', 'Getriebeart', 'Gänge', 'Hubraum', 'Kraftstoff',
       'Schadstoffklasse', 'haendler', 'privat', 'ort',
       'ausstattung_liste', 'Zylinder', 'Leergewicht']
    link = baseurl + str(df["url"].values[0])
    df["url"] = link
    # drop entries with zero
    df = df[~(df==0)]
    df = df[~(df=="0")]
    df = df.dropna(axis=1)

    # replace some values
    df[df["Garantie?"]=="Tipp:Prüfe das Fahrzeug bei der Besichtigung auf Mängel. Generell gehen wir bei allen Angeboten von einem guten Fahrzeug-Zustand aus."] == "Nein"
    return df

@app.route('/', methods=['GET', 'POST'])
def index():
    names = get_names(ACTORS)
    # you must tell the variable 'form' what you named the class, above
    # 'form' is the variable name used in this template: index.html
    form = NameForm()
    message = ""
    if form.validate_on_submit():
        brand = form.brand.data.lower()
        model = form.model.data.lower()
        ez = form.ez.data.lower()
        ks = form.ks.data.lower()

        # search query
        res = searchElastic(brand,model,ez,ks)
        # try to get response
        if len(res) > 0:
            df = pd.Series(data=res[0]["_source"]).to_frame().T
            df["similarity_score"] = res[0]["_score"]
            df = filterResults(df)
            df = df.T
            message = df.to_html(header=False,justify="center",render_links=True)
        else:
            message = "That car is not in our database."
    return render_template('index.html', names=names, form=form, message=message)

@app.route('/actor/<id>')
def actor(id):
    # run function to get actor data based on the id in the path
    id, name, photo = get_actor(ACTORS, id)
    if name == "Unknown":
        # redirect the browser to the error template
        return render_template('404.html'), 404
    else:
        # pass all the data for the selected actor to the template
        return render_template('actor.html', id=id, name=name, photo=photo)

# 2 routes to handle errors - they have templates too

@app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html'), 404

@app.errorhandler(500)
def internal_server_error(e):
    return render_template('500.html'), 500


# keep this as is
if __name__ == '__main__':
    app.run(debug=True)
