import os
import logging
import requests
import flask
import datetime


from flask import Flask, request, flash, redirect, render_template, url_for, jsonify
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
app.config.update(dict(
    SQLALCHEMY_DATABASE_URI='sqlite:///kraken.db',
    DEBUG=True,
    SECRET_KEY='development',
    LOG_LEVEL=logging.DEBUG,
    ENVIRONMENT='development'
))
app.config.from_envvar('FLASK_SETTINGS', silent=True)

logging.basicConfig(level=logging.DEBUG, filename='logs/' + app.config['ENVIRONMENT'] + '.log')

db = SQLAlchemy(app)

# TODO: reused model from sesh-dash! DRY!!! - at least move database stuff it into a separate file
class Sesh_Site(db.Model):
    __tablename__= 'sites'
    id = db.Column(db.Integer, primary_key=True)
    site_name = db.Column(db.String(100))

    def __str__(self):
        return self.site_name

class BoM_Data_Point(db.Model):
    __tablename__= 'data_points'
    id = db.Column(db.Integer(), primary_key=True)
    site_id = db.Column(db.Integer(), db.ForeignKey('sites.id'))
    site = db.relationship('Sesh_Site')
    time = db.Column(db.DateTime())
    soc = db.Column(db.Float())
    battery_voltage = db.Column(db.Float())
    AC_input = db.Column(db.Float())
    AC_output = db.Column(db.Float())
    AC_Load_in = db.Column(db.Float())
    AC_Load_out = db.Column(db.Float())
    pv_production = db.Column(db.Float(), default=0)
    inverter_state = db.Column(db.String(100))
    genset_state = db.Column(db.String(100))
    relay_state = db.Column(db.String(100))
    trans = db.Column(db.Integer(), default=0)

    def __str__(self):
        return " %s : %s : %s" %(self.time,self.site,self.soc)


db.create_all()


@app.route("/ping")
def ping():
    logging.debug("pong")
    return "pong"

@app.route("/input/post.json", methods=['POST', 'GET'])
def post():
    return jsonify({'hello': 'world'})

@app.route('/input/bulk', methods=['POST', 'GET'])
def bulk():
    return "bulk"


if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5000))
    app.run(port=port)

