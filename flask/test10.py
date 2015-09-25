#!/usr/bin/env python

# importing flask and cassandra modules
from cassandra.cluster import Cluster
from flask import Flask, jsonify, render_template, request
import json
from cassandra.query import SimpleStatement
import operator
from sets import Set
from flask_restful import Resource, Api

# importing script to fetch data from API in real time
#import fetch_data


import json


#result = es.search(index="movie_db", body={'query': {'match': {'description': 'CIA'}}})
from elasticsearch import Elasticsearch
es = Elasticsearch(['172.31.2.97'])
#result = es.search(index="test-31")
#print json.dumps(result, indent=2)

# setting up connections to cassandra
from cqlengine import connection
connection.setup(['52.88.228.98','52.11.49.170'], 'test')
#cluster = Cluster(['52.88.228.98','52.11.49.170'])
#session = cluster.connect('watch_events')

app = Flask(__name__)
api = Api(app)

# homepage
@app.route("/")
@app.route("/index")
def hello():
  return render_template("index.html")

# graph query page
@app.route("/graph")
def graph():
  return render_template("graph_query.html")

# get username and return graph
@app.route("/graph", methods=['POST'])
def graph_post():
  username = request.form["search term"] # get username entered
  density = request.form["density"]
  if density=="Max (1000 nodes)":
    nodes = 1000
  elif density=="High (500 nodes)":
    nodes = 500
  elif density=="Medium (200 nodes)":
    nodes = 200
  elif density=="Low (100 nodes)":
    nodes = 100
  elif density=="Very Low (50 nodes)":
    nodes = 50

if __name__ == "__main__":
  app.run(host='0.0.0.0')
