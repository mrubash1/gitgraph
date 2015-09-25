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

from elasticsearch import Elasticsearch
import json

es = Elasticsearch()
#result = es.search(index="movie_db", body={'query': {'match': {'description': 'CIA'}}})
result = es.search(index="test-31")
print json.dumps(result, indent=2)

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
