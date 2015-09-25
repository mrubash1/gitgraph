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
import fetch_data

# setting up connections to cassandra
cluster = Cluster(['52.88.228.98','52.11.49.170'])
session = cluster.connect('watch_events')

app = Flask(__name__)
api = Api(app)

# homepage
@app.route("/")
@app.route("/index")
def hello():
  return render_template("index.html")
