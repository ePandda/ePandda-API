import json
import requests
import collections
import datetime
import time
import logging
import urllib
from functools import wraps, update_wrapper
from flask import Flask, request, current_app, make_response, session, escape, Response
from flask_restful import Resource, Api
from werkzeug.security import safe_str_cmp

from epandda import banner
from epandda import geonames
from epandda import stats
from epandda import publications
from epandda import stratigraphy
from epandda import occurrences
from epandda import query
from epandda import taxonomy
from epandda import bugReport
from epandda import lithostratigraphy
from epandda import annotations
from epandda import createAnnotation
from epandda import oauth

from flask_cors import CORS, cross_origin
import sys
import os

# add current directory to path
sys.path.append(os.getcwd())
sys.path.append(os.getcwd() + "/api")

# load config file with database credentials, Etc.
config = json.load(open('./config.json'));

# Init
app = Flask(__name__)
app.debug = True
app.config['SECRET_KEY'] = config['auth_secret']
api = Api(app)
CORS(app)


# emit banner
api.add_resource(banner.banner, '/')
api.add_resource(query.query, '/query')
api.add_resource(geonames.geonames, '/geonames')
api.add_resource(stats.stats, '/stats')
api.add_resource(publications.publications, '/publications')
api.add_resource(stratigraphy.stratigraphy, '/stratigraphy')
api.add_resource(occurrences.occurrences, '/occurrences')
api.add_resource(taxonomy.taxonomy, '/taxonomy')
api.add_resource(bugReport.bugReport, '/bugReport')
api.add_resource(lithostratigraphy.lithostratigraphy, '/lithostratigraphy')
api.add_resource(annotations.annotations, '/annotations')
api.add_resource(annotations.single, '/annotations/single/<string:annotation_id>')
api.add_resource(createAnnotation.create, '/annotations/create')
api.add_resource(oauth.oauth, '/oauth')

if __name__ == '__main__':
  app.run()
