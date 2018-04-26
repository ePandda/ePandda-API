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
from epandda import stats
from epandda import bug_report
from epandda import annotations
from epandda import es_occurrences
from epandda import es_publications
from epandda import create_annotation
from epandda import oauth
from epandda import full_match_results

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
api.add_resource(stats.stats, '/stats')
api.add_resource(bug_report.bug_report, '/bug_report')
api.add_resource(annotations.annotations, '/annotations')
api.add_resource(es_occurrences.es_occurrences, '/es_occurrences')
api.add_resource(es_publications.es_publications, '/es_publications')
api.add_resource(annotations.single, '/annotations/single/<string:annotation_id>')
api.add_resource(create_annotation.create_annotation, '/annotations/create')
api.add_resource(oauth.oauth, '/oauth')
api.add_resource(full_match_results.full_match_results, '/full_match_results')

if __name__ == '__main__':
  app.run()
