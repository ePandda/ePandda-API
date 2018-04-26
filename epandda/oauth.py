import re
import json
import urllib3
import certifi
from flask import request, Response
from mongo import mongoBasedResource

class oauth(mongoBasedResource):

    # Overloaded getParams to bypass default lower() functionality.
    # Removed default to always include offset, limit, and source field lists.
    def getParams(self):

        if  self.paramCount > 0:
            return self.params

        desc = self.description()['params'][:]

        r = self.getRequest()
        r_as_json = request.get_json(silent=True)

        c = 0
        self.params = {}

        for p in desc:
            # JSON blob
            if r_as_json is not None:
                if(p['name'] in r_as_json):
                    self.params[p['name']] = r_as_json[p['name']]
                    c = c + 1
                else:
                    self.params[p['name']] = None

            # POST request
            elif request.method == 'POST':
                if(p['name'] in request.form):
                    self.params[p['name']] = request.form[p['name']]
                    c = c + 1
                else:
                    self.params[p['name']] = None

            # GET request
            elif request.method == 'GET':
                v = r.args.get(p['name'])
                if (v):
                    self.params[p['name']] = v
                    c = c + 1
                else:
                    self.params[p['name']] = None

        self.validateParams()
        self.paramCount = c

        return self.params



    def process(self):

        print 'In OAUTH response'

        # Get our params
        params = self.getParams()
        print "oauth params:"
        print params

        # Load API config
        self.config = json.load(open('./config.json'))

        # Init HTTP
        http = urllib3.PoolManager( cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())

        # ORCID oAuth
        redirect_url = self.config['orcid_redirect']
        client_id = self.config['orcid_client_id']
        client_secret = self.config['orcid_client_secret']
        code = ''
        if 'code' in params:
          code = params['code']

        print "CODE:"
        print code

        # TODO:Move this to webform button
        # This URL redirects user to ORCID, enters their user/pass and redirects to this endpoint with code
        #orcid_url = "https://orcid.org/oauth/authorize?client_id=" + client_id + "&response_type=code&scope=/authenticate&redirect_uri=" + redirect_url

        # Token URL
        encoded_body = json.dumps({
          'client_id': client_id,
          'client_secret': client_secret,
          'grant_type': 'authorization_code',
          'redirect_uri': redirect_url,
          'code': code,
        })
        orcid_url = "https://orcid.org/oauth/token"

        orcid_result = http.request('POST', orcid_url, headers={'Content-Type': 'application/json'}, body=encoded_body)
        if 200 != orcid_result.status:

          print "ORCID Auhtorize Error:"

          print orcid_result.status
          print orcid_result.data

        elif 200 == orcid_result.status:

          print "ORCID Authorize Return: "
          print orcid_result.data

          # Return object should contain:
          # access_token
          # token_type
          # refresh_token
          # expires_in
          # scope
          # orcid
          # name


        #return self.respond({
        #    'counts': 0,
        #   'results': [],
        #   'criteria': [],
        #})



    # Stub / "hidden endpoint"
    def description(self):
		return {
            'name': 'oauth',
            'maintainer': 'Jon Lauters',
            'maintainer_email': 'jon@epandda.org',
            'description': 'oauth callback',
            'private': True,
            'params': [
                {
                    "name": "code",
                    "label": "Authentication Code",
                    "type": "text",
                    "required": False,
                    "description": "Authentication Code to pass to ORCid oAuth to get access_token"
                }
            ]
        }
