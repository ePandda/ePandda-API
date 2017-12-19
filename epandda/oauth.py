import re
import json
import urllib3
import certifi
from mongo import mongoBasedResource

class oauth(mongoBasedResource):
    def process(self):

        print 'In OAUTH response'

        # Get our params
        params = self.getParams()

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
          code = parms['code']

        # TODO:Move this to webform button 
        # This URL redirects user to ORCID, enters their user/pass and redirects to this endpoint with code
        #orcid_url = "https://orcid.org/oauth/authorize?client_id=" + client_id + "&response_type=code&scope=/authenticate&redirect_uri=" + redirect_url

        # Token URL
        encoded_body = json.dumps({
          'client_id': client_id,
          'client_secret': client_secret,
          'grant_type': 'authorization_code',
          'code': code,
          'redirect_uri': redirect_url
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
            'name': '',
            'maintainer': 'Jon Lauters',
            'maintainer_email': 'jon@epandda.org',
            'description': 'oauth callback',
            'params': [
            ]
        }
                
