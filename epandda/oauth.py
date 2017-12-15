import re
import json
import urllib3
import certifi
from mongo import mongoBasedResource

class oauth(mongoBasedResource):
    def process(self):

        # Load API config
        self.config = json.load(open('./config.json'))

        # Init HTTP
        http = urllib3.PoolManager( cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())

        # ORCID oAuth 
        redirect_url = self.config['orcid_redirect']
        client_id = self.config['orcid_client_id']
        orcid_url = "https://orcid.org/oauth/authorize?client_id=" + client_id + "&response_type=code&scope=/authenticate&redirect_uri=" + redirect_url

        orcid_result = http.request('GET', orcid_url)
        if 200 != orcid_result.status:

          print "ORCID Auhtorize Error:"

          print orcid_result.status
          print orcid_result.data

        elif 200 == orcid_result.status:
          result = json.loads(orcid_result.data)
 
          print "ORCID Authorize Return: "
          print result


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
                
