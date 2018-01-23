import json
import requests
from flask import request, Response
from mongo import mongoBasedResource

# Human Created Annotations
#
# oAuth: ORCID Public API
#
# Get user's ORCID ID ( and maybe valid auth_token? ) from implemented form 
# Verify user has valid ORCID
# Verify and process annotation data object
# Insert into DB if successful and return msg and error status back to implementee form


# Create Annotation Class

class create(mongoBasedResource):

  
  # User Created Annotation submission
  def process(self):

      # Monogodb index for Annotations
      annotations = self.client.endpoints.annotations

      # returns dictionary of params as defined in endpoint description
      # will throw exception if required param is not present
      params = self.getParams()

      print "Create Annotations Params: "
      print params

      # Load API config
      self.config = json.load(open('./config.json'))
  
      # ORCID oAuth Config
      redirect_url  = self.config['orcid_redirect']
      client_id     = self.config['orcid_client_id']
      client_secret = self.config['orcid_client_secret']     

      # data params
      encoded_body = {
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': 'client_credentials',
        'scope': '/read-public'
      } 

      orcid_url = "https://orcid.org/oauth/token"

      # Requests format works with ORCID
      r = requests.post(orcid_url, data=encoded_body, headers={"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"})

      results_json = r.json()

      print "results from getting access token for public api"
      print results_json

      access_token = results_json['access_token']

      print "access token:"
      print str(access_token)

      if access_token:
      
          # Our client login worked, now let's get the ORCID-bio for our user

          orcidbio_url = 'https://orcid.org/0000-0003-4910-9085'
          orcid_email = 'jonathan.lauters@gmail.com'
          orcid = orcidbio_url.replace('https://orcid.org/', '')

          print "TEST orcid ( mine ): "
          print orcid

          bio_url = 'https://api.sandbox.orcid.org/v2.1/' + str(orcid) + '/person'
          bio_r = requests.get(bio_url, headers={"Content-Type": "application/orcid+json", "Authorization": "Bearer " + access_token})

          bio_results = bio_r.json()


          print "ORCID User Bio Results: "
          print bio_results










      # TODO: Process and validate data if 


      return self.respondWithDescription()

  # Set up Parameters
  def description(self):
      return {
          'name': 'Create Annotations',
          'maintainer': 'Jon Lauters',
          'maintainer_email': 'jon@epandda.org',
          'description': 'Human created Annotations endpoint (ORCID verified )',
          'params': [
              {
                  "name": "orcid",
                  "label": "ORCID",
                  "type": "text",
                  "required": False,
                  "description": "The unique identifier assigned by https://orcid.org/"
              },
              {
                  "name": "email",
                  "label": "Email Address",
                  "type": "text",
                  "required": False,
                  "description": "Email address associated with ORCID account"
              },
              {
                  "name": "annotation_tye",
                  "label": "Annotation Type",
                  "type": "text",
                  "required": False,
                  "description": "Match, Missing Data, Data Correction flag"
              },
              {
                  "name": "target_url",
                  "label": "Annotation Target URL",
                  "type": "text",
                  "required": False,
                  "description": "The URL for the target of the annotation ( specimen URL from iDigBio )"
              },
              {
                  "name": "body_url",
                  "label": "Annotation Body URL",
                  "type": "text",
                  "required": False,
                  "description": "The URL for the body of the annotation ( publication URL from PBDB )"
              },
              {
                  "name": "body_title",
                  "label": "Annotation Body Title",
                  "type": "text",
                  "required": False,
                  "description": "The Title of the annotation Body ( article title from PBDB )"
              },
              {
                  "name": "body_doi",
                  "label": "Annotation Body DOI",
                  "type": "text",
                  "required": False,
                  "description": "The DOI of the annotation Body"
              },
              { 
                  "name": "missing_data",
                  "label": "Missing Data",
                  "type": "text",
                  "required": False,
                  "description": "List of key value pairs of missing data"
              },
              {
                  "name": "data_correction",
                  "label": "Data Correction",
                  "type": "text",
                  "required": False,
                  "description": "List of key value pairs with description of why data correction fixes issue"
              }
              
          ] 
      } 
