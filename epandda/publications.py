import re
from mongo import mongoBasedResource
from flask_restful import reqparse

#parser = reqparse.RequestParser()

#parser.add_argument('scientific_name', type=str, help='Taxonomic name to search bibliographic records for')
#parser.add_argument('journal', type=str, help='Journal name where taxon was described')
#parser.add_argument('article', type=str, help='Article name where taxon was described')
#parser.add_argument('author', type=str, help='One of the authors of article describing taxon')
#parser.add_argument('stateProvinceName', type=str, help='State or province name to filter described taxon results')
#parser.add_argument('county', type=str, help='County name to filter described taxon results')
#parser.add_argument('locality', type=str, help='Locality name to filter described taxon results')

#
#
#
class publications(mongoBasedResource):
    def process(self):

        # Mongodb index for Publication
        pubIndex = self.client.endpoints.pubIndexV2


        # returns dictionary of params as defined in endpoint description
        # will throw exception if required param is not present
        params = self.getParams()

        if 'includeAnnotations' in params:
		if "true" == params['includeAnnotations']:
        	  params['includeAnnotations'] = True
        	else:
        		  params['includeAnnotations'] = False
        else:
        	  params['includeAnnotations'] = False

        # offset and limit returned as ints with default if not set
        offset = self.offset()
        limit = self.limit()

        if limit < 1:
          limit = 100

        pubQuery = []
        minScore = 0
        if self.paramCount > 0:

          criteria = {
            'endpoint': 'publications',
            'parameters': {},
            'matchPoints': [],
            'matchTerms': {
              'stateProvinceNames': [],
              'countryNames': [],
              'countyNames': [],
              'localityNames': [],
              'originalStates': [],
              'originalCountries': [],
              'originalCounties': [],
              'originalLocalities': []
            }
          }

          for p in ['stateProvinceName', 'author', 'scientific_name', 'journal', 'locality', 'county', 'article', 'countryName']:  

            if params[p]:

              if 'scientific_name' == p:
                higher_taxa = str(params[p]).lower()
                pubQuery.append({ "$or": [{"higher_taxa": higher_taxa }, { "index_term": { "$regex": re.compile(higher_taxa, re.IGNORECASE) }} ] })

              if 'stateProvinceName' == p:
                state = str(params[p]).lower()
                pubQuery.append({"states": re.compile(state, re.IGNORECASE)})
                criteria['matchTerms']['stateProvinceNames'].append( state )

              if 'countryName' == p:
                 country = str(params[p]).lower()
                 pubQuery.append({"countries": re.compile(country, re.IGNORECASE)})
                 criteria['matchTerms']['countryNames'].append( country )

              if 'county' == p:
                county = str(params[p]).lower()
                pubQuery.append({"counties": re.compile(county, re.IGNORECASE)})
                criteria['matchTerms']['countyNames'].append( county )

              # ... This doesn't exist as a thing yet :/
              if 'locality' == p:
                locality = str(params[p]).lower()

                # Search localities index if locality name given ..
                # Get state and county for this
                # Filter iDigBio results by locality ...


                pubQuery.append({"locality": locality})
                criteria['matchTerms']['localityNames'].append( locality )

              if 'author' == p:
                author = str(params[p]).lower()
                pubQuery.append({ "$or": [{ "author1_last": re.compile(author, re.IGNORECASE)}, {"author2_last": re.compile(author, re.IGNORECASE)}]})

              if 'journal' == p:
                journal = str(params[p]).lower()
                pubQuery.append({"pubtitle": { '$regex': re.compile(journal, re.IGNORECASE)}})

              if 'article' == p:
                article = str(params[p])
                pubQuery.append({"index_term": { '$regex': re.compile(article, re.IGNORECASE)} })
              
              if 'minimumScore' == p:
                minScore = params[p]

              if 'minimumScore' == p:
                minScore = params[p]

              criteria['parameters'][p] = str(params[p]).lower()

          d = []
          matches = {'idigbio': [], 'pbdb': [], 'faceted_matches': []}
          idbCount = 0
          pbdbCount = 0

          res = pubIndex.find({"$and":  pubQuery })
          if res:

            for i in res:

              if 'vetted' in i:

                for idb in i['vetted']:
                  if idb['score'] >= minScore:
                    matches['faceted_matches'].append({ 'pbdb_id': i['pid'], 'idigbio_uuid': idb['uuid'], 'matchedOn': idb['matched_on'], 'score': idb['score']})
                    matches['idigbio'].append( idb['uuid'] )

              matches['pbdb'].append( i['pid'] )

              if 'countryName' in i and i['countryName'] not in criteria['matchTerms']['countryNames']:
                criteria['matchTerms']['countryNames'].append(i['countryName'])

              if 'stateProvinceName' in i and i['stateProvinceName'] not in criteria['matchTerms']['stateProvinceNames']:
                criteria['matchTerms']['stateProvinceNames'].append(i['stateProvinceName'])

              if 'county' in i and i['county'] not in criteria['matchTerms']['countyNames']:
                criteria['matchTerms']['countyNames'].append(i['county'])

              if 'locality' in i:
                if i['locality'] not in criteria['matchTerms']['localityNames']:
                  criteria['matchTerms']['localityNames'].append(i['locality'])


              if 'originalStateProvinceName' in i:
                for origState in i['originalStateProvinceName']:
                  if origState not in criteria['matchTerms']['originalStates']:
                    criteria['matchTerms']['originialStates'].append(origState)

              if 'originalCountryName' in i:
                for origCountry in i['originalCountryName']:
                  if origCountry not in criteria['matchTerms']['originalCountries']:
                    criteria['matchTerms']['originalContries'].append(origCountry)

              if 'original_county' in i:
                for origCounty in i['original_county']:
                  if origCounty not in criteria['matchTerms']['originalCounties']:
                    criteria['matchTerms']['originalCounties'].append(origCounty)

              if 'original_locality' in i:
                for origLocality in i['original_locality']:
                  if origLocality not in criteria['matchTerms']['originalLocalities']:
                    criteria['matchTerms']['originalLocalities'].append(origLocality)

          finalMatches = {'idigbio': [], 'pbdb': []}
          finalMatches['idigbio'] = matches['idigbio']
          finalMatches['pbdb'] = matches['pbdb']

          idbCount = len(finalMatches['idigbio'])
          pbdbCount = len(finalMatches['pbdb'])

          resolveSet = { 'idigbio': finalMatches['idigbio'],
                         'pbdb': finalMatches['pbdb']}


          d.append({'matches': finalMatches})
          d = self.resolveReferences(d,'refs', 'both' )

          counts = {
            'totalCount': idbCount + pbdbCount,
            'idbCount': idbCount,
            'pbdbCount': pbdbCount
          }

          print "Responding data package ..."
          return self.respond({
              'counts': counts,
              'results': d,
              'criteria': criteria,
              'includeAnnotations': params['includeAnnotations'],
              'faceted_matches': matches['faceted_matches']
          })
        else:

          return self.respondWithDescription()


    def description(self):
        return {
            'name': 'Publication index',
            'maintainer': 'Jon Lauters',
            'maintainer_email': 'jon@epandda.org',
            'description': 'Returns specimen and publication records for a given scientific name. Results may be filtered using the available parameters.',
            'params': [
                {
                    "name": "scientific_name",
                    "label": "Scientific Name",
                    "type": "text",
                    "required": False,
                    "description": "Taxon to search occurrence records for",
                    "display": True,
					"formGroup": "search"
                },
                {
                    "name": "journal",
                    "label": "Journal",
                    "type": "text",
                    "required": False,
                    "description": "Then name of academic Journal a publication would be found",
                    "display": True,
					"formGroup": "search"
                },
                {
                    "name": "article",
                    "label": "Article",
                    "type": "text",
                    "required": False,
                    "description": "The name of the journal article the given scientific_name appears in",
                    "display": True,
					"formGroup": "search"
                },
                {
                    "name": "author",
                    "label": "Author",
                    "type": "text",
                    "required": False,
                    "description": "The name of the author who's article describes the given scientific_name",
                    "display": True,
					"formGroup": "search"
                },
                {
                    "name": "stateProvinceName",
                    "label": "State/Province",
                    "type": "text",
                    "required": False,
                    "description": "The state/province to search for scientific_name and publication references",
                    "display": True,
					"formGroup": "search"
                },
                {
                    "name": "countryName",
                    "label": "Country",
                    "type": "text",
                    "required": False,
                    "description": "The Country Name to search for publication references and specimens in",
                    "display": True,
					"formGroup": "search"
                },
                {
                    "name": "county",
                    "label": "County",
                    "type": "text",
                    "required": False,
                    "description": "The county to search for scientific_name and publication references",
                    "display": True,
					"formGroup": "search"
                },
                {
                    "name": "locality",
                    "label": "Locality",
                    "type": "text",
                    "required": False,
                    "description": "The locality name to search for scientific_name occurences and publication references",
                    "display": True,
					"formGroup": "search"
                },
                {
                    "name": "includeAnnotations",
                    "label": "Include Annotations",
                    "type": "boolean",
                    "required": False,
                    "description": "Toggles if OpenAnnotations section should be included or not",
                    "display": True,
					"formGroup": "settings"
                },
                {
                    "name": "minimumScore",
                    "label": " Minimum Score",
                    "type": "integer",
                    "required": False,
                    "description": "The minimum number of fields that must match before iDigBio specimens are returned",
                    "display": True,
					"formGroup": "settings"
                },
				{
					"name": "format",
					"label": "Data format",
					"type": "text",
					"required": False,
					"description": "Format of return data. Default is JSON. Other possible values are CSV, TAB.",
					"display": True,
					"formGroup": "settings",
					"validOptions": ["JSON", "CSV", "TAB"]
				}]}
