from elastic import elasticBasedResource
import json
import hashlib
import pprint

class es_publications(elasticBasedResource):

    def process(self):

        # returns dictionary of params as defined in endpoint description
        # will throw exception if required param is not present
        params = self.getParams()

        # offset and limit returned as ints with default if not set
        offset = self.offset()
        limit = self.limit()

        # iDigBio uses DarwinCore field names
        idigbioReplacements = {
            'scientificname': 'dwc:scientificName',
            'species': 'dwc:specificEpithet',
            'genus': 'dwc:genus',
            'family': 'dwc:family',
            'order': 'dwc:order',
            'class': 'dwc:class',
            'phylum': 'dwc:phylum',
            'kingdom': 'dwc:kingdom',
            'locality': 'dwc:locality',
            'county': 'dwc:county',
            'state': 'dwc:stateProvince',
            'country': 'dwc:country'
        }

        # There are a few PBDB edge cases too
        pbdbReplacements = {
            'scientificname': 'accepted_name',
            'author': 'occ_refs-author1last',
            'doi': 'occ_refs-doi',
            'editors': 'occ_refs-editors',
            'pubno': 'occ_refs-pubno',
            'pubtitle': 'occ_refs-pubtitle',
            'pubvol': 'occ_refs-pubvol',
            'pubyear': 'ref_pubyr',
            'country': 'cc1'
        }

        if self.paramCount > 0:
            res = None

            # Elastic Search Pagination
            if params['idigbioSearchAfter']:
                idbQuery['search_after'] = json.loads(params['idigbioSearchAfter'].strip('"'))
            if params['pbdbSearchAfter']:
                pbdbQuery['search_after'] = json.loads(params['pbdbSearchAfter'].strip('"'))

            # Parse the search term parameter and get the initial result from ES
            if not params['terms']:
                raise Exception({"ERROR": "You must provide at least one field:term pair"})


            # Build queries based on passed in params
            processed = self.processSearchTerms(params, idigbioReplacements, pbdbReplacements)

            idbQuery = processed['idbQuery']
            pbdbQuery = processed['pbdbQuery']

            # Query ES Indexes
            try:
                pbdbRes = self.es.search(index="pbdb", body=pbdbQuery)
                idbRes = self.es.search(index="idigbio", body=idbQuery)
            except Exception as e:
                return self.respondWithError(errors)

            print "PBDB Query (Author, State, Phylum):"
            print pbdbQuery

            print "PBDB Results:"

            pp = pprint.PrettyPrinter(indent=4)


            # Get the fields to match on



            # Generic Category Match
            #categoryMatch = {'idigbio': '', 'pbdb': ''}
            #if params[ matchLevelName ]:
            #    for level in categoryLevels:
            #        if params[ matchLevelName ] == level:
            #            categoryMatch['pbdb'] = params[ matchLevelName ]
            #            categoryMatch['idigbio'] = 'dwc:' + params[ matchLevelName ]


            # Parse locality Match
            localityMatch = self.getLocalityMatch(params['localityMatchLevel'])

			# Parse Taxon Match
            taxonMatch = self.getTaxonMatch(params['taxonMatchLevel'], idigbioReplacements, pbdbReplacements)

			# Parse Chrono Match
			# PBDB uses numeric min/max ma so we don't have to worry about that here
            chronoMatch, chronoMatchLevel = self.getChronoMatch(params['chronoMatchLevel'])

            # TODO -- Publication Matches?
            #
            matches = self.parseMatches(params, idbRes, pbdbRes, taxonMatch, localityMatch, chronoMatch, chronoMatchLevel, pbdbType='refs')
            return matches
        else:
            return self.respondWithDescription()

    def description(self):
        return {
            'name': 'Publications Index',
            'maintainer': 'Jon Lauters',
            'maintainer_email': 'jon@epandda.org',
            'description': 'Searches PBDB and iDigBio for publication <=> specimen matches and returns match groups based on taxonomy, bibliograhy and locality',
            'private': False,
            'params': [
                {
                    "name": "terms",
                    "label": "Search terms",
                    "type": "text",
                    "required": False,
                    "description": "Search field and term pairs. Formatted in a pipe delimited list with field and term separated by a colon. For example: genus:hadrosaurus|country:united states",
                    "display": True
                },
                {
                    "name": "matchOn",
                    "label": "Match on",
                    "type": "text",
                    "required": False,
                    "description": "By default this matches records based on locality, taxonomy and chronostratigraphy. Set this parameter to skip matching on one of these fields",
                    "display": True
                },
                {
                    "name": "chronoMatchLevel",
                    "label": "Chronostratigraphy match leve",
                    "type": "text",
                    "required": False,
                    "description": "The geologic time unit to use in matching records. Allowed values: age|epoch|period|era",
                    "display": True
                },
                {
                    "name": "taxonMatchLevel",
                    "label": "Taxonomy match level",
                    "type": "text",
                    "required": False,
                    "description": "The taxonomic rank to use in matching records. Allowed values: scientificName|genus|family|order|class|phylum|kingdom",
                    "display": True
                },
                {
                    "name": "localityMatchLevel",
                    "label": "Locality match level",
                    "type": "text",
                    "required": False,
                    "description": "The geographic unit to use in matching records. Allowed values: geopoint|locality|county|state|country",
                    "display": True
                },
				{
					"name": "idigbioSearchAfter",
					"label": "iDigBio ;ast returned record to search from",
					"type": "text",
					"required": False,
					"description": "This implements paging in a more effecient way in ElasticSearch. It should be provided the last return search result in order to request a subsequent page of search results",
					"display": False
				},
				{
					"name": "pbdbSearchAfter",
					"label": "PBDB last returned record to search from",
					"type": "text",
					"required": False,
					"description": "This implements paging in a more effecient way in ElasticSearch. It should be provided the last return search result in order to request a subsequent page of search results",
					"display": False
				},
				{
					"name": "skipCache",
					"label": "Skip Cache",
					"type": "boolean",
					"required": False,
					"description": "By default ePandda caches results for 12 hours. To force the API to bypass this cache pass this parameter to retrieve a new dataset",
					"display": True
				}
            ]}
