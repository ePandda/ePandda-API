from elastic import elasticBasedResource
import json
import hashlib
#
#
#
class es_occurrences(elasticBasedResource):
	def process(self):
		# returns dictionary of params as defined in endpoint description
		# will throw exception if required param is not present
		params = self.getParams()
		# offset and limit returned as ints with default if not set
		offset = self.offset()
		limit = self.limit()

		format = self.format()

		# iDigBio uses strange field names
		idigbioReplacements = {'scientificname': 'dwc:scientificName', 'dwc:specificEpithet': 'species', 'genus': 'dwc:genus', 'family': 'dwc:family', 'order': 'dwc:order', 'class': 'dwc:class', 'phylum': 'dwc:phylum', 'kingdom': 'dwc:kingdom',
		'locality': 'dwc:locality', 'county': 'dwc:county', 'state': 'dwc:stateProvince', 'country': 'dwc:country', 'geopoint': 'idigbio:geoPoint', 'basisofrecord': 'dwc:basisOfRecord', 'latestepochorhighestseries': 'dwc:latestEpochOrHighestSeries', 'earliestepochorlowestseries': 'dwc:earliestEpochOrLowestSeries'}

		# There are a few PBDB edge cases too
		pbdbReplacements = {'scientificname': 'accepted_name', 'country': 'cc1'}
		if self.paramCount > 0:
			res = None
			errors = {"GENERAL": []}
			returnMedia = False
			mediaOnly = False
			if params['returnMedia'] and params['returnMedia'].lower() == 'true':
				returnMedia = True
			if params['mediaOnly'] and params['mediaOnly'].lower() == 'true':
				mediaOnly = True
			processed = self.processSearchTerms(params, idigbioReplacements, pbdbReplacements)
			idbQuery = processed['idbQuery']
			pbdbQuery = processed['pbdbQuery']

			if params['idigbioSearchAfter']:
				idbQuery['search_after'] = json.loads(params['idigbioSearchAfter'].strip('"'))
			if params['pbdbSearchAfter']:
				pbdbQuery['search_after'] = json.loads(params['pbdbSearchAfter'].strip('"'))

			# Add geo match filters
			if params['geoPointFields']:
				pbdbQuery, idbQuery = self.addGeoFilters(idbQuery, pbdbQuery, params['geoPointFields'])
			idbRes = None
			pbdbRes = None
			if idbQuery:
				try:
					idbRes = self.es.search(index="idigbio", body=idbQuery)
				except Exception as e:
					errors['GENERAL'].append("iDigBio search failed")
			else:
				idbRes = {'hits':{'total': 0}}
			if pbdbQuery:
				try:
					pbdbRes = self.es.search(index="pbdb", body=pbdbQuery)
				except Exception as e:
					errors['GENERAL'].append("PBDB search failed")
			else:
				pbdbRes = {'hits':{'total': 0}}
			matches = {'results': {}}

			# Parse locality Match
			localityMatch = self.getLocalityMatch(params['localityMatchLevel'])

			# Parse Taxon Match
			taxonMatch = self.getTaxonMatch(params['taxonMatchLevel'], idigbioReplacements, pbdbReplacements)

			# Parse Chrono Match
			# PBDB uses numeric min/max ma so we don't have to worry about that here
			chronoMatch, chronoMatchLevel = self.getChronoMatch(params['chronoMatchLevel'])
			if len(errors["GENERAL"]) > 0:
				return self.respondWithError(errors)

			matches = self.parseMatches(params, idbRes, pbdbRes, taxonMatch, localityMatch, chronoMatch, chronoMatchLevel, returnMedia=returnMedia, mediaOnly=mediaOnly)
			return matches
		else:
			return self.respondWithDescription()


	def description(self):
		return {
			'name': 'Occurrence Index',
			'maintainer': 'Michael Benowitz',
			'maintainer_email': 'michael@epandda.org',
			'description': 'Searches PBDB and iDigBio for occurrences and returns match groups based on taxonomy, chronostratigraphy and locality',
			'private': False,
			'params': [
				{
					"name": "terms",
					"label": "Search terms",
					"type": "text",
					"required": False,
					"description": "The main search field. A simple query can be made on any taxonomic term, to query a specific field provide field and term pairs formatted in a pipe delimited list with field and term separated by a colon. For example: genus:hadrosaurus|country:united states",
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
					"label": "Chronostratigraphy match level",
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
					"name": "geoPointFields",
					"label": "Search by georeference",
					"type": "text",
					"required": False,
					"description": "Search records by georeference, either by distance from a point, within a bounding box or within a polygon",
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
					"name": "returnMedia",
					"label": "Return media from iDigBio",
					"type": "boolean",
					"required": False,
					"description": "Toggle to return any matching media from iDigBio",
					"display": True
				},
				{
					"name": "mediaOnly",
					"label": "Return only media records from iDigBio",
					"type": "boolean",
					"required": False,
					"description": "Set to return only media records from iDigBio that match your results",
					"display": True
				},
				{
					"name": "skipCache",
					"label": "Skip Cache",
					"type": "boolean",
					"required": False,
					"description": "By default ePandda caches results for 12 hours. To force the API to bypass this cache pass this parameter to retrieve a new dataset",
					"display": True
				},
				{
					"name": "format",
					"label": "Data format",
					"type": "string",
					"required": False,
					"description": "Format of return data. Default is JSON. Other possible values are CSV, TAB.",
					"display": True
				}
			]
		}
