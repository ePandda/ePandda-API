from mongo import mongoBasedResource
from flask_restful import reqparse
import gridfs
import json
from elasticsearch import Elasticsearch
import hashlib

parser = reqparse.RequestParser()

# Add Arguments (params) to parser here ...
parser.add_argument('taxon', type=str, help='Taxonomic name to search occurrences for')
parser.add_argument('locality', type=str, help='Locality name to filter taxonomic occurrences by')
parser.add_argument('period', type=str, help='The geologic time period to filter taxonomic occurrences by')
parser.add_argument('institution_code', type=str, help='The abbreviated code submitted by data provider to filter taxonomic occurrences by')

#
#
#
class es_occurrences(mongoBasedResource):
	def process(self):

		# We use Elasticsearch to do matching
		es = Elasticsearch(['http://whirl.mine.nu:9200'])
		# returns dictionary of params as defined in endpoint description
		# will throw exception if required param is not present
		params = self.getParams()
		# offset and limit returned as ints with default if not set
		offset = self.offset()
		limit = self.limit()

		# iDigBio uses strange field names
		idigbioReplacements = {'scientificname': 'dwc:scientificName', 'dwc:specificEpithet': 'species', 'genus': 'dwc:genus', 'family': 'dwc:family', 'order': 'dwc:order', 'class': 'dwc:class', 'phylum': 'dwc:phylum', 'kingdom': 'dwc:kingdom',
		'locality': 'dwc:locality', 'county': 'dwc:county', 'state': 'dwc:stateProvince', 'country': 'dwc:country'}

		# There are a few PBDB edge cases too
		pbdbReplacements = {'scientificName': 'accepted_name'}

		if self.paramCount > 0:
			res = None
			query = {
				"from": offset,
				"size": limit,
				"query":{
					"bool":{
						"should":[]
					}
				}
			}

			# Parse the search term parameter and get the initial result from ES
			if not params['terms']:
				return {"ERROR": "You must provide at least one field:term pair"}
			searchTerms = params['terms'].split('|')
			for search in searchTerms:
				field, term = search.split(':')
				if field in idigbioReplacements:
					query['query']['bool']['should'].append({"match": {idigbioReplacements[field]: term}})
				if field in pbdbReplacements:
					query['query']['bool']['should'].append({"match": {pbdbReplacements[field]: term}})
					continue
				if field == 'geopoint' or field == 'paleogeopoint':
					if field == 'paleogeopoint':
						field = 'paleoGeoPoint'
					else:
						field = 'geoPoint'
					# Check to see if a radius was set, else use default (10km)
					matchRadius = '10km'
					if params['geoPointRadius']:
						matchRadius = params['geoPointRadius']
					query['query']['bool']['filter'] = {'geo_distance': {'distance': matchRadius, field: term}}
					continue
				query['query']['bool']['should'].append({"match": {field: term}})

			if len(query['query']['bool']['should']) == 0:
				query['query']['bool']['should'] = {'match_all': {}}
			res = es.search(index="endpoint", body=query)
			# Get the fields to match on
			matches = {}
			localityMatch = {'idigbio': 'dwc:county', 'pbdb': 'county'}
			if params['localityMatchLevel']:
				localityMatch['pbdb'] = params['localityMatchLevel']
				localityMatch['idigbio'] = 'dwc:' + params['localityMatchLevel']
				if params['localityMatchLevel'] == 'state':
					localityMatch['idigbio'] = localityMatch['idigbio'] + 'Province'

			taxonMatch = {'idigbio': 'dwc:genus', 'pbdb': 'genus'}
			if params['taxonMatchLevel']:
				localityMatch['pbdb'] = params['taxonMatchLevel']
				localityMatch['idigbio'] = 'dwc:' + params['taxonMatchLevel']

			# PBDB uses numeric min/max ma so we don't have to worry about that here
			chronoMatch = {'idigbio': {'early': 'dwc:earliestPeriodOrLowestSystem', 'late': 'dwc:latestPeriodOrHighestSystem'}}
			if params['chronoMatchLevel']:
				if params['chronoMatchLevel'] == 'stage':
					chronoMatch = {'idigbio': {'early': 'dwc:earliestAgeOrLowestStage', 'late': 'dwc:latestAgeOrHighestStage'}}
				elif params['chronoMatchLevel'] == 'series':
					chronoMatch = {'idigbio': {'early': 'dwc:earliestEpochOrLowestSeries', 'late': 'dwc:latestEpochOrHighestSeries'}}
				elif params['chronoMatchLevel'] == 'erathem':
					chronoMatch = {'idigbio': {'early': 'dwc:earliestEraOrLowestErathem', 'late': 'dwc:latestEraOrHighestErathem'}}
			if res:
				for hit in res['hits']['hits']:
					recHash = hashlib.sha1()
					data = {'sourceRecords': [], 'links': [], 'matchFields': {}}
					matchList = []
					if hit['_type'] == 'idigbio':

						# Store the primary ID
						linkID = hit['_source']['idigbio:uuid']
						linkField= 'idigbio:uuid'

						matchList.append({"match": {localityMatch['pbdb']: hit['_source'][localityMatch['idigbio']]}})
						data['matchFields'][localityMatch['idigbio']] = hit['_source'][localityMatch['idigbio']]
						recHash.update(data['matchFields'][localityMatch['idigbio']])

						matchList.append({"match": {taxonMatch['pbdb']: hit['_source'][taxonMatch['idigbio']]}})
						data['matchFields'][taxonMatch['idigbio']] = hit['_source'][taxonMatch['idigbio']]
						recHash.update(data['matchFields'][taxonMatch['idigbio']])

						chrono_early = chrono_late = None
						ma_start_score = ma_end_score = 0
						ma_start = 1000
						ma_end = 0
						if hit['_source'][chronoMatch['idigbio']['early']] and hit['_source'][chronoMatch['idigbio']['late']]:
							chrono_early = hit['_source'][chronoMatch['idigbio']['early']]
							chrono_late = hit['_source'][chronoMatch['idigbio']['late']]
						elif hit['_source'][chronoMatch['idigbio']['early']]:
							chrono_early = hit['_source'][chronoMatch['idigbio']['early']]
							chrono_late = hit['_source'][chronoMatch['idigbio']['early']]
						elif hit['_source'][chronoMatch['idigbio']['late']]:
							chrono_early = hit['_source'][chronoMatch['idigbio']['late']]
							chrono_late = hit['_source'][chronoMatch['idigbio']['late']]
						if chrono_early and chrono_late:
							ma_start_res = es.search(index="chronolookup", body={"query": {"match": {"name": {"query": chrono_early, "fuzziness": "AUTO"}}}})
							for ma_start_hit in ma_start_res['hits']['hits']:
								if hit['_score'] > ma_start_score:
									ma_start = ma_start_hit["_source"]["start_ma"]
									ma_start_score = ma_start_hit['_score']
							ma_end_res = es.search(index="chronolookup", body={"query": {"match": {"name": {"query": chrono_late, "fuzziness": "AUTO"}}}})
							for ma_end_hit in ma_end_res['hits']['hits']:
								if hit['_score'] > ma_end_score:
									ma_end = ma_start_hit["_source"]["end_ma"]
									ma_end_score = ma_start_hit['_score']
							data['matchFields']['chronostratigraphy'] = [ma_start, ma_end]
							matchList.append({"range": {"min_ma": {"lte": ma_start}}})
							matchList.append({"range": {"max_ma": {"gte": ma_end}}})
						recHash.update(str(ma_start)+str(ma_end))

						linkQuery = {
							"size": 1000,
							"query":{
								"bool":{
									"must": matchList
								}
							}
						}
					elif hit['_type'] == 'pbdb':

						matchList.append({"match": {localityMatch['idigbio']: hit['_source'][localityMatch['pbdb']]}})
						data['matchFields'][localityMatch['pbdb']] = hit['_source'][localityMatch['pbdb']]
						recHash.update(data['matchFields'][localityMatch['pbdb']])

						matchList.append({"match": {taxonMatch['idigbio']: hit['_source'][taxonMatch['pbdb']]}})
						data['matchFields'][taxonMatch['pbdb']] = hit['_source'][taxonMatch['pbdb']]
						recHash.update(data['matchFields'][taxonMatch['pbdb']])

						matchChrono = []
						ma_start_res = es.search(index="chronolookup", body={"query": {"match": {"start_ma": {"query": hit['_source']['min_ma']}}}})
						for ma_start_hit in ma_start_res['hits']['hits']:
							matchChrono.append(ma_start_hit['_source']['name'])
						ma_end_res = es.search(index="chronolookup", body={"query": {"match": {"name": {"query": hit['_source']['max_ma']}}}})
						for ma_end_hit in ma_end_res['hits']['hits']:
							matchChrono.append(ma_end_hit['_source']['name'])
						data['matchFields']['chronostratigraphy'] = matchChrono
						recHash.update(str(matchChrono))
						linkID = hit['_source']['occurrence_no']
						linkField = 'occurrence_no'
						linkQuery = {
							"size": 1000,
							"query":{
								"bool":{
									"must": matchList,
									"should": [
										{"terms": {"dwc:earliestAgeOrLowestStage": matchChrono}},
										{"terms": {"dwc:latestAgeOrHighestStage": matchChrono}},
										{"terms": {"dwc:earliestPeriodOrLowestSystem": matchChrono}},
										{"terms": {"dwc:latestPeriodOrHighestSystem": matchChrono}},
										{"terms": {"dwc:earliestEpochOrLowestSeries": matchChrono}},
										{"terms": {"dwc:latestEpochOrHighestSeries": matchChrono}},
										{"terms": {"dwc:earliestEraOrLowestErathem": matchChrono}},
										{"terms": {"dwc:latestEraOrHighestErathem": matchChrono}}
									]
								}
							}
						}
					linkResult = es.search(index="endpoint", body=linkQuery)
					for link in linkResult['hits']['hits']:
						if hit['_type'] == 'idigbio':
							data['links'].append([link['_source']['occurrence_no'], link['_score']])
						elif hit['_type'] == 'pbdb':
							data['links'].append([link['_source']['idigbio:uuid'], link['_score']])
					hashRes = recHash.hexdigest()
					if hashRes in matches:
						matches[hashRes]['sources'].append(linkID)
						for link in data['links']:
							if {link[0]: link[1]} not in matches[hashRes]['matches']:
								matches[hashRes]['matches'].append({link[0]: link[1]})
					else:
						matches[hashRes] = {'fields': data['matchFields'], 'matches': [], 'sources': [linkID]}
						for link in data['links']:
							matches[hashRes]['matches'].append({link[0]: link[1]})
						matches[hashRes]['fields'] = data['matchFields']
				return matches
		else:
			return self.respondWithDescription()

	def description(self):
		return {
			'name': 'Occurrence index',
			'maintainer': 'Michael Benowitz',
			'maintainer_email': 'michael@epandda.org',
			'description': 'Returns specimens collected from a given locality',
			'params': [
				{
					"name": "terms",
					"label": "Search terms",
					"type": "text",
					"required": False,
					"description": "Search field and term pairs. Formatted in a pipe delimited list with field and term separated by a colon. For example: genus:hadrosaurus|country:united states"
				},
				{
					"name": "matchOn",
					"label": "Match on",
					"type": "text",
					"required": False,
					"description": "By default this matches records based on locality, taxonomy and chronostratigraphy. Set this parameter to skip matching on one of these fields",
				},
				{
					"name": "chronoMatchLevel",
					"label": "Chronostratigraphy match level",
					"type": "text",
					"required": False,
					"description": "The geologic time unit to use in matching records. Allowed values: stage|system|series|erathem"
				},
				{
					"name": "taxonMatchLevel",
					"label": "Taxonomy match level",
					"type": "text",
					"required": False,
					"description": "The taxonomic rank to use in matching records. Allowed values: scientificName|genus|family|order|class|phylum|kingdom"
				},
				{
					"name": "localityMatchLevel",
					"label": "Locality match level",
					"type": "text",
					"required": False,
					"description": "The geographic unit to use in matching records. Allowed values: geopoint|locality|county|state|country"
				},
				{
					"name": "geoPointRadius",
					"label": "Maximum Distance from Geopoint",
					"type": "integer",
					"required": False,
					"description": "The maximum radius, in km, to return results within if querying on a geoPoint field"
				}
			]}
