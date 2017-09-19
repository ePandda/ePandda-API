from mongo import mongoBasedResource
from flask_restful import reqparse
import gridfs
import json
import re
from elasticsearch import Elasticsearch

parser = reqparse.RequestParser()

parser.add_argument('scientific_name', type=str, help='taxonomic name to search stratigraphy for')
parser.add_argument('formation', type=str, help='formation name to filter taxonomic results by')
parser.add_argument('state_province', type=str, help='state or province name to filter taxonomic results by')
parser.add_argument('county', type=str, help='county name to filter taxonomic results by')
parser.add_argument('locality', type=str, help='locality name to filter taxonomic results by')

#
#
#
class stratigraphy(mongoBasedResource):
    def process(self):
    	# Mongodb index for localities
		sindex = self.client.endpoints.chronoStratIndex
		lookup = self.client.endpoints.chronostratLookup
		# Mongodb gridFS instance
		grid = gridfs.GridFS(self.client.endpoints)                   
	
		# We use Elasticsearch to provide fuzzy matching on search terms
		es = Elasticsearch(['http://whirl.mine.nu:9200'])	
	
		# returns dictionary of params as defined in endpoint description
		# will throw exception if required param is not present
		params = self.getParams()
		# offset and limit returned as ints with default if not set
		offset = self.offset()
		limit = self.limit()

		if self.paramCount > 0:
			imageRes = None
			res = None
			criteria = {'endpoint': 'stratigraphy', 'parameters': {}, 'matchTerms': []}
			stratQuery = []
			for p in [{'stage': ['lowStage', 'highStage', 'upperChronostratigraphy.stage', 'lowerChronostratigraphy.stage']}, {'series': ['lowSeries', 'highSeries', 'upperChronostratigraphy.series', 'lowerChronostratigraphy.series']}, {'system': ['lowSystem', 'highSystem', 'upperChronostratigraphy.system', 'lowerChronostratigraphy.system']}, {'erathem': ['lowErathem', 'highErathem', 'upperChronostratigraphy.erathem', 'lowerChronostratigraphy.erathem']}]:
				val = p.keys()[0]
				if (params[val]):
					criteria['parameters'][val] = params[val]
					chrono_range = re.match(r'^([\w\s]+)[\s]*(?:-|to|$)[\s]*([\w\s]*)$', params[val])
					chrono_early = chrono_range.group(1)
					if chrono_range.group(2) != '':
						chrono_late = chrono_range.group(2)
					else:
						chrono_late = chrono_early
					ma_start_score = ma_end_score = 0
					ma_start_res = es.search(index="chronolookup", body={"query": {"match": {"name": {"query": chrono_early, "fuzziness": "AUTO"}}}})
					for hit in ma_start_res['hits']['hits']:
						if hit['_score'] > ma_start_score:
							ma_start = hit["_source"]["start_ma"]
							ma_start_score = hit['_score']
					ma_end_res = es.search(index="chronolookup", body={"query": {"match": {"name": {"query": chrono_late, "fuzziness": "AUTO"}}}})
					for hit in ma_end_res['hits']['hits']:
						if hit['_score'] > ma_end_score:
							ma_end = hit["_source"]["end_ma"]
							ma_end_score = hit['_score']
					stratQuery.append({'earlyBound': {'$lte': ma_start}, 'lateBound': {'$gte': ma_end}})
			if(params['chronostratigraphy']):
				criteria['parameters']['chronostratigraphy'] = params['chronostratigraphy']
				stratQuery.append({'$text': {'$search': '"' + params['chronostratigraphy'] + '"'}})

			if (len(stratQuery) == 0):
				return self.respondWithError({"GENERAL": "No valid parameters specified"})
			if len(stratQuery) > 0:
				res = sindex.find({'$or': stratQuery})
			d = []
			matches = {'idigbio': [], 'pbdb': []}
			idbCount = 0
			pbdbCount = 0
			if res:
				for i in res:
					temp_doc = {}
					for level in ['lowStage', 'highStage', 'lowSeries', 'highSeries', 'lowSystem', 'highSystem', 'lowErathem', 'highErathem', 'upperChronostratigraphy', 'lowerChronostratigraphy']:
						if level in i:
							temp_doc[level] = i[level]
					criteria['matchTerms'].append(temp_doc)
					
					if 'pbdbGridFile' in i:
						pbdbGrids = i['pbdbGridFile']
						for file in pbdbGrids:
							pbdb_doc = grid.get(file)
							pbdb_matches = json.loads(pbdb_doc.read())
							matches['pbdb'] = matches['pbdb'] + pbdb_matches

					if 'idbGridFile' in i:
						if type(i['idbGridFile']) is list:
							idbGrids = i['idbGridFile']
							for file in idbGrids:
								idb_doc = grid.get(file)
								idb_matches = json.loads(idb_doc.read())
								matches['idigbio'] = matches['idigbio'] + idb_matches
						else:
							idb_doc = grid.get(i['idbGridFile'])
							idb_matches = json.loads(idb_doc.read())
							matches['idigbio'] = matches['idigbio'] + idb_matches
			
			idbCount = len(matches['idigbio'])
			pbdbCount = len(matches['pbdb'])

			item = {'matches': {'idigbio': matches['idigbio'], 'pbdb': matches['pbdb']}}
			d.append(item)
			d = self.resolveReferences(d)
			counts = {'totalCount': idbCount + pbdbCount, 'idbCount': idbCount, 'pbdbCount': pbdbCount}

			#d['pbdb_resolved'] = d['pbdb_resolved'][offset:limit]
			
			media = []
			if imageRes:
				for m in imageRes:
					images = m['media_uris']
					for image in images:
						media.append(image)
			return self.respond({'counts': counts, 'results': d, 'criteria': criteria, 'media': media})

		else:
			return self.respondWithDescription()
    
    def description(self):
		return {
			'name': 'Chronostratigraphic Name Index',
			'maintainer': 'Michael Benowitz',
			'maintainer_email': 'michael@epandda.org',
			'description': 'Returns specimens matchting chronostratigraphic terms',
			'params': [
			{
				"name": "chronostratigraphy",
				"label": "Chronostratigraphy Full Text",
				"type": "text",
				"required": False,
				"description": "Search for a single term in the chronostratigraphic hierarchy"
			},{
				"name": "stage",
				"label": "Stage/Age",
				"type": "text",
				"required": False,
				"description": "Search a single or range of age/stages. Can be formatted as 'age1-age2' or 'age1 to age2'"
			},{
				"name": "series",
				"label": "Series/Epoch",
				"type": "text",
				"required": False,
				"description": "Search a single or range of series/epochs. Can be formatted as 'epoch1-epoch2' or 'epoch1 to epoch2'"
			},{
				"name": "system",
				"label": "System/Period",
				"type": "text",
				"required": False,
				"description": "Search a single or range of age/stages. Can be formatted as 'period1-period2' or 'period1 to period2'"
			},{
				"name": "erathem",
				"label": "Erathem/Era",
				"type": "text",
				"required": False,
				"description": "Search a single or range of age/stages. Can be formatted as 'era1-era2' or 'era1 to era2'"
			}]
		}
