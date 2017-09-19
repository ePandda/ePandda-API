from mongo import mongoBasedResource
from flask_restful import reqparse
import gridfs
import json
from bson import ObjectId
import requests
from requests.exceptions import ConnectionError
import urllib


parser = reqparse.RequestParser()
parser.add_argument('taxnomy', type=str, help='Taxon hierarchy to search on')

#
#
#
class taxonomy(mongoBasedResource):
	def process(self):
		# Mongodb index for localities
		tindex = self.client.endpoints.taxonIndex
		mindex = self.client.endpoints.mediaIndex3
		# Mongodb gridFS instance
		grid = gridfs.GridFS(self.client.endpoints)                   

		# returns dictionary of params as defined in endpoint description
		# will throw exception if required param is not present
		params = self.getParams()
		# offset and limit returned as ints with default if not set
		offset = self.offset()
		limit = self.limit()

		if self.paramCount > 0:
			imageRes = None
			res = None
			criteria = {'endpoint': 'taxonomy', 'parameters': {}, 'matchTerms': {'scientificNames': []}}
			taxonQuery = []
			for p in [{'scientificName': ['scientificNames', 'originalScientificName']}, {'species': ['species', 'taxonomy.species']}, {'genus': ['genus', 'taxonomy.genus']}, {'family': ['family', 'taxonomy.family']}, {'order': ['order', 'taxonomy.order']}, {'class': ['class', 'taxonomy.class']}, {'phylum': ['phylum', 'taxonomy.phylum']}, {'kingdom': ['kingdom', 'taxonomy.kingdom']}, {'other': ['taxonomy.noRank']}]:
				val = p.keys()[0]
				if (params[val]):
					criteria['parameters'][val] = params[val]
					taxonSubQuery = {'$or': []}
					for field in p[val]:
						taxonSubQuery['$or'].append({field: params[val]})
					taxonQuery.append(taxonSubQuery)
			
			if(params['fullTaxonomy']):
				criteria['parameters']['fullTaxonomy'] = params['fullTaxonomy']
				taxonQuery.append({'$text': {'$search': '"' + params['fullTaxonomy'] + '"', '$caseSensitive': False}})
			
			
			if (len(taxonQuery) == 0):
				return self.respondWithError({"GENERAL": "No valid parameters specified"})
			if len(taxonQuery) > 0:
				res = tindex.find({'$and': taxonQuery})
			
			d = []
			matches = {'idigbio': [], 'pbdb': []}
			idbCount = 0
			pbdbCount = 0
			if res:
				for i in res:
					if 'scientificNames' in i:
						scientificNames = i['scientificNames']
						for sciName in scientificNames:
							if sciName not in criteria['matchTerms']['scientificNames']:
								criteria['matchTerms']['scientificNames'].append(sciName)
					if 'taxonomy' in i:
						taxonomy = i['taxonomy']
						taxon_ranks = taxonomy.keys()
						for rank in taxon_ranks:
							if rank in criteria['matchTerms']:
								for term in taxonomy[rank]:
									if term not in criteria['matchTerms'][rank]:
										criteria['matchTerms'][rank].append(term)
							else:
								criteria['matchTerms'][rank] = []
								for term in taxonomy[rank]:
									criteria['matchTerms'][rank].append(term)

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
			
			
			imageQuery = []
			media = []
			if(params['images'] == 'true'):
				imgRes = mindex.find({'idigbio_uuids': {'$in': matches['idigbio']}})
				for res in imgRes:
					imgSpecimens = res['mediaURIs']
					for specimen in imgSpecimens:
						url = 'https://www.idigbio.org/portal/records/' + specimen
						links = imgSpecimens[specimen]
						for link in links:	
							if [url, link] in media:
								continue
							media.append([url, link])
			
			idbCount = len(matches['idigbio'])
			pbdbCount = len(matches['pbdb'])

			item = {'matches': {'idigbio': matches['idigbio'], 'pbdb': matches['pbdb']}}
			d.append(item)
			d = self.resolveReferences(d)
			counts = {'totalCount': idbCount + pbdbCount, 'idbCount': idbCount, 'pbdbCount': pbdbCount}

			return self.respond({'counts': counts, 'results': d, 'criteria': criteria, 'media': media})

		else:
			return self.respondWithDescription()

	def description(self):
		return {
			'name': 'Taxonomic name index',
			'maintainer': 'Michael Benowitz',
			'maintainer_email': 'michael@epandda.org',
			'description': 'Returns specimens matchting taxonomic terms',
			'params': [
			{
				"name": "fullTaxonomy",
				"label": "Taxonomy Full Text",
				"type": "text",
				"required": False,
				"description": "A search across all levels of of the taxonomic hierarchy"
			},
			{
				"name": "genus",
				"label": "Genus",
				"type": "text",
				"required": False,
				"description": "Genus"
			},
			{
				"name": "species",
				"label": "Species",
				"type": "text",
				"required": False,
				"description": "The species (specific epithet) of the specimen"
			},
			{
				"name": "scientificName",
				"label": "Scientific Name",
				"type": "text",
				"required": False,
				"description": "A formatted scientific name, including or not including scientific name authorship and year"
			},
			{
				"name": "family",
				"label": "Family",
				"type": "text",
				"required": False,
				"description": "Taxonomic family"
			},
			{
				"name": "order",
				"label": "Order",
				"type": "text",
				"required": False,
				"description": "Taxonomic order"
			},
			{
				"name": "class",
				"label": "Class",
				"type": "text",
				"required": False,
				"description": 'Taxonomic class'
			},
			{
				"name": "phylum",
				"label": "Phylum",
				"type": "text",
				"required": False,
				"description": "Taxonomic phylum"
			},
			{
				"name": "kingdom",
				"label": "Kingdom",
				"type": "text",
				"required": False,
				"description": "Taxonomic kingdom"
			},
			{
				"name": "other",
				"label": "Other",
				"type": "text",
				"required": False,
				"description": "Other terms such as superFamily, 'above family' as well as a generic 'noRank' field"
			},
			{
				"name": "images",
				"label": "Taxon Images",
				"type": "boolean",
				"required": False,
				"description": "Set to true to return images any images from iDigBio that match the search terms"
			}
		]}
