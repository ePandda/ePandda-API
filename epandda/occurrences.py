from mongo import mongoBasedResource
from flask_restful import reqparse
import gridfs
import json

parser = reqparse.RequestParser()

# Add Arguments (params) to parser here ...
parser.add_argument('scientific_name', type=str, help='Taxonomic name to search occurrences for')
parser.add_argument('locality', type=str, help='Locality name to filter taxonomic occurrences by')
parser.add_argument('period', type=str, help='The geologic time period to filter taxonomic occurrences by')
parser.add_argument('institution_code', type=str, help='The abbreviated code submitted by data provider to filter taxonomic occurrences by')

#
#
#
class occurrences(mongoBasedResource):
	def process(self):

		lindex = self.client.endpoints.localityIndex                       # Mongodb index for localities
		tindex = self.client.endpoints.taxonIndex						     # Mongodb index for taxa
		cindex = self.client.endpoints.chronoStratIndex					 # Mongodb index for chronostratigraphy
		ltindex = self.client.endpoints.lithoStratIndex
		grid = gridfs.GridFS(self.client.endpoints)

		# returns dictionary of params as defined in endpoint description
		# will throw exception if required param is not present
		params = self.getParams()
		# offset and limit returned as ints with default if not set
		offset = self.offset()
		limit = self.limit()

		if self.paramCount > 0:
			chronoRes = None
			localityRes = None
			res = None
			lithoRes = None
			criteria = {'endpoint': 'occurrences', 'parameters': {}, 'matchTerms': {'scientificNames': [], 'stateProvinceNames': [], 'countryNames': [], 'countyNames': [], 'localityNames': [], 'originalStates': [], 'originalCountries': [], 'originalCounties': [], 'originalLocalities': [], 'chronostratigraphy': [], 'lithostratigraphy': []}}
			taxonQuery = []
			localityQuery = []
			lithQuery = []
			stratQuery = []
			if params['taxon_name']:
				taxon_name = params['taxon_name']
				res = tindex.find({"$text": {"$search": '"' + taxon_name + '"'}})

			if params['locality']:
				locality = params['locality']
				localityRes = lindex.find({'$text': {'$search': '"' + locality + '"'}})

			if params['chronostratigraphy']:
				chronoStrat = params['chronostratigraphy']
				chronoRes = cindex.find({'$text': {'$search': '"' + chronoStrat + '"'}})

			if params['lithostratigraphy']:
				lithoStrat = params['lithostratigraphy']
				lithoRes = ltindex.find({'$text': {'$search': '"' + lithoStrat + '"'}})

			d = []
			matches = {'idigbio': [], 'pbdb': []}
			taxonMatches = {'idigbio': [], 'pbdb': []}
			chronoMatches = {'idigbio': [], 'pbdb': []}
			lithoMatches = {'idigbio': [], 'pbdb': []}
			idbCount = 0
			pbdbCount = 0
			# taxonomy
			if res:
				for i in res:
					for i in res:
						taxonomy = i['taxonomy']
						scientificNames = i['scientificNames']
						for sciName in scientificNames:
							if sciName not in criteria['matchTerms']['scientificNames']:
								criteria['matchTerms']['scientificNames'].append(sciName)
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
								taxonMatches['pbdb'] = taxonMatches['pbdb'] + pbdb_matches

						if 'idbGridFile' in i:
							if type(i['idbGridFile']) is list:
								idbGrids = i['idbGridFile']
								for file in idbGrids:
									idb_doc = grid.get(file)
									idb_matches = json.loads(idb_doc.read())
									taxonMatches['idigbio'] = taxonMatches['idigbio'] + idb_matches
							else:
								idb_doc = grid.get(i['idbGridFile'])
								idb_matches = json.loads(idb_doc.read())
								taxonMatches['idigbio'] = taxonMatches['idigbio'] + idb_matches

			# locality
			geoMatches = {'idigbio': [], 'pbdb': []}
			if localityRes:
				for i in localityRes:
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
								criteria['matchTerms']['originalStates'].append(origState)
					if 'originalCountryName' in i:
						for origCountry in i['originalCountryName']:
							if origCountry not in criteria['matchTerms']['originalCountries']:
								criteria['matchTerms']['originalCountries'].append(origCountry)
					if 'original_country' in i:
						for origCounty in i['original_county']:
							if origCounty not in criteria['matchTerms']['originalCounties']:
								criteria['matchTerms']['originalCounties'].append(origCounty)
					if 'original_locality' in i:
						for origLocality in i['original_locality']:
							if origCounty not in criteria['matchTerms']['originalLocalities']:
								criteria['matchTerms']['originalLocalities'].append(origLocality)
					if 'pbdbGridFile' in i:
						if type(i['pbdbGridFile']) is list:
							pbdbGrids = i['pbdbGridFile']
							for file in pbdbGrids:
								pbdb_doc = grid.get(file)
								pbdb_matches = json.loads(pbdb_doc.read())
								geoMatches['pbdb'] = geoMatches['pbdb'] + pbdb_matches
						else:
							idb_doc = grid.get(i['pbdbGridFile'])
							idb_matches = json.loads(idb_doc.read())
							geoMatches['pbdb'] = geoMatches['pbdb'] + idb_matches

					if 'idbGridFile' in i:
						if type(i['idbGridFile']) is list:
							idbGrids = i['idbGridFile']
							for file in idbGrids:
								idb_doc = grid.get(file)
								idb_matches = json.loads(idb_doc.read())
								geoMatches['idigbio'] = geoMatches['idigbio'] + idb_matches
						else:
							idb_doc = grid.get(i['idbGridFile'])
							idb_matches = json.loads(idb_doc.read())
							geoMatches['idigbio'] = geoMatches['idigbio'] + idb_matches

			# chronostratigraphy
			if chronoRes:
				for i in chronoRes:
					temp_doc = {}
					for level in ['lowStage', 'highStage', 'lowSeries', 'highSeries', 'lowSystem', 'highSystem', 'lowErathem', 'highErathem', 'upperChronostratigraphy', 'lowerChronostratigraphy']:
						if level in i:
							temp_doc[level] = i[level]
					criteria['matchTerms']['chronostratigraphy'].append(temp_doc)

					if 'pbdbGridFile' in i:
						pbdbGrids = i['pbdbGridFile']
						for file in pbdbGrids:
							pbdb_doc = grid.get(file)
							pbdb_matches = json.loads(pbdb_doc.read())
							chronoMatches['pbdb'] = chronoMatches['pbdb'] + pbdb_matches

					if 'idbGridFile' in i:
						if type(i['idbGridFile']) is list:
							idbGrids = i['idbGridFile']
							for file in idbGrids:
								idb_doc = grid.get(file)
								idb_matches = json.loads(idb_doc.read())
								chronoMatches['idigbio'] = chronoMatches['idigbio'] + idb_matches
						else:
							idb_doc = grid.get(i['idbGridFile'])
							idb_matches = json.loads(idb_doc.read())
							chronoMatches['idigbio'] = chronoMatches['idigbio'] + idb_matches

			# lithostratigraphy
			if lithoRes:
				for i in lithoRes:
					temp_doc = {}

					criteria['matchTerms']['lithostratigraphy'].append({'name': i['name'], 'rank': i['rank']})

					if 'pbdb_matches' in i:
						pbdb_matches = i['pbdb_matches']
						lithoMatches['pbdb'] = lithoMatches['pbdb'] + pbdb_matches

					if 'idb_matches' in i:
						idb_matches = i['idb_matches']
						lithoMatches['idigbio'] = lithoMatches['idigbio'] + idb_matches

			print 'Locality Counts: ' + str(len(geoMatches['idigbio'])) + ' | ' + str(len(geoMatches['pbdb']))
			print 'Taxon Counts: ' + str(len(taxonMatches['idigbio'])) + ' | ' + str(len(taxonMatches['pbdb']))
			print 'Chrono Counts: ' + str(len(chronoMatches['idigbio'])) + ' | ' + str(len(chronoMatches['pbdb']))
			print 'Litho Counts: ' + str(len(lithoMatches['idigbio'])) + ' | ' + str(len(lithoMatches['pbdb']))

			idbGeoSet = set(geoMatches['idigbio'])
			pbdbGeoSet = set(geoMatches['pbdb'])
			idbTaxonSet = set(taxonMatches['idigbio'])
			pbdbTaxonSet = set(taxonMatches['pbdb'])
			idbChronoSet = set(chronoMatches['idigbio'])
			pbdbChronoSet = set(chronoMatches['pbdb'])
			idbLithoSet = set(lithoMatches['idigbio'])
			pbdbLithoSet = set(lithoMatches['pbdb'])

			idbMatches = idbGeoSet | idbTaxonSet | idbChronoSet | idbLithoSet

			for idbMatchSet in  [idbGeoSet, idbTaxonSet, idbChronoSet, idbLithoSet]:
				if len(idbMatchSet) < 1:
					continue
				idbMatches = idbMatches & idbMatchSet

			pbdbMatches = pbdbGeoSet | pbdbTaxonSet | pbdbChronoSet | pbdbLithoSet
			for pbdbMatchSet in  [pbdbGeoSet, pbdbTaxonSet, pbdbChronoSet, pbdbLithoSet]:
				if len(pbdbMatchSet) < 1:
					continue
				pbdbMatches = pbdbMatches & pbdbMatchSet

			matches['idigbio'] = list(idbMatches)
			matches['pbdb'] = list(pbdbMatches)

			idbCount = len(matches['idigbio'])
			pbdbCount = len(matches['pbdb'])

			item = {'matches': {'idigbio': matches['idigbio'], 'pbdb': matches['pbdb']}}
			d.append(item)
			d = self.resolveReferences(d)
			counts = {'totalCount': idbCount + pbdbCount, 'idbCount': idbCount, 'pbdbCount': pbdbCount}
			d['pbdb_resolved'] = d['pbdb_resolved'][offset:limit]
			return self.respond({'counts': counts, 'results': d, 'criteria': criteria})
		else:
			return self.respondWithDescription()

	def description(self):
		return {
			'name': 'Occurrence index',
			'maintainer': 'Seth Kaufman',
			'maintainer_email': 'seth@epandda.org',
			'description': 'Returns specimens collected from a given locality',
			'params': [
				{
					"name": "taxon_name",
					"label": "Taxonomy",
					"type": "text",
					"required": False,
					"description": "The taxa to search occurrences for"
				},
				{
					"name": "locality",
					"label": "Locality",
					"type": "text",
					"required": False,
					"description": "The locality name to bound taxonomic occurences to",
				},
				{
					"name": "chronostratigraphy",
					"label": "Chronostratigraphy",
					"type": "text",
					"required": False,
					"description": "The geologic time period to filter taxon occurrences by"
				},
				{
					"name": "lithostratigraphy",
					"label": "Lithostratigraphy",
					"type": "text",
					"required": False,
					"description": "The lithostratigraphic unit to filter taxon occurrences by"
				}
			]}
