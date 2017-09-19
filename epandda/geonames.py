from mongo import mongoBasedResource
from flask_restful import reqparse
import gridfs
import json
from bson import ObjectId
from bson import Decimal128


parser = reqparse.RequestParser()
parser.add_argument('locality', type=str, help='Locality name to to search geoname records by')

#
#
#
class geonames(mongoBasedResource):
	def process(self):
		# Mongodb index for localities
		lindex = self.client.endpoints.localityIndex

		# Mongodb index for geoPoints
		pindex = self.client.endpoints.geoPointIndex

		# Mongodb gridFS instance
		grid = gridfs.GridFS(self.client.endpoints)

		# returns dictionary of params as defined in endpoint description
		# will throw exception if required param is not present
		params = self.getParams()


		# offset and limit returned as ints with default if not set
		offset = self.offset()
		limit = self.limit()

		if limit < 1:
			limit = 100

		if self.paramCount > 0:
			criteria = {'endpoint': 'geoname', 'parameters': {}, 'matchPoints': [], 'matchTerms': {'stateProvinceNames': [], 'countryNames': [], 'countyNames': [], 'localityNames': [], 'originalStates': [], 'originalCountries': [], 'originalCounties': [], 'originalLocalities': []}}
			geoQuery = []

			for p in [{'countryName': ['countryName', 'originalCountry'], {'countryCode': ['countryCode'], {'locality': ['originalLocality']}, {'stateProvinceName': ['stateProvinceName', 'originalStateProvince']}, {'stateProvinceCode': ['stateProvinceCode']}, {'county': ['county', 'originalCounty']}]:
				val = p.keys()[0]
				if (params[val]):
					criteria['parameters'][val] = params[val]
					geoSubQuery = {'$or': []}
                    for field in p[val]:
                        geoSubQuery['$or'].append({field: params[val]})
                    geoQuery.append(geoSubQuery)

			if(params['geolocation']):
				criteria['parameters']['geolocation'] = params['geolocation']
				geoQuery.append({'$text': {'$search': '"' + params['geolocation'] + '"', '$caseSensitive': False}})

			latLngQuery = []
			if params['geoPoint']:
				criteria['parameters']['geoPoint'] = params['geoPoint']
				point = params['geoPoint'].split(",")

				try:
					lat = float(point[0])
					lon = float(point[1])
				except Exception as e:
					return self.respondWithError({"GENERAL": "Invalid coordinates"})

				if lon < -180 or lon > 180:
					return self.respondWithError({"GENERAL": "Longitude is invalid"})

				if lat < -90 or lat > 90:
					return self.respondWithError({"GENERAL": "Latitude is invalid"})

				point = [lon, lat]
				if params['geoRadius']:
					criteria['parameters']['geoRadius'] = params['geoRadius']
					radius = int(params['geoRadius'])
				else:
					# Set a default distance from the provided point, 10,000m
					radius = 10000
				latLngQuery = {'coordinates': {'$near': {'$geometry': {'type': "Point", 'coordinates': point}, '$maxDistance': radius}}}
			if (len(geoQuery) == 0 and 'geoPoint' not in params):
				return self.respondWithError({"GENERAL": "No parameters specified"})

			if len(geoQuery) > 0:
				res = lindex.find({'$and': geoQuery})
			else:
				res = None
			if params['geoPoint']:
				geoRes = pindex.find(latLngQuery)
			else:
				geoRes = None
			d = []
			geoMatches = {'idigbio': [], 'pbdb': []}
			matches = {'idigbio': [], 'pbdb': []}
			idbCount = 0
			pbdbCount = 0
			if res:
				for i in res:
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
								matches['pbdb'] = matches['pbdb'] + pbdb_matches
						else:
							idb_doc = grid.get(i['idbGridFile'])
							idb_matches = json.loads(idb_doc.read())
							matches['idigbio'] = matches['idigbio'] + idb_matches

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

			if geoRes:
				for r in geoRes:
					lng_lat = r['coordinates']['coordinates']
					dec_lng_lat = [str(lng_lat[0]), str(lng_lat[1])]
					criteria['matchPoints'].append(dec_lng_lat)
					if 'idb_data' in r:
						for spec in r['idb_data'][0]:
							geoMatches['idigbio'].append(spec)
					if 'pbdb_data' in r:
						for spec in r['pbdb_data'][0]:
							geoMatches['pbdb'].append(spec)

			finalMatches = {'idigbio': [], 'pbdb': []}
			if len(geoMatches['idigbio']) > 0 and len(matches['idigbio']) > 0:
				finalMatches['idigbio'] = set(matches['idigbio']) & set(geoMatches['idigbio'])
			else:
				finalMatches['idigbio'] = geoMatches['idigbio'] + matches['idigbio']
			idbCount = len(finalMatches['idigbio'])
			if len(geoMatches['pbdb']) > 0 and len(matches['pbdb']) > 0:
				finalMatches['pbdb'] = set(matches['pbdb']) & set(geoMatches['pbdb'])
			else:
				finalMatches['pbdb'] = geoMatches['pbdb'] + matches['pbdb']
			pbdbCount = len(finalMatches['pbdb'])
			resolveSet = {'idigbio': finalMatches['idigbio'][offset:limit], 'pbdb':  finalMatches['pbdb'][offset:limit]}
			item = {'matches': finalMatches}
			d.append(item)
			d = self.resolveReferences(d)

			counts = {'totalCount': idbCount + pbdbCount, 'idbCount': idbCount, 'pbdbCount': pbdbCount}


			return self.respond({'counts': counts, 'results': d, 'criteria': criteria})
		else:
			return self.respondWithDescription()

	def description(self):
		return {
			'name': 'Geographic name/locality index',
			'maintainer': 'Michael Benowitz',
			'maintainer_email': 'michael@epandda.org',
			'description': 'Returns specimens collected from a given locality',
			'params': [
				{
					"name": "geolocation",
					"label": "Full Text Geographic Search",
					"type": "text",
					"required": False,
					"description": "A generic place name, will search across locality, county, state/province and country fields"
				},
				{
					"name": "countryName",
					"label": "Country Name",
					"type": "text",
					"required": False,
					"description": "Full country names"
				},
				{
					"name": "countryCode",
					"label": "Country Code (2-Letter)",
					"type": "text",
					"required": False,
					"description": "2-letter country code, provided from geonames.org"
				},
				{
					"name": "stateProvinceName",
					"label": "State/Province Name",
					"type": "text",
					"required": False,
					"description": "State or province name. Do not include qualifiers (e.g. State, Province) to return better results"
				},
				{
					"name": "stateProvinceCode",
					"label": "State Province Code (2-Letter or 2-Digit)",
					"type": "text",
					"required": False,
					"description": "State or province name. Do not include qualifiers (e.g. State, Province) to return better results"
				},
				{
					"name": "county",
					"label": "County Name",
					"type": "text",
					"required": False,
					"description": 'County name. Do not add "County" to your search string'
				},
				{
					"name": "locality",
					"label": "Locality Name",
					"type": "text",
					"required": False,
					"description": "The specific locality where a specimen was recorded"
				},
				{
					"name": "geoPoint",
					"label": "Geographic Point (Latitude, Longitude)",
					"type": "text",
					"required": False,
					"description": "The Latitude, Longitude to search on. Provide a comma separated string with no space (e.g. '-103.2,29.2')."
				},
				{
					"name": "geoRadius",
					"label": "Distance from Geographic Point in Meters",
					"type": "text",
					"required": False,
					"description": "The distance from the provided geoPoint to search within, in meters. Provide only the distance with no units"
				}
			]}
