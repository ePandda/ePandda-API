from base import baseResource
import json
import pycountry
import hashlib
from bson import Binary, Code, json_util, BSON, ObjectId
from bson.json_util import dumps

class elasticBasedResource(baseResource):
	def __init__(self):
		super(elasticBasedResource, self).__init__()

	def _queryMedia(self, coreid):
		mediaURLs = []
		mediaQuery = {"query": {"term": {"coreid.keyword": coreid}}}
		mediaRes = self.es.search(index="idigbio_media", body=mediaQuery)
		if mediaRes:
			for media in mediaRes['hits']['hits']:
				mediaURLs.append(media['_source']['ac:accessURI'])
		return mediaURLs

	def processSearchTerms(self, params, idigbioReplacements, pbdbReplacements):

		searchTerms = params['terms'].split('|')
		limit = self.limit()

		idbQuery = {
		   "size": limit,
		   "query":{
			   "bool":{
				   "must":[]
			   }
		   },
		   "sort": [
			   "_score",
			   "_doc"
		   ]
		}

		pbdbQuery = {
		   "size": limit,
		   "query":{
			   "bool":{
				   "must":[]
			   }
		   },
		   "sort": [
			   "_score",
			   "_doc"
		   ]
		}

		processed = {'idbQuery': idbQuery, 'pbdbQuery': pbdbQuery }
		for search in searchTerms:
				pbdbAdded = False
				idbAdded = False

				try:
					field, term = search.split(':')
				except ValueError:
					errmsg = "You provided a malformed terms query. Please ensure that this parameter is comprised of one or more field:term pairs separated by pipes (|)"
					return self.respondWithError( errmsg )

				# Translate terms for iDigBio
				if field in idigbioReplacements:
					processed['idbQuery']['query']['bool']['must'].append({"match": {idigbioReplacements[field]: term}})
					idbAdded = True

				# Translate terms for PBDB
				if field in pbdbReplacements:
					if field == 'country':

						ccParts = term.split(' ')
						for i in range(len(ccParts)):
							ccParts[i] = ccParts[i][0].upper() + ccParts[i][1:]

						countryTerm = ' '.join(ccParts)

						try:
							country = pycountry.countries.get(name=countryTerm)
							countryCode = country.alpha_2
							term = countryCode
						except KeyError:
							errmsg = "The country provided in the terms query could not be found. Please check your query and provide a valid country name"
							return self.respondWithError(errmsg)

					processed['pbdbQuery']['query']['bool']['must'].append({"match": {pbdbReplacements[field]: term}})
					pbdbAdded = True

				# Add Translated Terms to ES Query
				if idbAdded is False:
					processed['idbQuery']['query']['bool']['must'].append({"match": {field: term}})
				if pbdbAdded is False:
					processed['pbdbQuery']['query']['bool']['must'].append({"match": {field: term}})

		# Default query if no params were given
		if len(processed['idbQuery']['query']['bool']['must']) == 0:
			processed['idbQuery']['query']['bool']['must'] = {'match_all': {}}
		if len(processed['pbdbQuery']['query']['bool']['must']) == 0:
			processed['pbdbQuery']['query']['bool']['must'] = {'match_all': {}}

		return processed
	# geoPoint v paleoGeoPoint
	def addGeoFilters(self, idbQuery, pbdbQuery, geoParam):
		paramParts = geoParam.split('|')
		coords = paramParts[0].split(';')
		matchRadius = 10
		if len(paramParts) > 1:
			try:
				matchRadius = int(paramParts[1])
			except ValueError:
				pass
		matchDistance = str(matchRadius) + 'km'

		matchField = None
		if len(paramParts) > 2:
			matchField = paramParts[2].lower()

		queries = [(idbQuery, 'idigbio:geoPoint'), (pbdbQuery, 'geoPoint')]
		if matchField == 'paleo' or matchField == 'paleogeopoint':
			idbQuery = None
			queries = [(pbdbQuery, 'paleoGeoPoint'), (idbQuery, None)]

		for query in queries:
			if query[0] is None:
				continue
			if len(coords) == 1:
				geoFilter = {'geo_distance': {'distance': matchDistance, query[1]: coords[0]}}
			elif len(coords) == 2:
				geoFilter = {'geo_bounding_box': {query[1]: {"top_left": coords[0], "bottom_right": coords[1]}}}
			elif len(coords) > 2:
				geoFilter = {'geo_polygon': {query[1]: {"points": coords}}}
			query[0]['query']['bool']['filter'] = geoFilter


		return pbdbQuery, idbQuery

	def getLocalityMatch(self, matchLevel):
		localityMatch = {'idigbio': 'dwc:county', 'pbdb': 'county'}
		if matchLevel:
			localityMatch['pbdb'] = matchLevel
			localityMatch['idigbio'] = 'dwc:' + matchLevel
			if matchLevel == 'state':
				localityMatch['idigbio'] = localityMatch['idigbio'] + 'Province'
			elif matchLevel == 'country':
				localityMatch['pbdb'] = 'cc1'
		return localityMatch

	def getTaxonMatch(self, matchLevel, idigbioReplacements, pbdbReplacements):
		taxonMatch = {'idigbio': 'dwc:genus', 'pbdb': 'genus'}
		if matchLevel:
			if matchLevel in idigbioReplacements:
				taxonMatch['idigbio'] = idigbioReplacements[matchLevel]
			else:
				taxonMatch['idigbio'] = 'dwc:' + matchLevel
			if params['taxonMatchLevel'] in pbdbReplacements:
				taxonMatch['pbdb'] = pbdbReplacements[matchLevel]
			else:
				taxonMatch['pbdb'] = matchLevel

		return taxonMatch

	def getChronoMatch(self, matchLevel):
		chronoMatch = {'idigbio': {'early': 'dwc:earliestPeriodOrLowestSystem', 'late': 'dwc:latestPeriodOrHighestSystem'}}
		chronoMatchLevel = None
		if matchLevel:
			chronoMatchLevel = matchLevel
			if matchLevel == 'age':
				chronoMatch = {'idigbio': {'early': 'dwc:earliestAgeOrLowestStage', 'late': 'dwc:latestAgeOrHighestStage'}}
			elif matchLevel == 'epoch':
				chronoMatch = {'idigbio': {'early': 'dwc:earliestEpochOrLowestSeries', 'late': 'dwc:latestEpochOrHighestSeries'}}
			elif matchLevel == 'era':
				chronoMatch = {'idigbio': {'early': 'dwc:earliestEraOrLowestErathem', 'late': 'dwc:latestEraOrHighestErathem'}}
		else:
			chronoMatchLevel = 'period'
		return chronoMatch, chronoMatchLevel

	def parseMatches(self, params, idbRes, pbdbRes, taxonMatch, localityMatch, chronoMatch, chronoMatchLevel, returnMedia=False, pbdbType='occs'):

		matches = {'results': {}}
		for res in [pbdbRes, idbRes]:
			if res['hits']['total'] == 0:
				continue

			if res['hits']['hits'][0]['_type'] == 'idigbio':
				matches['idigbio_search_after'] = json.dumps(res['hits']['hits'][-1]['sort'])
			else:
				matches['pbdb_search_after'] = json.dumps(res['hits']['hits'][-1]['sort'])

			for hit in res['hits']['hits']:

				recHash = hashlib.sha1()
				data = {'sourceRecords': [], 'links': [], 'matchFields': {}}

				idbMatchList = []
				pbdbMatchList = []
				noMatch = False

				if hit['_type'] == 'idigbio':

					# Store the primary ID
					linkID = hit['_source']['idigbio:uuid']
					linkField= 'idigbio:uuid'

					# iDigBio Locality Matches
					if hit['_source'][localityMatch['idigbio']] is not None:
						idbMatchList.append({"match": {localityMatch['idigbio']: hit['_source'][localityMatch['idigbio']]}})
						data['matchFields'][localityMatch['idigbio']] = hit['_source'][localityMatch['idigbio']]
						recHash.update(data['matchFields'][localityMatch['idigbio']].encode('utf-8'))

						if params['localityMatchLevel']:
							ccParts = hit['_source'][localityMatch['idigbio']].split(' ')
							for i in range(len(ccParts)):
								ccParts[i] = ccParts[i][0].upper() + ccParts[i][1:]

							countryTerm = ' '.join(ccParts)

							try:
								country = pycountry.countries.get(name=countryTerm)
								pbdbMatchList.append({"match": {localityMatch['pbdb']: country.alpha_2}})
							except KeyError:
								noMatch = True
						else:
							pbdbMatchList.append({"match": {localityMatch['pbdb']: hit['_source'][localityMatch['idigbio']]}})

					else:
						noMatch = True

					# iDigBio Taxon Matches
					if hit['_source'][taxonMatch['idigbio']] is not None:
						pbdbMatchList.append({"match": {taxonMatch['pbdb']: hit['_source'][taxonMatch['idigbio']]}})
						idbMatchList.append({"match": {taxonMatch['idigbio']: hit['_source'][taxonMatch['idigbio']]}})
						data['matchFields'][taxonMatch['idigbio']] = hit['_source'][taxonMatch['idigbio']]
						recHash.update(data['matchFields'][taxonMatch['idigbio']])
					else:
						noMatch = True

					# iDigBio Chronostrat Matches
					chrono_early = chrono_late = None
					ma_start_score = ma_end_score = 0
					ma_start = 1000
					ma_end = 0

					if hit['_source'][chronoMatch['idigbio']['early']] and hit['_source'][chronoMatch['idigbio']['late']]:

						chrono_early = hit['_source'][chronoMatch['idigbio']['early']]
						chrono_late = hit['_source'][chronoMatch['idigbio']['late']]

						idbMatchList.append({"match": {chronoMatch['idigbio']['early']: hit['_source'][chronoMatch['idigbio']['early']]}})
						idbMatchList.append({"match": {chronoMatch['idigbio']['late']: hit['_source'][chronoMatch['idigbio']['late']]}})
					elif hit['_source'][chronoMatch['idigbio']['early']]:

						chrono_early = hit['_source'][chronoMatch['idigbio']['early']]
						chrono_late = hit['_source'][chronoMatch['idigbio']['early']]

						idbMatchList.append({"match": {chronoMatch['idigbio']['early']: hit['_source'][chronoMatch['idigbio']['early']]}})
						idbMatchList.append({"match": {chronoMatch['idigbio']['late']: hit['_source'][chronoMatch['idigbio']['early']]}})
					elif hit['_source'][chronoMatch['idigbio']['late']]:

						chrono_early = hit['_source'][chronoMatch['idigbio']['late']]
						chrono_late = hit['_source'][chronoMatch['idigbio']['late']]

						idbMatchList.append({"match": {chronoMatch['idigbio']['early']: hit['_source'][chronoMatch['idigbio']['late']]}})
						idbMatchList.append({"match": {chronoMatch['idigbio']['late']: hit['_source'][chronoMatch['idigbio']['late']]}})
					else:
						noMatch = True

					# Do chronoLookup query if chrono_eary and chrono_late are set
					if chrono_early and chrono_late:

						# Late Query
						ma_start_res = self.es.search(index="chronolookup", body={"query": {"match": {"name": chrono_late}}})
						for ma_start_hit in ma_start_res['hits']['hits']:
							if ma_start_hit['_source']['level'] == chronoMatchLevel:
								if hit['_score'] > ma_start_score:
									ma_start = ma_start_hit['_source']['start_ma']
									ma_start_score = ma_start_hit['_score']

						# Early Query
						ma_end_res = self.es.search(index="chronolookup", body={"query": {"match": {"name": chrono_early}}})
						for ma_end_hit in ma_end_res['hits']['hits']:
							if ma_end_hit['_source']['level'] == chronoMatchLevel:
								if hit['_score'] > ma_end_score:
									ma_end = ma_start_hit['_source']['end_ma']
									ma_end_score = ma_start_hit['_score']

						data['matchFields']['chronostratigraphy'] = {"max_ma": ma_start, "min_ma": ma_end}

						pbdbMatchList.append({"range": {"min_ma": {"lte": ma_start}}})
						pbdbMatchList.append({"range": {"max_ma": {"gte": ma_end}}})

					recHash.update( str(ma_start) + str(ma_end) )
					linkQuery = {
						"size": 10,
						"query":{
							"bool":{
								"must": pbdbMatchList
							}
						}
					}
					queryIndex = 'pbdb'

				elif hit['_type'] == 'pbdb':

					# PBDB Locality Matches
					if hit['_source'][localityMatch['pbdb']] is not None:

						if params['localityMatchLevel'] == 'country':
							try:
								country = pycountry.countries.get(alpha_2=hit['_source'][localityMatch['pbdb']])
								idbMatchList.append({"match": {localityMatch['idigbio']: country.name}})
							except KeyError:
								noMatch = True
						else:
							idbMatchList.append({"match": {localityMatch['idigbio']: hit['_source'][localityMatch['pbdb']]}})

						pbdbMatchList.append({"match": {localityMatch['pbdb']: hit['_source'][localityMatch['pbdb']]}})
						data['matchFields'][localityMatch['pbdb']] = hit['_source'][localityMatch['pbdb']]
						recHash.update(data['matchFields'][localityMatch['pbdb']].encode('utf-8'))
					else:
						noMatch = True

					# PBDB Taxon Matches
					if hit['_source'][taxonMatch['pbdb']] is not None:

						idbMatchList.append({"match": {taxonMatch['idigbio']: hit['_source'][taxonMatch['pbdb']]}})
						pbdbMatchList.append({"match": {taxonMatch['pbdb']: hit['_source'][taxonMatch['pbdb']]}})
						data['matchFields'][taxonMatch['pbdb']] = hit['_source'][taxonMatch['pbdb']]
						recHash.update(data['matchFields'][taxonMatch['pbdb']])
					else:
						noMatch = True


					# PBDB Chrono matching ( uses chronolookup index in ES )
					matchChrono = []
					chronoQuery = {
						"query": {
							"bool": {
								"must": [
									{ "range": { "start_ma": { "gte": hit['_source']['max_ma'] } } },
									{ "range": { "end_ma": { "lte": hit['_source']['min_ma'] } } },
									{ "match": { "level": chronoMatchLevel } }
								]
							}
						}
					}

					chronoRes = self.es.search(index="chronolookup", body=chronoQuery)

					for chrono in chronoRes['hits']['hits']:
						matchChrono.append(chrono['_source']['name'])

					data['matchFields']['chronostratigraphy'] = matchChrono

					if not matchChrono:
						noMatch = True

					pbdbMatchList.append({"range": {"min_ma": {"lte": hit['_source']['max_ma']}}})
					pbdbMatchList.append({"range": {"max_ma": {"gte": hit['_source']['min_ma']}}})

					recHash.update( str(matchChrono) )

					linkID = hit['_source']['occurrence_no']
					linkField = 'occurrence_no'

					linkQuery = {
						"size": 10,
						"query": {
							"bool": {
								"must": idbMatchList,
								"should": [
									{ "terms": { chronoMatch['idigbio']['early']: matchChrono}},
									{ "terms": { chronoMatch['idigbio']['late']: matchChrono}}
								]
							}
						}
					}
					queryIndex = 'idigbio'

				#matches['search_after'] = json.dumps(hit["sort"])
				hashRes = recHash.hexdigest()

				if hashRes in matches['results']:
					sourceRow = self.resolveReference(hit["_source"], hit["_id"], hit["_type"], pbdbType)
					matches['results'][hashRes]['sources'].append(sourceRow)
				else:

					# Resolve Reference, hardset the pbdb_type to refs
					sourceRow = self.resolveReference(hit["_source"], hit["_id"], hit["_type"], pbdbType)
					matches['results'][hashRes] = {'fields': data['matchFields'], 'totalMatches': 0, 'matches': None, 'sources': [sourceRow], 'fullMatchQuery': None}
					if noMatch is False:
						linkResult = self.es.search(index=queryIndex, body=linkQuery)
						totalMatches = linkResult['hits']['total']
						if totalMatches > 0:
							matches['results'][hashRes]['matches'] = []
							for link in linkResult['hits']['hits']:
								row = self.resolveReference(link['_source'], link['_id'], link['_type'], pbdbType)
								row['score'] = link['_score']
								row['type'] = link['_type']
								matches['results'][hashRes]['matches'].append(row)
								data['links'].append([link['_id'], link['_score'], link['_type']])

							matches['results'][hashRes]['totalMatches'] = totalMatches
							linkQuery['size'] = totalMatches
							matches['results'][hashRes]['fullMatchQuery'] = json.dumps(linkQuery)

						matches['results'][hashRes]['fields'] = data['matchFields']

					if queryIndex == 'idigbio':
						sourceQuery = {
							"query":{
								"bool":{
									"must": pbdbMatchList
								}
							}
						}

					elif queryIndex == 'pbdb':
						sourceQuery = {
							"query":{
								"bool":{
									"must": idbMatchList
								}
							}
						}

					matches['results'][hashRes]['fullSourceQuery'] = json.dumps(sourceQuery)

				if hit["_type"] == 'idigbio':
					sourceType = 'idigbio'
					matchType = 'pbdb'
				else:
					sourceType = 'pbdb'
					matchType = 'idigbio'

				matches['results'][hashRes]['sourceType'] = sourceType
				matches['results'][hashRes]['matchType'] = matchType
		matches['queryInfo'] = {'idigbioTotal': idbRes['hits']['total'], 'pbdbTotal': pbdbRes['hits']['total'], 'matchCriteria': {'chronostratigraphyMatch': chronoMatch, 'taxonomyMatch': taxonMatch, 'localityMatch': localityMatch}}
		return matches
