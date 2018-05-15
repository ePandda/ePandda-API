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
            errors = {"GENERAL": []}
            # Elastic Search Pagination
            if params['idigbioSearchAfter']:
                idbQuery['search_after'] = json.loads(params['idigbioSearchAfter'].strip('"'))
            if params['pbdbSearchAfter']:
                pbdbQuery['search_after'] = json.loads(params['pbdbSearchAfter'].strip('"'))

            # Parse the search term parameter and get the initial result from ES
            if not params['terms']:
                errors["terms"] = ["You must provide at least one field:term pair"]
                return self.respondWithError(errors)

            # Build queries based on passed in params
            processed = self.processSearchTerms(params, idigbioReplacements, pbdbReplacements)

            idbQuery = processed['idbQuery']
            pbdbQuery = processed['pbdbQuery']

            # If any of the PBDB-specific author fields are added, append these to the query
            if params['title'] and pbdbQuery:
                pbdbQuery['query']['bool']['must'].append({'match': {'occ_refs-reftitle': params['title']}})

            if params['journal'] and pbdbQuery:
                pbdbQuery['query']['bool']['must'].append({'match': {'occ_refs-pubtitle': params['journal']}})

            # ref_author, ref_author1last, ref_author2last, ref_author1init, ref_author2init and ref_formatted
            if params['authors'] and pbdbQuery:
                authorTerm = params['authors']
                authorsQuery = {
                    'bool': {
                        'should': [
                            {'match': {'ref_author': authorTerm}},
                            {'match': {'occ_ref-otherauthors': authorTerm}},
                            {'match': {'occ_refs-author1last': authorTerm}},
                            {'match': {'occ_refs-author2last': authorTerm}},
                            {'match': {'occ_refs-author1init': authorTerm}},
                            {'match': {'occ_refs-author2init': authorTerm}},
                            {'match': {'occ_refs-formatted': authorTerm}}
                        ]
                    }
                }
                pbdbQuery['query']['bool']['must'].append(authorsQuery)

            if params['title'] or params['authors'] or params['journal']:
                idbQuery = None
            # Query ES Indexes
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
                    print e
                    errors['GENERAL'].append("PBDB search failed")
            else:
                pbdbRes = {'hits':{'total': 0}}

            # Parse locality Match
            localityMatch = self.getLocalityMatch(params['localityMatchLevel'])

			# Parse Taxon Match
            taxonMatch = self.getTaxonMatch(params['taxonMatchLevel'], idigbioReplacements, pbdbReplacements)

			# Parse Chrono Match
			# PBDB uses numeric min/max ma so we don't have to worry about that here
            chronoMatch, chronoMatchLevel = self.getChronoMatch(params['chronoMatchLevel'])

            if len(errors["GENERAL"]) > 0:
				return self.respondWithError(errors)

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
            'description': 'Searches PBDB and iDigBio for publication <=> specimen matches and returns match groups based on taxonomy, bibliography and locality',
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
                    "name": "title",
                    "label": "Article Title",
                    "type": "text",
                    "required": False,
                    "description": "The full or partial title of a journal article, drawn from the ref_title field in PBDB",
                    "display": True
                },
                {
                    "name": "journal",
                    "label": "Journal",
                    "type": "text",
                    "required": False,
                    "description": "The full or partial title of the journal, drawn from the pub_title field in PBDB",
                    "display": True
                },
                {
                    "name": "authors",
                    "label": "Author Names",
                    "type": "text",
                    "required": False,
                    "description": "The last names, or last names + first initials of article authors. Drawn from the ref_author, ref_otherauthors, ref_author1last, ref_author2last, ref_author1init, ref_author2init and ref_formatted fields from PBDB",
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
