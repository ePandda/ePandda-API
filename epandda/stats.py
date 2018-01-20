from flask_restful import Resource, Api
from base import baseResource
#
# Emit API stats
#
class stats(baseResource):
    def process(self):

        localityFields = {'localities': 'originalLocality', 'counties': 'county', 'stateProvinces': 'stateProvinceName', 'countries': 'countryName'}
        # Get any supplied parameters
        # There are no required parameters at this time
        params = self.getParams()
        if self.paramCount > 0:
            criteria = {'endpoint': 'stats', 'parameters': []}
            response = {}
            if params['totalRecords']:
                totalQuery = {
                    "size": 0,
                    "query": {
                        "match_all": {}
                    }
                }
                idigbioRes = self.es.search(index="idigbio", body=totalQuery)
                pbdbRes = self.es.search(index="pbdb", body=totalQuery)
                idbCount = idigbioRes['hits']['total']
                pbdbCount = pbdbRes['hits']['total']
                totalCount = pbdbCount + idbCount
                criteria['parameters'].append('totalRecords')
                response['totalRecords'] = totalCount
                response['specimens'] = idbCount
                response['occurrences'] = pbdbCount

            #localityIndex = endpoints.localityIndex
            #for place in ['countries', 'stateProvinces', 'counties', 'localities']:
            #	if params[place]:
            #		placeTerm = localityFields[place]
            #		#placeCount = len(list(localityIndex.aggregate([{'$group': {'_id': {'localities': '$' + placeTerm}}}])))
            #    	placeCount = localityIndex.count()
            #    	criteria['parameters'].append(place)
            #    	response[place] = placeCount

            #if params['geoPoints']:
            #    geoPointIndex = endpoints.geoPointIndex
            #    geoCount = geoPointIndex.find().count()
            #    criteria['parameters'].append('geoPoints')
            #    response['geoPoints'] = geoCount

            #if params['taxonomies']:
            #    taxonIndex = endpoints.taxonIndex
            #    taxonCount = taxonIndex.find().count()
            #    criteria['parameters'].append('taxonomies')
            #    response['taxonomies'] = taxonCount
        else:
          return self.respondWithDescription()
        # Indexes for querying stats from
        # TODO: Cache these results
        return self.respond({'results': response, 'criteria': criteria})

    def description(self):
        return {
            'name': 'API statistics',
            'maintainer': 'Michael Benowitz',
            'maintainer_email': 'michael@epandda.org',
            'description': 'Interesting API statistics',
            'params': [
            {
                "name": "stateProvinces",
                "label": "States/Provinces",
                "type": "boolean",
                "required": False,
                "description": "The number of unique states represented in the collections"
            },
            {
                "name": "countries",
                "label": "Countries",
                "type": "boolean",
                "required": False,
                "description": "The number of unique countries represented in the collections"
            },
            {
                "name": "counties",
                "label": "Counties",
                "type": "boolean",
                "required": False,
                "description": "The number of unique counties represented in the collections"
            },
            {
                "name": "localities",
                "label": "Localities",
                "type": "boolean",
                "required": False,
                "description": "The number of unique localities represented in the collections"
            },
            {
                "name": "geoPoints",
                "label": "Geographic Coordinates",
                "type": "boolean",
                "required": False,
                "description": "The number of unique geographic coordinates represented in the collections"
            },
            {
                "name": "totalRecords",
                "label": "Total Records",
                "type": "boolean",
                "required": False,
                "description": "The count of all specimen/occurrence records included in project from iDigBio and PBDB"
            },
            {
                "name": "taxonomies",
                "label": "Taxonomies",
                "type": "boolean",
                "required": False,
                "description": "The number of unique taxonomic hierarchies represented in the collections"
            }
            ]
        }
