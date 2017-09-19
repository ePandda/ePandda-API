from mongo import mongoBasedResource
from flask_restful import reqparse
import gridfs
import json
from elasticsearch import Elasticsearch

parser = reqparse.RequestParser()

parser.add_argument('unit', type=str, help='stratigraphic unit to search for')
parser.add_argument('unit_type', type=str, help='type of stratigraphic unit to limit search to')

#
#
#
class lithostratigraphy(mongoBasedResource):
    def process(self):
        # Mongodb index for localities
        lindex = self.client.endpoints.lithoStratIndex
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
            res = None
            unit_score = 0
            unit_names = []
            criteria = {'endpoint': 'lithostratigraphy', 'parameters': {}, 'matchTerms': []}
            lithoQuery = []
            #if(params['lithostratigraphy']):
            #    criteria['parameters']['lithostratigraphy'] = params['lithostratigraphy']
            #    lithoQuery.append({'$text': {'$search': '"' + params['lithostratigraphy'] + '"'}})
            #else:
            #    return self.respondWithError({"GENERAL": "Need to specific a lithostratigraphic term"})
			
            #if(params['rank']):
            #    lithoQuery.append({'rank': params['rank']})
            if(params['lithostratigraphy']):
                criteria['parameters']['lithostratigraphy'] = params['lithostratigraphy']
                if params['rank']:
                    criteria['parameters']['rank'] = params['rank']
                    litho_units = es.search(index="litholookup", body={"query": {"bool": {"must": [{"match": {"name": {"query": params["lithostratigraphy"], "fuzziness": "AUTO"}}}, {"term": {"level": params["rank"]}}]}}})
                else:
                    litho_units = es.search(index="litholookup", body={"query": {"match": {"name": {"query": params["lithostratigraphy"], "fuzziness": "AUTO"}}}})
            else:
                return self.respondWithError({"GENERAL": "Need to specific a lithostratigraphic term"})
            for unit in litho_units['hits']['hits']:
                if unit['_score'] > unit_score:
                    unit_names.append(unit["_source"]["name"])
                    unit_rank = unit['_source']['level']
                    unit_score = unit['_score']
                    if 'child_formations' in unit['_source']:
                        for child_formation in unit['_source']['child_formations']:
                            unit_names.append(child_formation)
                    if 'child_members' in unit['_source']:
                        for child_member in unit['_source']['child_members']:
                            unit_names.append(child_member)
            lithoQuery.append({"name": {'$in': unit_names}})
            if len(lithoQuery) > 0:
                res = lindex.find({'$and': lithoQuery})
            else:
                return self.respondWithError({"GENERAL": "No valid parameters specified"})

            d = []
            matches = {'idigbio': [], 'pbdb': []}
            idbCount = 0
            pbdbCount = 0
            if res:
                for i in res:
                    criteria['matchTerms'].append({'unit': i['name'], 'type': i['rank']})

                    if 'pbdb_matches' in i:
                        matches['pbdb'] = matches['pbdb'] + i['pbdb_matches']

                    if 'idb_matches' in i:
                        matches['idigbio'] = matches['idigbio'] + i['idb_matches']
            idbCount = len(matches['idigbio'])
            pbdbCount = len(matches['pbdb'])

            item = {'matches': {'idigbio': matches['idigbio'], 'pbdb': matches['pbdb']}}
            d.append(item)
            print "Resolving References..."
            d = self.resolveReferences(d)
            print "Resolved References!!!"
            counts = {'totalCount': idbCount + pbdbCount, 'idbCount': idbCount, 'pbdbCount': pbdbCount}

            return self.respond({'counts': counts, 'results': d, 'criteria': criteria})

        else:
            return self.respondWithDescription()

    def description(self):
		return {
			'name': 'Lithostratigraphic Name Index',
			'maintainer': 'Michael Benowitz',
			'maintainer_email': 'michael@epandda.org',
			'description': 'Returns specimens matching lithostratigraphic units',
			'params': [
			{
				"name": "lithostratigraphy",
				"label": "Lithostratigraphy Full Text",
				"type": "text",
				"required": False,
				"description": "Search on the full lithostratigraphic hierarchy "
			},{
				"name": "rank",
				"label": "lithostratigraphic Rank",
				"type": "text",
				"required": False,
				"description": "Limit your search to a lithostratigraphic rank. Accepted values are: Bed, Member, Formation & Group"
			}]
		}
