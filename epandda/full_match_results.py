from mongo import mongoBasedResource
from flask_restful import reqparse
import json

parser = reqparse.RequestParser()

# Add Arguments (params) to parser here ...
parser.add_argument('matchQuery', type=str, help='JSON string of ElasticSearchQuery to base full query on')
parser.add_argument('sourceQuery', type=str, help='JSON string of ElasticSearchQuery to base source query on')

#
#
#
class full_match_results(mongoBasedResource):
    def process(self):
        # returns dictionary of params as defined in endpoint description
        # will throw exception if required param is not present
        params = self.getParams()
        if self.paramCount > 0:
            if params['matchQuery']:
                query = json.loads(params['matchQuery'])
            elif params['sourceQuery']:
                query = json.loads(params['sourceQuery'])

            res = self.es.search(index="idigbio,pbdb", body=query)
            results = {"results": [], "total": res['hits']['total'], "query": query}
            for hit in res['hits']['hits']:
                row = self.resolveReference(hit['_source'], hit['_id'], hit['_type'])
                results["results"].append(row)
            return results

        else:
            return self.respondWithDescription()

    def description(self):
		return {
			'name': 'Full Result Response',
			'maintainer': 'Michael Benowitz',
			'maintainer_email': 'michael@epandda.org',
			'description': 'Returns full data sets for matching criteria returned from the main Occurrence endpoint',
			'params': [
				{
					"name": "sourceQuery",
					"label": "Source Query",
					"type": "text",
					"required": False,
					"description": "The query to the source collection as a JSON string",
					"display": True
				},
				{
					"name": "matchQuery",
					"label": "Match Query",
					"type": "text",
					"required": False,
					"description": "The query to the matched collection as a JSON string",
					"display": True
				}
            ]
        }
