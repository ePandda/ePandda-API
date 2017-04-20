from mongo import mongoBasedResource
from response_handler import response_handler

#
#
#
class occurrences(mongoBasedResource):
    def get(self):

        # required
        taxon_name = self.getRequest().args.get('taxon_name')   # param
        
        # optional
        locality   = self.getRequest().args.get('locality')       # param
        period     = self.getRequest().args.get('period')
        inst_code  = self.getRequest().args.get('institution_code')

        
        # params dict list 
        params = [
          {
            "name": "taxon_name",
            "type": "text",
            "value": taxon_name,
            "required": True,
            "description": "The taxa to search occurrences for"
          },
          {
            "name": "locality",
            "type": "text",
            "value": locality,
            "required": False,
            "description": "The locality name to bound taxonomic occurences to",
          },
          {
            "name": "period",
            "type": "text",
            "value": period,
            "description": "The geologic time period to filter taxon occurrences by"
          },
          {
            "name": "institution_code",
            "type": "text",
            "char_limit": "TBD",
            "value": inst_code,
            "description": "The abbreviated institution code that houses the taxon occurrence specimen"
          }
        ]   



        # TODO: use occ Index
        lindex = self.client.test.spIndex                       # Mongodb index for localities

        if taxon_name:

          # TODO: update for Occurrences

          res = lindex.find({"$text": {"$search": locality}})

          d = []
          for i in res:
              terms = i['original_terms']
              terms.append(locality)
              item = {"terms": terms, "matches": {"pbdb": i['pbdb_data'], "idigbio": i['idb_data']}}
              d.append(item)

          d = self.resolveReferences(d)
          resp = self.toJson(d)

        else:

          # TODO: Think how best to automate / make dynamic
          resp = {
            'endpoint_description': 'returns specimens collected from a given locality',
            'params': params
          }

        return response_handler( resp )
