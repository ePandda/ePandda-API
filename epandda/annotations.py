import re
from mongo import mongoBasedResource

#
# Single Annotation View
#

class single(mongoBasedResource):

  def process(self):
    pass

  def get(self, annotation_id):

    # Mongodb index for Annotations
    annotations = self.client.endpoints.annotations

    annotation_id_string = "urn:uuid:" + str(annotation_id)
    res = annotations.find({'@id': annotation_id_string}, {'_id': False})
   
    d = []
    for i in res:
      d.append(i)

    return self.respond({
      'counts': 1,
      'results': d,
      'criteria': {'annotation_uuid': annotation_id}
    })

  def description(self):
        return {
            'name': 'Single Annotation',
            'maintainer': 'Jon Lauters',
            'maintainer_email': 'jon@epandda.org',
            'description': 'Returns single annotation. (ex: https://api.epandda.org/annotations/single/<uuid>)',
            'params': []
           
        }


#
# Main Annotations Class Functionality
#

class annotations(mongoBasedResource):
    def process(self):

        # Mongodb index for Annotations
        annotations = self.client.endpoints.annotations
  
        # returns dictionary of params as defined in endpoint description
        # will throw exception if required param is not present
        params = self.getParams()
        
        # offset and limit returned as ints with default if not set
        offset = self.offset()
        limit = self.limit()

        if limit < 1:
          limit = 100

        annoQuery = []
        if self.paramCount > 0:

          criteria = {
            'endpoint': 'annotations',
            'parameters': {},
          }

          for p in ['annotationDate', 'annotationDateAfter', 'annotationDateBefore', 'quality_score']:  
 
            if params[p]:

              if 'annotationDate' == p:
                annoQuery.append({"annotatedAt": { '$regex': params[p]} })

              if 'annotationDateAfter' == p:
                annoQuery.append({"annotatedAt": { '$gte': params[p]} }) 
            
              if 'annotationDateBefore' == p:
                annoQuery.append({"annotatedAt": { '$lte': params[p]} })

              if 'quality_score' == p:
                annoQuery.append({"quality_score": { '$gte': int(params[p]) }})

              criteria['parameters'][p] = str(params[p]).lower()

          d = []
 
          # Total Count:
          annoCount = annotations.find({}).count()

          if annoQuery:
            res = annotations.find({"$and":  annoQuery }, {'_id': False}).skip(offset).limit(limit)
            annoCount = res.count()

          else:
            # Allows for optional Date param since you can't $and on nothing.
            res = annotations.find({}, {'_id': False}).skip(offset).limit(limit)
            


          if res:
              for i in res:
                  d.append(i)


          counts = {'totalCount': annoCount, 'annotationsCount': len(d)}

          return self.respond({
              'counts': counts, 
              'results': d,
              'criteria': criteria,
          })
        else:

          return self.respondWithDescription()
            

    def description(self):
        return {
            'name': 'Annotations',
            'maintainer': 'Jon Lauters',
            'maintainer_email': 'jon@epandda.org',
            'description': 'Returns openAnnotations for linked data in ePANDDA.',
            'params': [
                {
                    "name": "annotationDate",
                    "label": "Annotation Date",
                    "type": "text",
                    "required": False,
                    "description": "Filter annotation results by provided date ( simple date match ). Format annotationDate=YYYY-MM-DD"
                },
                {
                    "name": "annotationDateAfter",
                    "label": "Annotation Date After",
                    "type": "text",
                    "required": False,
                    "description": "Filter annotation results equal to or after provided date. Format annotationDateAfter=YYYY-MM-DD"
                },
                {
                    "name": "annotationDateBefore",
                    "label": "Annotation Date Before",
                    "type": "text",
                    "required": False,
                    "description": "Filter annotation results before provided date. Format annotationDateBefore=YYYY-MM-DD"
                },
                {
                   "name": "quality_score",
                   "label": "Quality Score",
                   "type": "text",
                   "required": False,
                   "description": "Filter annotation results equal to or greater than value"
                }
            ]
        }
                
