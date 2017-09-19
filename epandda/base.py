from flask import request, Response
from flask_restful import Resource, Api
import json
from pymongo import MongoClient
from bson import Binary, Code, json_util, BSON, ObjectId
from bson.json_util import dumps
import datetime
import importlib
import inspect
from sources import idigbio
from sources import paleobio
from sources import paleobio_refs
import re
import annotation

#
# Base class for API resource
# Sets up environment shared by all API calls, such as loading the API config file
#

class baseResource(Resource):
    def __init__(self):
        super(baseResource, self).__init__()

        # Load API config
        self.config = json.load(open('./config.json'))

        self.client = MongoClient("mongodb://" + self.config['mongodb_user'] + ":" + self.config['mongodb_password'] + "@" + self.config['mongodb_host'])
        #self.client = MongoClient("mongodb://127.0.0.1")
        self.idigbio = self.client.idigbio.occurrence
        self.pbdb = self.client.pbdb.pbdb_occurrences
        self.refs = self.client.pbdb.pbdb_refs
        self.annotations = self.client.endpoints.annotations


        self.params = None
        self.paramCount = 0

        self.fields = {
            "idigbio": [],
            "paleobio": []
        }

        self.returnResponse = True

        self.sources = {
            "idigbio": idigbio.idigbio(),
            "paleobio": paleobio.paleobio(),
            "paleobio_refs": paleobio_refs.paleobio_refs()
        }

    #
    # Return request object
    #
    def getRequest(self):
        return request

    #
    # Resolve underlying data source ids (from iDigBio, PBDB, Etc.) to URLs the end-user can use
    #
    def resolveReferences(self, data, pbdb_type='occs', show_type='full'):

        resolved_references = {"idigbio_resolved" : [], "pbdb_resolved": [] }

        idigbio_fields = self.getFieldsForSource("idigbio", True)
        paleobio_fields = self.getFieldsForSource("paleobio", True)

        if "refs" == pbdb_type:
          paleobio_fields = self.getFieldsForSource("paleobio_refs", True) 

        offset = self.offset()
        limit = self.limit()

        idigbio_ids = []
        pbdb_ids = []

        # Could params this if need be?
        use_UUID = True

        for item in data:
            if len(idigbio_ids) < offset + limit:

                if ObjectId.is_valid( item["matches"]["idigbio"] ):
                  use_UUID = False

                idigbio_ids = idigbio_ids + item["matches"]["idigbio"]

            if len(pbdb_ids) < offset + limit:

                if ObjectId.is_valid( item["matches"]["pbdb"] ):
                  use_UUID = False

                pbdb_ids = pbdb_ids + item["matches"]["pbdb"]

            if (len(idigbio_ids) >= offset + limit) and (len(pbdb_ids) >= offset + limit):
                 break

        if offset > 0:
            idigbio_ids = idigbio_ids[offset:]
            pbdb_ids = pbdb_ids[offset:]

        if limit > 0:
            idigbio_ids = idigbio_ids[offset:limit]
            pbdb_ids = pbdb_ids[offset:limit]

        # Double limiting? 
        if not use_UUID:
            idigbio_ids = map(lambda id: ObjectId(id), idigbio_ids[0:limit])
            pbdb_ids = map(lambda id: ObjectId(id), pbdb_ids[0:limit])

            #
            # resolve idigbio refs
            #
            m = self.idigbio.find({"_id": {"$in" : idigbio_ids}})
        else:
            m = self.idigbio.find({"idigbio:uuid": {"$in" : idigbio_ids}})
            idigbio_records = {}

            for i in m:
                idigbio_records[i['idigbio:uuid']] = i

            pbdb_ids = [ int(pbdb_id) for pbdb_id in pbdb_ids ]

            if "refs" == pbdb_type:

              print "making refs query"
              pbdb_ids_str = [ str(pbdb_id) for pbdb_id in pbdb_ids ]
              p = self.refs.find({"pid": {"$in": pbdb_ids_str}}, {"_id": False})
              pbdb_records = {}
              for i in p:
                pbdb_records[ int(i['pid']) ] = i

            else: 
              p = self.pbdb.find({"occurrence_no": {"$in" : pbdb_ids}})
              pbdb_records = {}
              for i in p:
                 pbdb_records[i['occurrence_no']] = i

        resolved = []
        for idb_uuid in idigbio_ids:
            row = {"uuid": str(idb_uuid), "url": "https://www.idigbio.org/portal/records/" + str(idb_uuid)}

            if idigbio_fields is not None:
                for f in idigbio_fields:
                	if idb_uuid in idigbio_records:
                		if f in idigbio_records[idb_uuid]:
                			row[f] = idigbio_records[idb_uuid][f]
            resolved.append(row)

        resolved_references["idigbio_resolved"] = resolved

        if 'refs' == pbdb_type:
          show_type = 'both'

        resolved = []
        for mitem in data:

          for pbdbid in pbdb_ids:
            row = {"url": 'https://paleobiodb.org/data1.2/' + pbdb_type + '/single.json?id=' + str(pbdbid) + '&show=' + show_type }

            if paleobio_fields is not None:
                for f in paleobio_fields:
                    if f in pbdb_records[pbdbid]:
                        row[f] = pbdb_records[pbdbid][f]
            
            resolved.append(row)

        resolved_references["pbdb_resolved"] = resolved

        return resolved_references

    #
    # Set parameter data directly
    #
    def setParams(self, data):
        desc = self.description()['params'][:]

        if "offset" not in data:
            data["offset"] = 0
        if "limit" not in data:
            data["limit"] = 10

        for p in desc:
            if p["name"] not in data:
                data[p["name"]] = None

        self.params = data
        self.validateParams()
        self.paramCount = len(data)

    #
    # Get request parameters. Only parameters declared in the endpoint's description will be
    # returned, along with standard control parameters like offset and limit.
    #
    # If a parameter is not set it will be returned as None.
    #
    # If an error occurs while parsing parameters (Eg. a required parameter is missing; no parameters are set)
    # an exception will be raised.
    #
    def getParams(self):
        if  self.paramCount > 0:
            return self.params

        desc = self.description()['params'][:]
        r = self.getRequest()
        r_as_json = request.get_json(silent=True)

        # always pull offset and limit params
        desc.append({"name": "offset"})
        desc.append({"name": "limit"})

        # always pull in source field lists
        desc.append({"name": "idigbio_fields"})
        desc.append({"name": "paleobio_fields"})

        self.params = {}

        c = 0

        for p in desc:
            # JSON blob
            if r_as_json is not None:
                if(p['name'] in r_as_json):
                    self.params[p['name']] = r_as_json[p['name']].lower()
                    c = c + 1
                else:
                    self.params[p['name']] = None

            # POST request
            elif request.method == 'POST':
                if(p['name'] in request.form):
                    self.params[p['name']] = request.form[p['name']].lower()
                    c = c + 1
                else:
                    self.params[p['name']] = None

            # GET request
            elif request.method == 'GET':
                v = r.args.get(p['name'])
                if (v):
                    self.params[p['name']] = v.lower()
                    c = c + 1
                else:
                    self.params[p['name']] = None

        self.validateParams()

        self.paramCount = c
        
        return self.params

    #
    # Validate parameter values
    #
    def getFieldsForSource(self, source, defaultToAll=False):
        ucFields = {'idigbio:geopoint': 'idigbio:geoPoint', 'dwc:scientificname': 'dwc:scientificName'}
        if source in self.sources:
            fields_available_in_source = self.sources[source].availableFields()
            if source + "_fields" in self.params and self.params[source + "_fields"] is not None and len(self.params[source + "_fields"]) > 0:
                fields = re.split("[,; ]", self.params[source + "_fields"])
		i = 0
                for field in fields:
		    if field in ucFields:
                        fields[i] = ucFields[field]
                    i += 1
                return list(set(fields) & set(fields_available_in_source))
            elif defaultToAll:
                return fields_available_in_source

        return None


    #
    # Validate parameter values
    #
    def validateParams(self):
        desc = self.description()['params'][:]
        errors = {}

        for p in desc:
            # Throw errors for missing required values
            if 'required' in p and p['required'] and (self.params[p['name']] is None or len(self.params[p['name']]) == 0):
                errors[p['name']] = "No value for required parameter " + p['name']

        if len(errors) > 0:
            raise Exception(errors)

        return True

    #
    # Return query list from submitted multi-query JSON blob
    # An exception will be raised if the query list is not set or empty
    #
    def getQueryList(self):
        r = self.getRequest()
        r_as_json = request.get_json(silent=True)

        if (r_as_json is None):
            raise Exception({"GENERAL": "Query parameters are not set"})

        if "queries" in r_as_json and type(r_as_json['queries']) is list and len(r_as_json['queries']) > 0:
            return r_as_json['queries']

        raise Exception({"GENERAL": "Query list is empty or not set"})

    #
    # Get row offset for returned result set
    #
    def offset(self):
        if self.params is None or self.params.get('offset') is None:
            self.getParams()
        return 0 if self.params['offset'] is None else int(self.params['offset'])

    #
    # Get maximum number of records to return in result set
    #
    def limit(self):
        if self.params is None or self.params.get('limit') is None:
            self.getParams()
        return 10 if self.params['limit'] is None else int(self.params['limit'])

    #
    # Default description block for endpoints that don't describe themselves
    #
    def description(self):
        return {
            'maintainer': 'Orphan Pandda',
            'maintainer_email': 'orphan@epandda.org',
            'name': '[NO NAME]',
            'description': '[NO DESCRIPTION]' + str(self),
            'params': [{
                "name": "sample_parameter",
                "type": "text",
                "required": False,
                "description": "If the endpoint maintainer had bothered to document the available parameters, each parameter would be described in this format."
            }]
        }

    #
    # Return data object as string
    #
    def serialize(self, data):
        return dumps(data)

    #
    # Return data object as JSON. Can handle Mongo cursors.
    #
    def toJson(self, data):
        # Serialize Mongo cursor as JSON
        json_data = json.loads(dumps(data, sort_keys=True, indent=4, default=json_util.default))

        # Kill _id keys (and maybe some others too soon...)
        #for record in json_data:
        #  record.pop('_id', None)
        return json_data


    #
    # Default handler for GET requests
    # (just calls process() and returns response as-is, which should be fine 99.9999999999% of the time)
    #
    def get(self):
        try:
            return self.process()
        except Exception as e:
            return self.respondWithError(e.args[0])

    #
    # Default handler for POST requests
    # (just calls process() and returns response as-is, which should be fine 99.9999999999% of the time)
    #
    def post(self):
        #return self.process()
        try:
            return self.process()
        except Exception as e:
            return self.respondWithError(e.args[0])

    #
    #
    #
    def getParameterNames(self):
        desc = self.description()

        names = []
        for f in desc['params']:
            names.append(f['name'])

        return names

    #
    # Return an API response. The format of the response can vary based upon
    # respType, of which are supported:
    #
    #   "data" => an API data response for a valid user query [DEFAULT]
    #   "routes" => a list of valid API endpoints, with descriptions for each.
    #
    def respond(self, return_object, respType="data"):

        # Default Response
        if respType == "routes":
            defaults = {
                "description": "",
                "routes": {},
                "timeReturned": str(datetime.datetime.now()),
                "v": self.config['version']
            }
        elif respType == "queries":
            defaults = {
                "queries": [],
                "timeReturned": str(datetime.datetime.now()),
                "v": self.config['version']
            }
        else:
            defaults = {
                "counts": {},
                "timeReturned": str(datetime.datetime.now()),
                "offset": self.offset(),
                "limit": self.limit(),
                "success": True,
                "specimenData": False,
                "includeAnnotations": False,
                "results": {},
                "criteria": {},
                "v": self.config['version'],
            }

        resp = {}

        for k in defaults:
            if k in return_object:
                resp[k] = return_object[k]
            else:
                resp[k] = defaults[k]

        # Default Mimetype with override handling
        mime_type = "application/json"
        if "mimetype" in return_object:
            mime_type = return_object['mimetype']

        # Default Status Code with override handling
        status_code = 200
        if "status_code" in return_object:
            status_code = return_object['status_code']

        if "data" in return_object:
            resp['data'] = return_object['data']

        if "params" in return_object:
            resp['params'] = return_object['params']
        if 'media' in return_object:
            resp['media'] = return_object['media']


        # Get annotations by resolved PBDB Ref URL
	if "includeAnnotations" in resp:
          if resp['includeAnnotations']:
            resp['annotations'] = []
            for pb in return_object['results']['pbdb_resolved']:
              annoCursor = self.annotations.find({"hasBody.@id": pb['url']},{"_id": False})
              for anno in annoCursor:
                resp['annotations'].append( anno )


        if self.returnResponse:
            return Response(json.dumps(resp, sort_keys=True, indent=4, separators=(',', ': ')).encode('utf8'), status=status_code, mimetype=mime_type)
        else:
            return resp

    #
    # Determine if method is defined in a subclass.
    #
    def isMethod(self, obj, name):
        return hasattr(obj, name) and inspect.ismethod(getattr(obj, name))

    #
    #
    #
    def loadEndpoint(self, endpoint):
        try:
            module = importlib.import_module("." + endpoint, "epandda")
            for x in dir(module):
                obj = getattr(module, x)

                if inspect.isclass(obj) and issubclass(obj, Resource) and obj is not Resource and self.isMethod(obj, "process"):
                    return obj()
            return None
        except ImportError as e:
            return None

    #
    # Respond with description of endpoint. Used when user hits an endpoint with no parameters.
    #
    def respondWithDescription(self):

        if self.returnResponse:
            return Response(json.dumps(self.description(), sort_keys=True, indent=4, separators=(',', ': ')).encode('utf8'), status=200, mimetype="text/json")
        else:
            return self.description()

    #
    # Respond with error message.
    #
    def respondWithError(self, errors):
        names = self.getParameterNames()

        # process errors, omitting ones not related to a parameter or GENERAL (the generic error heading)
        errors_filtered = {}

        for i in errors:
            if i == "GENERAL" or i in names:
                if type(errors[i]) is list:
                    errors_filtered[i] = errors[i]
                elif type(errors[i]) is tuple:
                    errors_filtered[i] = [v for key in errors[i] for v in key]
                else:
                    errors_filtered[i] = [errors[i]]
            else:
                errors_filtered[i] = "Unknown error: " + errors

        if self.returnResponse:
            return Response(json.dumps({"errors": errors_filtered}, sort_keys=True, indent=4, separators=(',', ': ')).encode('utf8'), status=500, mimetype="text/json")
        else:
            return {"errors": errors_filtered}
