from flask import Flask, request, current_app, make_response, session, escape, Response
import hashlib
import json

config = json.load(open('./config.json'));


#
# Extend make_cache_key from flask_caching to enable cache keys to be set on api parameters
#
def make_cache_key(*args, **kwargs):
    path = request.path
    args = request.args
    args_str = str(args.to_dict())
    cache_key = str(hashlib.md5(path+args_str).hexdigest())
    return cache_key

#
# Allow users to bypass cache by setting a parameter
#
def bypass_caching(*args, **kwargs):
    path = request.path
    args = request.args
    
    if 'debug' in config and config['debug'] == True:
    	return True
    	
    if 'skipCache' in args and (args['skipCache'] == '1' or args['skipCache'].lower() == 'true'):
        return True
        
    return False
