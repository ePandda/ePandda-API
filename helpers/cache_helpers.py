from flask import Flask, request, current_app, make_response, session, escape, Response
import hashlib
#
# Extend make_cache_key from flask_caching to enable cache keys to be set on api parameters
#
def make_cache_key(*args, **kwargs):
    path = request.path
    args = request.args
    args_str = str(args.to_dict())
    cache_key = str(hashlib.md5(path+args_str).hexdigest())
    return cache_key
