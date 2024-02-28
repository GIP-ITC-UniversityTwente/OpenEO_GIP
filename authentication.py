from flask import Flask, jsonify, make_response, request
from flask_restful import Resource
from authenticationdatabase import authenticationDB
import hashlib
from datetime import datetime, timedelta
from globals import globalsSingleton
import functools

def authenticateError():
    err = globalsSingleton.errorJson('AuthenticationRequired', 0,'')
    resp =  make_response(jsonify(err),err['code']) 
    resp.headers['WWW-Authenticate'] = 'Bearer realm="Main"'
    return resp

def requires_authorization(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if 'Authorization' not in request.headers:
            return authenticateError()

        auth = request.headers['Authorization']
        auth = auth.split()[1]
        if 'basic//' not in auth:
            return authenticateError()

        token = auth.split('basic//')[1]
        if authenticationDB.tokenExpired(token) == '?':
            return authenticateError()

        return f(*args, **kwargs)
    return decorated


class AuthenticatedResource(Resource):
    decorators = []
    decorators.append(requires_authorization)

    def __init__(self):
        Resource.__init__(self)

class Authenitication(Resource):
    def __init__(self):
        Resource.__init__(self)  

    def get(self):
        r = request
        auth = request.authorization
        if not auth or not authenticationDB.login(auth.username, auth.password):
            return authenticateError()
        hash = hashlib.sha256((globalsSingleton.token_secret +
                               auth.username +
                               str(datetime.now())).encode('UTF-8'))
        hex = hash.hexdigest()
        endTime = datetime.now() + timedelta(days=1)
        endTime = endTime.strftime("%Y/%m/%d %H:%M:%S")
        authenticationDB.addToken(hex, auth.username, str(endTime))
        return make_response(jsonify({
                'user_id': auth.username,
                'access_token': hex
            }), 200)
