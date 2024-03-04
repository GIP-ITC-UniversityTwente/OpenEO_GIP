from flask import request
from authenticationdatabase import authenticationDB
from globals import authenticateError

class UserInfo:
    def __init__(self, sessioninfo):
        self.username = 'undefined'
        if sessioninfo != None and sessioninfo.authorization != None:
            if hasattr(sessioninfo.authorization, 'token'):
                auth = sessioninfo.authorization.token
                if 'basic//' not in auth:
                    return authenticateError()

                token = auth.split('basic//')[1]
                if authenticationDB.tokenExpired(token) == '?':
                    return authenticateError() 
                else:                   
                    self.username = authenticationDB.getUserFromToken(token)
            else: 
                if sessioninfo.authorization['username'] != None:
                    self.username = sessioninfo.authorization['username']   
    
    def __eq__(self, other):
        if not isinstance(other, UserInfo):
            return NotImplemented
        
        return self.username == other.username

