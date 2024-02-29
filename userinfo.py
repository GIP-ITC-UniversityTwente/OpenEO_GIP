from flask import request
from authenticationdatabase import authenticationDB
from globals import authenticateError

class UserInfo:
    def __init__(self, sessioninfo):
        #note this all very temporary for debug user, needs to redesigned for real auhtentication\
        # but this hides tyhis process behind a class
        self.username = 'undefined'
        if sessioninfo != None and sessioninfo.authorization != None:
            if 'token' in sessioninfo.authorization:
                auth = sessioninfo.authorization.token
                auth = auth.split()[1]
                if 'basic//' not in auth:
                    return authenticateError()

                token = auth.split('basic//')[1]
                if authenticationDB.tokenExpired(token) == '?':
                    return authenticateError() 
                else:                   
                    self.username = authenticationDB.getUserFromToken()
            else: 
                if sessioninfo.authorization['username'] != None:
                    self.username = sessioninfo.authorization['username']
    
    def __eq__(self, other):
        if not isinstance(other, UserInfo):
            return NotImplemented
        
        return self.username == other.username

