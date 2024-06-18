import os
import json
from pathlib import Path
import pickle
from operations.registerOperations import initOperationMetadata 
from constants import constants
import datetime
import common
import uuid
import sqlite3
from authenticationdatabase import authenticationDB
from flask import jsonify, make_response
import fnmatch
import copy

def authenticateError():
    err = globalsSingleton.errorJson('AuthenticationRequired', 0,'')
    resp =  make_response(jsonify(err),err['code']) 
    resp.headers['WWW-Authenticate'] = 'Bearer realm="Main"'
    return resp

def getOperation(operationName)        :
    if  operationName in globalsSingleton.operations:
        return globalsSingleton.operations[operationName]
    return None

class Globals : 
    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(Globals, cls).__new__(cls)
        return cls.instance
    try:
        serverValid = True
        openeoip_config = None
        internal_database = {}
        signed_url_secret = str(uuid.uuid4())
        token_secret = str(uuid.uuid4()) 
        operations = initOperationMetadata(getOperation)
        authenticationDB.deleteTokens()

    except Exception as ex:
        serverValid = False

    def __init__(self):
        self.initGlobals()
        
    def initGlobals(self):
        if  self.openeoip_config == None:
            openeoip_configfile = open('./config/config.json')
            self.openeoip_config = json.load(openeoip_configfile)
            codesfile = open('./config/default_error_codes.json')
            self.default_errrors = json.load(codesfile) 
       

                               
    def create_table(self, create_table_sql):
        try:
            c = self.databseConn.cursor()
            c.execute(create_table_sql)
        except Exception as e:
            print(e)  
    
    def create_connection(self, db_file):
        self.databseConn = None
        try:
            self.databseConn = sqlite3.connect(db_file)
            print(sqlite3.version)
        except Exception as e:
            print(e)
        finally:
            if self.databseConn:
                self.databseConn.close()

    def insertRasterInDatabase(self, raster):
        self.internal_database[raster['id']] = raster

    def filepath2raster(self, filename):
        items = self.internal_database.items()
        for item in items:
            if item[0] == filename:
                return item[1]
        return '?' 
    
    def id2Raster(self, id):
        items = self.internal_database.items()
        #if size ==0 then the scan on data location has not happened so we look into the saved properties of a previous scan
        if len(items) == 0:
            if self.loadIdDatabase():
                items = self.internal_database.items()
       
        for item in items:
            p = item[0]
            if p == id:
                raster = item[1]
                if not os.path.exists(raster['dataSource']): #virtual datasets with no real source
                    return raster
                mttime = datetime.datetime.fromtimestamp(os.path.getmtime(raster['dataSource']))
                if mttime == raster['lastmodified']:
                    return raster
        return None        
    
    def saveIdDatabase(self):
        home = Path.home()
        loc = globalsSingleton.openeoip_config['data_locations']['system_files']
        sytemFolder = os.path.join(home, loc['location'])
        propertiesFolder = os.path.join(home, sytemFolder)
        common.makeFolder(propertiesFolder)
                        
        propsPath = os.path.join(propertiesFolder, 'id2filename.table')
        propsFile = open(propsPath, 'wb')
        cp_db = {}
        #keys SYNTHETIC_DATA may not be saved as they contain a 'unpickable' member and are generated anyway
        for key,value in self.internal_database.items():
            if key.find('SYNTHETIC_DATA') == -1:
                cp_db[key] = value

        pickle.dump(cp_db, propsFile)
        propsFile.close() 

    def loadIdDatabase(self):
        home = Path.home()
        loc = globalsSingleton.openeoip_config['data_locations']['system_files']
        sytemFolder = os.path.join(home, loc['location'])        
        propertiesFolder = os.path.join(home, sytemFolder)
        common.makeFolder(propertiesFolder)
        
        propertiesPath = os.path.join(propertiesFolder, 'id2filename.table')
        if ( os.path.exists(propertiesPath)):
            file_stats = os.stat(propertiesPath)
            if file_stats.st_size > 0:
                with open(propertiesPath, 'rb') as f:
                    data = f.read()
                self.internal_database = pickle.loads(data) 
                return True
        return False            
        
    def errorJson(self, errorStringCode, id, message):
        if errorStringCode == constants.CUSTOMERROR:
            return {"id" : id, "code" : 400, "message" : message }
        else:
            if errorStringCode in self.default_errrors:
                err = self.default_errrors[errorStringCode]
                predefCode = err['http']
                message = err['description']                   
                return {"id" : id, "code" : predefCode, "message" : message }
                
        return {"id" : id, "code" : 400, "message" : message }

    
        
globalsSingleton = Globals()

                






