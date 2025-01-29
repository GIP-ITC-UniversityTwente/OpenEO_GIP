from flask_restful import Resource
from flask import make_response, jsonify, request
from globals import globalsSingleton
from constants.constants import *
from processmanager import linkSection
from userinfo import UserInfo
from authentication import AuthenticatedResource
from pathlib import Path
import os
from datetime import datetime
from processmanager import makeBaseResponseDict
import subprocess
import json
import jsonschema
from jsonschema import validate
import glob
from rasterdata import RasterData
import ilwis
import shutil


def find_sources(data):
    sources = []

    def recursive_search(d):
        # Check if current level is a dictionary
        if isinstance(d, dict):
            for key, value in d.items():
                if key == 'source' and value != 'all': # all is a special value 'tag' and can be ignored
                    sources.append(value)
                else:
                    # Recursive call for nested structures
                    recursive_search(value)
        # If it's a list, iterate over each item
        elif isinstance(d, list):
            for item in d:
                recursive_search(item)

    # Start the search
    recursive_search(data)
    return sources

def checkMetadata(metajson, folder):
    metafile = open('./config/metadataformat.json')
    metadict = json.load(metafile) 
    try:
        validate(instance=metajson, schema=metadict)
        raster = globalsSingleton.id2Raster(metajson['id'])
        if raster != None:
            return "Id for this dataset is already in use:"  + metajson['id']
        sources = find_sources(metajson)
        datafolder = metajson['dataFolder']
        for datasource in sources:
            fpath = os.path.join(folder, datafolder, datasource)
            if not os.path.exists(fpath):
                return "data source not found: " + datasource

    except jsonschema.exceptions.ValidationError as e:
       return "metadata invalid:" +  e.message   
    return ""

def checkdata(folder, fpath):
    message = ""
    if not os.path.exists(folder):
        return "no user folder exists"
    
    file_extension = os.path.splitext(fpath)[1]
    if file_extension != '.zip'and file_extension != '.gz':
        if file_extension == '.tif' or file_extension == '.nc' :
            ilwRaster = ilwis.RasterCoverage(fpath)
            if ilwRaster:
                raster = RasterData() 
                raster.load(ilwRaster, 'ilwisraster')
                globalsSingleton.insertRasterInDatabase(raster) 
                globalsSingleton.saveIdDatabase() 
                datafolder = raster['dataFolder']
                if not os.path.exists(datafolder):
                    os.makedirs(datafolder)
                shutil.move(fpath, datafolder)
                raster.toMetadataFile(folder, raster['title'] + '.metadata') 
            else:            
                return "uploaded file must be a tuple of data-file and a metadata file in zipped format"
            
    if file_extension == '.gz':
        b = os.path.basename(fpath)
        parts = b.split('.')
        basename = parts[0]
        if parts[1] == 'tar':
            #dont use the python tarfile lib; its very, very slow
            subprocess.run(["tar", "-xf", fpath, "-C", folder], check=True)
        else:
             #dont use the python gzip lib; its very, very slow
            subprocess.run(["gunzip", fpath], check=True)
        files = glob.glob(folder + "/*.metadata")
        for filename in files:
            path = os.path.join(folder, filename) 
            with open(path, "r") as fp:
                    fmeta = json.load(fp) 
                    message = checkMetadata(fmeta, folder)  
            if message == "":                    
                raster = RasterData()                       
                raster.load(path, 'metadata')
                globalsSingleton.insertRasterInDatabase(raster) 
                globalsSingleton.saveIdDatabase()    

    return message


class OpenEOUploadFile(AuthenticatedResource):
   
    def put(self, path):
        binary_data = request.data
        user = UserInfo(request)
        username = user.username
        rootdata = globalsSingleton.openeoip_config['data_locations']['root_user_data_location']['location']
    
        folder = os.path.join(rootdata, username)

        if not os.path.exists(folder):
            err = globalsSingleton.errorJson('FilePathInvalid', -1,'')
            return make_response(jsonify(err),err['code']) 
        filename = os.path.basename(path)

        dir = username + "/" + filename
        fpath = os.path.join(folder, filename)

        with open(fpath, 'wb') as f:
            f.write(binary_data)

        file_size = os.path.getsize(fpath)
        mod_time = os.path.getmtime(fpath)
        mod_date = datetime.fromtimestamp(mod_time).strftime('%Y-%m-%d %H:%M:%S') 

        err = checkdata(folder, fpath)
        if err == "":
            r = {'path' : dir, 'size' : file_size, 'modified' : mod_date}
            return make_response(r, 200) 
        return make_response(makeBaseResponseDict(-1, 'error', 404, None, err),400) 
              





