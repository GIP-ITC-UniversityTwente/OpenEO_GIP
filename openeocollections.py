import os
import json
from flask import Flask, jsonify, request
from flask_restful import Api, Resource
##from globals import openeoip_config
from globals import globalsSingleton
from eoreader.bands import *
from pathlib import Path
from userinfo import UserInfo
from rasterdata import RasterData
import common
import logging
import tests.addTestRasters as tr


class OpenEOIPCollections(Resource):
    def get(self):
        ##request_json = request.get_json()
        return jsonify(loadCollections()) 
    

def processMetaFile(filename):
    

    return []

def loadFile(fullPath, extraMetadataAll):
    try:
        raster = RasterData()                    

        if Path(fullPath).suffix != ".metadata":
            raster.load(fullPath, 'eoreader', extraMetadataAll)
        else:    
            raster.load(fullPath, 'metadata')

        if raster['id'] != None:
            globalsSingleton.insertRasterInDatabase(raster) 

        return raster            
            
    except Exception as ex:
        common.logMessage(logging.ERROR,str(ex),common.process_user)
    return None

def loadCollections():
    """
    Loads all collections for the current user by reading raster files.

    Returns:
        A dictionary containing all collections and links.
    """
    user = UserInfo(request)
    data_locations = _getDataLocations(user)
    common.logMessage(logging.INFO, 'reading collections', common.process_user)

    allCollections = []
    for location in data_locations:
        path = location["location"]
        extraMetadataAll = _loadExtraMetadata(path)
        allCollections.extend(_processLocation(path, extraMetadataAll))

    allJson = {
        "collections": allCollections,
        "links": globalsSingleton.openeoip_config['links']
    }

    _finalizeCollections()
    return allJson

def _getDataLocations(user):
    """
    Retrieves the data locations for the current user.

    Args:
        user: The current user.

    Returns:
        A list of data location dictionaries.
    """
    data_locations = []
    data_locations.append(globalsSingleton.openeoip_config['data_locations']['root_data_location'])
    loc = globalsSingleton.openeoip_config['data_locations']['root_user_data_location']
    loc['location'] = os.path.join(loc['location'], user.username)
    data_locations.append(loc)
    return data_locations

def _loadExtraMetadata(path):
    """
    Loads extra metadata from the specified path.

    Args:
        path: The path to the directory containing the extra metadata file.

    Returns:
        The extra metadata as a dictionary, or None if the file does not exist.
    """
    extraPath = os.path.join(path, 'extrametadata.json')
    return openExtraMetadata(extraPath)

def _processLocation(path, extraMetadataAll):
    """
    Processes a single data location, reading raster files and metadata.

    Args:
        path: The path to the data location.
        extraMetadataAll: The extra metadata for the location.

    Returns:
        A list of collection dictionaries.
    """
    collections = []
    if os.path.isdir(path):
        files = os.listdir(path)
        for filename in files:
            if filename != 'extrametadata.json':
                fullPath = os.path.join(path, filename)
                if not os.path.isdir(fullPath):
                    collection = _processFile(fullPath, filename, extraMetadataAll)
                    if collection:
                        collections.append(collection)
    return collections

def _processFile(fullPath, filename, extraMetadataAll):
    """
    Processes a single file, loading its raster data and metadata.

    Args:
        fullPath: The full path to the file.
        filename: The name of the file.
        extraMetadataAll: The extra metadata for the location.

    Returns:
        A dictionary representing the collection, or None if the file could not be processed.
    """
    name = os.path.splitext(filename)[0]
    common.logMessage(logging.INFO, f'reading file {filename}', common.process_user)

    raster = globalsSingleton.id2Raster(name)
    if raster is None:
        raster = loadFile(fullPath, extraMetadataAll)
        if raster is None:
            return None

    collectionJsonDict = raster.toShortDictDefinition()
    common.logMessage(logging.INFO, f'finished file {filename}', common.process_user)
    return collectionJsonDict if collectionJsonDict else None

def _finalizeCollections():
    """
    Finalizes the collection loading process by saving the ID database and adding test rasters.
    """
    globalsSingleton.saveIdDatabase()
    rasters = tr.setTestRasters(5)
    for r in rasters:
        globalsSingleton.raster_database[r['id']] = r
    common.logMessage(logging.INFO, 'finished reading collections', common.process_user)

def openExtraMetadata(extraPath):
    extraMetadataAll = None
    if os.path.exists(extraPath):
        extraMd = open(extraPath)
        extraMetadataAll = json.load(extraMd)
    return extraMetadataAll   
         
def createCollectionJson(product, extraMetadata, fullpath, id=None):
    stac_version = globalsSingleton.openeoip_config['stac_version']
    toplvl_dict = {'stac_version' : stac_version, 
                   'type' : 'Collection', 
                   'id' : product.stac.id, 
                   'title' : product.stac.title,
                   'description' : getExtra('description', extraMetadata, 'None'), 
                   'extent' : createExtentPart(product),
                   'license' : getExtra('license', extraMetadata, 'unknown'),                   
                   'keywords' : getExtra('keywords', extraMetadata, ''),
                   'providers' : getExtra('providers', extraMetadata, 'unknown'),
                   'links' : getExtra('links', extraMetadata, [])
                   }
  
    globalsSingleton.insertFileNameInDatabase(product.stac.id, fullpath)
    
    return toplvl_dict

def createExtentPart(product) :
    bbox = {}
    bbox['bbox'] = [product.stac.bbox]
    time = [str(product.stac.datetime), str(product.stac.datetime)]
    interval = {}
    interval['interval'] = [time]
    ext = {'spatial' : bbox, 'temporal' : interval}

    return ext;  

def getExtra(key, extraMetaData, defValue):
    if extraMetaData == None:
        return defValue

    if key in extraMetaData:
        return extraMetaData[key]
    return defValue




