from pathlib import Path
from workflow import processGraph
import os

import importlib
import json
import logging
import openeologging

# this method traverse the 'operations' folder to find all source modules that implement the
# registerOperation method. If so it will call the registerOperation method which will create a 
# class instance for that operation which holds the metadata of an operation. This object is stored 
# with as key the operation id and can be usud for executing the operation (given the correct parameters)
def initOperationMetadata(getOperation):

# Specify the subdirectory path
    operationsMetaData = {}
    openeologging.logMessage(logging.INFO, 'registering operations')
    current_dir = os.path.dirname(os.path.abspath(__file__))
    subfolders = [ f.path for f in os.scandir(current_dir) if f.is_dir() ]
    for folder in subfolders:
        # find all sources in the subfolder which implement the registerOperation method and register them
        operationsMetaData = loadOperationsFolder(folder, operationsMetaData)                   

    # find all sources in the main folder which implement the registerOperation method and register them
    operationsMetaData = loadOperationsFolder(current_dir,operationsMetaData)
    openeologging.logMessage(logging.INFO, 'finished registering operations')
    
    # udfs
    rootLocation = os.path.join(os.path.dirname(__file__), '..')
    rootLocation = os.path.abspath(rootLocation)
    configFileLocation = os.path.join(rootLocation, 'config/config.json')
    configFileLocation = os.path.abspath(configFileLocation)
    if not os.path.exists(configFileLocation):
        openeologging.logMessage(logging.CRITICAL, "missing \'config.json\' file at " + configFileLocation)
        raise Exception("missing \'config.json\' file at " + configFileLocation)
    
    openeoip_configfile = open(configFileLocation)
    openeologging.logMessage(logging.INFO, 'reading udfs')
    openeoip_config = json.load(openeoip_configfile)
    udf_locations = openeoip_config["data_locations"]["udf_locations"]
    for udf_location in udf_locations:
        location = udf_location["location"]
        if not os.path.exists(location):
             openeologging.logMessage(logging.WARNING, 'udf location doesnt exist: ' + location)
             continue
        file_names = [f for f in os.listdir(location) if f.endswith('.udf')]
        for filename in file_names:
            udfpath = os.path.join(location, filename)        
            f = open(udfpath)
            data = json.load(f)
            processValues = data['process']
            wf = processGraph.ProcessGraph(processValues['process_graph'], None, getOperation)
            operationsMetaData[processValues['id']] = wf
    openeologging.logMessage(logging.INFO, 'finished reading udfs')            
    return operationsMetaData

def loadOperationsFolder(folder,operationsMetaData):
    if not os.path.exists(folder):
        openeologging.logMessage(logging.WARNING, 'operation location doesnt exist: ' + folder)
        return operationsMetaData  
    
    file_names = [f for f in os.listdir(folder) if f.endswith('.py')]
    if len(file_names) != 0:
        parts = folder.split(os.sep)
        foldername = parts[-1]
        subdirectory = foldername
        for filename in file_names:
            module_name = filename[:-3]

                # Import the module dynamically
            try:
                module = importlib.import_module(f'{subdirectory}.{module_name}', package=__package__)
            
                if hasattr(module, 'registerOperation'): 
                    opObject = module.registerOperation()
                    if isinstance(opObject, list):
                        for func in opObject:
                            operationsMetaData[func.name] = func 
                    else:                   
                        operationsMetaData[opObject.name] = opObject
            except Exception as ex:
                continue


    return operationsMetaData