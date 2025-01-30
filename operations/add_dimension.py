from openeooperation import *
from operationconstants import *
from constants import constants
import common

class AddDimension(OpenEoOperation):
    def __init__(self):
        self.loadOpenEoJsonDef('add_dimension.json')
        self.kind = constants.PDPREDEFINED
                        

    def prepare(self, arguments):
        self.runnable = False

        toServer, job_id = self.getDefaultArgs(arguments) 
        self.logStartPrepareOperation(job_id)                
        dimname = arguments['name']['resolved']
        dimname = self.mapname(dimname)                
        rasters = arguments['data']['resolved']
        for raster in rasters:
            if not isinstance(raster, RasterData):
                self.handleError(toServer, job_id, 'data','data must be a raster', 'ProcessParameterInvalid')
            for fname in raster[DIMORDER]:
                if fname == dimname:
                    self.handleError(toServer, job_id, 'data','A dimension with the specified name already exists', 'ProcessParameterInvalid') 
        self.dimname = dimname 
        self.labels = arguments['label']['resolved']
        self.rasters = rasters
                   
        self.runnable = True
        self.logEndPrepareOperation(job_id)                    

    def run(self,openeojob, processOutput, processInput):
        if self.runnable:
            outData = []
            self.logStartOperation(processOutput, openeojob)
            for raster in self.rasters:
                exrasters = {}                
                if self.dimname == DIMTEMPORALLAYER:
                    p = len(raster[DIMORDER])
                    raster[DIMORDER].insert(p - 1, DIMTEMPORALLAYER)
                else:    
                    raster[DIMORDER].insert(0, self.dimname)
                    for key, value in raster['rasters'].items():
                        exrasters['0:' + key] = value  
                    raster['rasters'] = exrasters
                items = {}
                labels = []                    
                for idx,label in enumerate(self.labels): 
                    layer = RasterLayer() if self.dimname == DIMTEMPORALLAYER else dict()
                    layer[DATASOURCE] = '' # calculated or derived product there is no source
                    layer[TEMPORALEXTENT] = label if self.dimname == DIMTEMPORALLAYER else ''
                    layer[LAYERINDEX] = idx
                    layer['eo:cloud_cover'] = 0
                    labels.append(label)               
                    items[label] = layer  
                refsystem =  'Gregorian calendar / UTC' if self.dimname == DIMTEMPORALLAYER else ''                                                                           
                raster[DIMENSIONSLABEL][self.dimname] =  {'items' :items, 'labels' : labels,'unit' : '' , 'RefSystem': refsystem} 
                
                outData.append(raster)
            self.logEndOperation(processOutput,openeojob)
            return  createOutput(constants.STATUSFINISHED, outData, constants.DTRASTER)
        
        return createOutput('error', "operation no runnable", constants.DTERROR )  

def registerOperation():
   return AddDimension()      