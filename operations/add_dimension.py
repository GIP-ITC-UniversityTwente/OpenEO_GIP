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
        if 'serverChannel' in arguments:
                toServer = arguments['serverChannel']
                job_id = arguments['job_id']
        dimname = arguments['name']['resolved']
        dimname = self.mapname(dimname)                
        rasters = arguments['data']['resolved']
        for raster in rasters:
            if not isinstance(raster, RasterData):
                self.handleError(toServer, job_id, 'data','data must be a raster', 'ProcessParameterInvalid')
            for fname in raster[STRUCTUREDEFDIM]:
                if fname == dimname:
                    self.handleError(toServer, job_id, 'data','A dimension with the specified name already exists', 'ProcessParameterInvalid') 
        self.dimname = dimname 
        self.labelname = arguments['label']['resolved']
        self.rasters = rasters
                   
        self.runnable = True           

    def run(self,openeojob, processOutput, processInput):
        if self.runnable:
            outData = []
            self.logStartOperation(processOutput, openeojob)
            for raster in self.rasters:
                raster[STRUCTUREDEFDIM].insert(0, self.dimname)
                raster[METADATDEFDIM][self.dimname] = {}
                exrasters = {}
                for key, value in raster['rasters'].items():
                    exrasters['0:' + key] = value
                raster['rasters'] = exrasters
                outData.append(raster)
            self.logEndOperation(processOutput,openeojob)
            return  createOutput(constants.STATUSFINISHED, outData, constants.DTRASTER)
        
        return createOutput('error', "operation no runnable", constants.DTERROR )  

def registerOperation():
   return AddDimension()      