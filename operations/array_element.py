from openeooperation import *
from operationconstants import *
from constants import constants
from rasterdata import *
from common import getRasterDataSets
from pathlib import Path

class ArrayElementOperation(OpenEoOperation):
    def __init__(self):
        self.loadOpenEoJsonDef('array_element.json')
        
      
        self.kind = constants.PDPREDEFINED

    def prepare(self, arguments):
        self.runnable = False
        if 'serverChannel' in arguments:
            toServer = arguments['serverChannel']
            job_id = arguments['job_id']
            
        self.inputRasters = arguments['data']['resolved'] 
        if self.inputRasters == None:
            self.handleError(toServer, job_id, 'Input raster','No input raster found', 'ProcessParameterInvalid')

        if 'index' in arguments:
            self.bandIndex = arguments['index']['resolved']  
            if len(self.inputRasters) <= self.bandIndex:
                self.handleError(toServer, job_id, 'band index',"Number of raster bands doesnt match given index", 'ProcessParameterInvalid')
        if 'label' in arguments:
            self.bandIndex = -1
            for idx in range(len(self.inputRasters)):
                for item in self.inputRasters[idx].bands:
                    if item['name'] == arguments['label']['resolved']:
                        self.bandIndex = idx
                        break
            if self.bandIndex == -1:
                self.handleError(toServer, job_id, 'band label',"label can't be found", 'ProcessParameterInvalid')
        self.runnable = True

   
    def run(self,openeojob, processOutput, processInput):
        if self.runnable:
            outputRaster = self.inputRasters[self.bandIndex]
            return createOutput(constants.STATUSFINISHED, [outputRaster], constants.DTRASTER)
        
        return createOutput('error', "operation not runnable", constants.DTERROR)
           
def registerOperation():
     return ArrayElementOperation()