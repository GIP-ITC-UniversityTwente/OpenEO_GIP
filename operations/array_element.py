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
        self.inputRasters = None

        inpData = arguments['data']['resolved'] 
        if not isinstance(inpData, list) and len(inpData > 0):
             self.handleError(toServer, job_id, 'Input raster','Invalid input list', 'ProcessParameterInvalid')  
        self.rasterCase = isinstance(inpData[0], RasterData)
        if self.rasterCase:
            self.inputRasters = inpData
            if self.inputRasters == None:
                self.handleError(toServer, job_id, 'Input raster','No input raster found', 'ProcessParameterInvalid')
            self.bandIndex = -1
            if isinstance(inpData[0], RasterData):
                idx = self.args2bandIndex(toServer, job_id, inpData, arguments )
                if idx == -1:
                    self.handleError(toServer, job_id, 'band label or index',"label or index can't be found", 'ProcessParameterInvalid')
                self.inputRasters = inpData                
                self.bandIndex = idx  
            

        else:
            self.array1 = inpData
            self.index = arguments['index']['resolved'] 
            if self.index >= len(self.array1):
                self.handleError(toServer, job_id, 'index',"greater than length list/array", 'ProcessParameterInvalid') 
        
        self.runnable = True

   
    def run(self,openeojob, processOutput, processInput):
        if self.runnable:
            self.logStartOperation(processOutput, openeojob)
            if self.rasterCase:
                outputRaster = self.inputRasters[self.bandIndex]
                self.logEndOperation(processOutput,openeojob)
                return createOutput(constants.STATUSFINISHED, [outputRaster], constants.DTRASTER)
            else:
                a = self.array1[self.index]
                self.logEndOperation(processOutput,openeojob)
                return createOutput(constants.STATUSFINISHED, a, self.type2type(a))
        
        return createOutput('error', "operation not runnable", constants.DTERROR)
           
def registerOperation():
     return ArrayElementOperation()