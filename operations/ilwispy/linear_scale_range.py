from openeooperation import *
from operationconstants import *
from constants import constants
from common import openeoip_config


class LinearScaleRangeOperation(OpenEoOperation):
    def __init__(self):
        self.loadOpenEoJsonDef('linear_scale_range.json')
        
        self.kind = constants.PDPREDEFINED

    def prepare(self, arguments):
        self.runnable = True
        toServer, job_id = self.getDefaultArgs(arguments) 
        self.logStartPrepareOperation(job_id) 

        self.rasterSizesEqual = True
        self.inpMax = arguments['inputMax']['resolved']
        self.inpMin = arguments['inputMin']['resolved']
        self.outMax = arguments['outputMax']['resolved']
        self.outMin = arguments['outputMin']['resolved']

        rasters = arguments['x']['resolved']

        if not isinstance(rasters, list):
                self.handleError(toServer, job_id, 'Input raster','invalid input. rasters are not a list', 'ProcessParameterInvalid')
        if len(rasters) == 0:
            self.handleError(toServer, job_id, 'Input raster','invalid input. rasters are zero length', 'ProcessParameterInvalid')
        self.inputRasters = []
        setWorkingCatalog( rasters[0], self.name)
        self.createExtra(rasters[0]) 
        for raster in rasters:
            if not isinstance(raster, DataCube):
                self.handleError(toServer, job_id, 'Input raster','invalid input. rasters are not valid', 'ProcessParameterInvalid')
            if raster.getRaster().datadef().domain().ilwisType() != ilwis.it.NUMERICDOMAIN:
                self.handleError(toServer, job_id, 'Input raster','invalid datatype in raster. Must be numeric', 'ProcessParameterInvalid')
            ilwRasters = raster.getRasters()                
            for ilwRaster in ilwRasters:
                self.rasterSizesEqual = ilwRaster.size() == ilwRasters[0].size()
                if not self.rasterSizesEqual:
                    break
            self.inputRasters.extend(ilwRasters)
        if self.rasterSizesEqual:
            self.extra['bands'] =  [{'type' : 'float', BANDINDEX : 0, 'name' : 'combined','details' : {} }] 
        self.logEndPrepareOperation(job_id) 
              

    def run(self,openeojob, processOutput, processInput):
        if self.runnable:
            self.logStartOperation(processOutput, openeojob)
            outputRasters = [] 
            outputRcs = []  
            for raster in self.inputRasters:
                outputRcs.append(ilwis.do("linearstretch", raster,self.inpMin, self.inpMax, self.outMin, self.outMax))
            common.registerIlwisIds(openeojob.job_id, outputRcs)               
            outputRasters.extend(self.setOutput(openeojob.job_id,outputRcs, self.extra))
            return createOutput(constants.STATUSFINISHED, outputRasters, constants.DTRASTERLIST)
        
        return createOutput('error', "operation no runnable", constants.DTERROR)
        
def registerOperation():
     return LinearScaleRangeOperation()