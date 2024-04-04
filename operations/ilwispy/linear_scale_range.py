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
        if 'serverChannel' in arguments:
            toServer = arguments['serverChannel']
            job_id = arguments['job_id']

        self.rasterSizesEqual = True
        self.inpMax = arguments['inputMax']['resolved']
        self.inpMin = arguments['inputMin']['resolved']
        self.outMax = arguments['outputMax']['resolved']
        self.outMin = arguments['outputMin']['resolved']

        last_key = list(arguments)[-1]
        raster = arguments[last_key]['resolved']
        if not isinstance(raster, RasterData):
            self.handleError(toServer, job_id, 'Input raster','invalid input. rasters are not valid', 'ProcessParameterInvalid')

        if raster.getRaster().datadef().domain().ilwisType() != ilwis.it.NUMERICDOMAIN:
            self.handleError(toServer, job_id, 'Input raster','invalid datatype in raster. Must be numeric', 'ProcessParameterInvalid')
        setWorkingCatalog( raster)
        self.inputRaster = raster.getRaster()
        self.createExtra(raster, 0) 
        return ""
              

    def run(self,openeojob, processOutput, processInput):
        if self.runnable:
            self.logStartOperation(processOutput, openeojob)

            outputRc = ilwis.do("linearstretch", self.inputRaster,self.inpMin, self.inpMax, self.outMin, self.outMax)
            outputRasters = []   
            common.registerIlwisIds([outputRc])               
            outputRasters.extend(self.setOutput([outputRc], self.extra))
            return createOutput(constants.STATUSFINISHED, outputRasters, constants.DTRASTERLIST)
        
        return createOutput('error', "operation no runnable", constants.DTERROR)
        
def registerOperation():
     return LinearScaleRangeOperation()