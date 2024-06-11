from openeooperation import *
from operationconstants import *
from constants import constants
from rasterdata import RasterData
import ilwis
import numpy

class BaseAggregateData(OpenEoOperation):
    def base_prepareRaster(self, arguments):
            self.runnable = False 
            if 'serverChannel' in arguments:
                toServer = arguments['serverChannel']
                job_id = arguments['job_id']  

            inpData = arguments['data']['resolved']
            if len(inpData) == 0:
                self.handleError(toServer, job_id, 'Input raster','invalid input. Number of rasters is 0', 'ProcessParameterInvalid')
            if isinstance(inpData[0], RasterData):
                self.rasters = inpData

                for rc in self.rasters:
                    if not rc:
                        self.handleError(toServer, job_id, 'Input raster','invalid input. rasters are not valid', 'ProcessParameterInvalid')
                    for raster in rc['rasters'].values():
                        if raster.datadef().domain().ilwisType() != ilwis.it.NUMERICDOMAIN:
                            self.handleError(toServer, job_id, 'Input raster', 'invalid datatype in raster. Must be numeric', 'ProcessParameterInvalid')
    
            elif isinstance(inpData ,list):
                self.array = inpData
    
    def base_run(self,openeojob, processOutput, processInput):
        if self.runnable:
            self.logStartOperation(processOutput, openeojob)
            if hasattr(self, 'rasters'):
                outputRasters = []
                for rc in self.rasters:
                    self.createExtra(rc, True)
                    for ilwRaster in rc['rasters'].values():
                        outputRc = ilwis.do("aggregaterasterstatistics", ilwRaster,self.method)

                        outputRasters.extend(self.makeOutput([outputRc], self.extra))

                self.logEndOperation(processOutput,openeojob)
                return createOutput(constants.STATUSFINISHED, outputRasters, constants.DTRASTER)
            elif hasattr(self, 'array'):
                result = self.aggFunc(self.array)
                self.logEndOperation(processOutput,openeojob)
                return createOutput(constants.STATUSFINISHED, result, self.type2type(result))

        message = common.notRunnableError(self.name, openeojob.job_id) 
        return createOutput('error', message, constants.DTERROR)      