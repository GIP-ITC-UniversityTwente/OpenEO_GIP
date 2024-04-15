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
                    if rc.getRaster().dataType() != ilwis.it.NUMERICDOMAIN:
                       self.handleError(toServer, job_id, 'Input raster', 'invalid datatype in raster. Must be numeric', 'ProcessParameterInvalid')
    
                self.rasterSizesEqual = self.checkSpatialDimensions(self.rasters)  
                self.method = 'unknown'
            elif type(arguments['data']) is numpy.array: ## will this work, ftm no testable case
                self.array = arguments['data']
                self.aggFunc = numpy.mean
            
     
    def base_run(self,openeojob, processOutput, processInput):
        if self.runnable:
            self.logStartOperation(processOutput, openeojob)
            if hasattr(self, 'rasters'):
                outputRasters = []
                for rc in self.rasters:
                    raster = rc.getRaster()
                    outputRc = ilwis.do("aggregaterasterstatistics", raster,self.method)
                    extra = self.constructExtraParams(rc, rc['temporalExtent'], 0)
                    outputRasters.extend(self.setOutput([outputRc], extra))

                self.logEndOperation(processOutput,openeojob)
                return createOutput(constants.STATUSFINISHED, outputRasters, constants.DTRASTER)
        message = common.notRunnableError(self.name, openeojob.job_id) 
        return createOutput('error', message, constants.DTERROR)      