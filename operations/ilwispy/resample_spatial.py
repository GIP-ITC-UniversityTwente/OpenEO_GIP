from openeooperation import *
from operationconstants import *
from constants import constants


import openeo


class ResampleSpatial(OpenEoOperation):
    def __init__(self):
        self.loadOpenEoJsonDef('resample_spatial.json')
        
        self.kind = constants.PDPREDEFINED

    def prepare(self, arguments):
        self.runnable = False
        toServer, job_id = self.getDefaultArgs(arguments) 
        self.logStartPrepareOperation(job_id)         

        method = arguments['method']['resolved']
        if method == 'near':
            self.method = 'nearestneighbour'
        elif method == 'cubic':
            self.method = 'bicubic'
        elif method == 'bilinear':
            self.method = 'bilinear'
        else:
            self.handleError(toServer, job_id, 'method','unsupported interpolation method: ' + method, 'ProcessParameterInvalid')
        
        pixelSize  = arguments['resolution']['resolved']
        if pixelSize < 0:
            self.handleError(toServer, job_id, 'resolution','resolution must be zero or greater', 'ProcessParameterInvalid')

        data = arguments['data']['resolved']
        for r in data:
            if r.isValid():
                if type(r) is RasterData:
                    self.extra = self.constructExtraParams(r, r[TEMPORALEXTENT], 0)                 
                    self.inputRaster = r.getRaster()                  
                    pixSize = r.getRaster().geoReference().pixelSize()
                    if pixSize == 0:
                        self.pixelSize = pixSize
                    else:
                        self.pixelSize = pixelSize 
                else:
                    self.handleError(toServer, job_id, 'Input Raster', 'invalid input. rasters are not valid', 'ProcessParameterInvalid')
           

        projection = arguments['projection']['resolved']
      

        if not isinstance(projection, int):
            self.handleError(toServer, job_id, 'projection', 'only epsg numbers allowed as projection definition', 'ProcessParameterInvalid')
        
        self.csy = ilwis.CoordinateSystem('epsg:' + str(projection))
        if bool(self.csy) == False:
            self.handleError(toServer, job_id, 'projection', 'Coordinate system invalid in resample_spatial', 'ProcessParameterInvalid')
        setWorkingCatalog(self.inputRaster, self.name) 
        self.runnable = True
        self.logEndPrepareOperation(job_id) 

              

    def run(self,openeojob , processOutput, processInput):
        if self.runnable:
            self.logStartOperation(processOutput, openeojob)
            ##put2Queue(processOutput, {'progress' : 0, 'job_id' : openeojob.job_id, 'status' : 'running'})
            
            env : ilwis.Envelope = self.inputRaster.envelope()
            grf = ilwis.do('createcornersgeoreference', \
                           env.minCorner().x, env.minCorner().y, env.maxCorner().x, env.maxCorner().y, \
                           self.pixelSize, self.csy, True, '.')

            outputRc = ilwis.do("resample", self.inputRaster, grf, self.method)
            outputRasters = [] 
            common.registerIlwisIds(outputRc)                 
            outputRasters.extend(self.setOutput([outputRc], self.extra))
            ##put2Queue(processOutput,{'progress' : 100, 'job_id' : openeojob.job_id, 'status' : 'finished'}) 
            self.logEndOperation(processOutput,openeojob, outputs=outputRasters)
            return createOutput(constants.STATUSFINISHED, outputRasters, constants.DTRASTER)  
        message = common.notRunnableError(self.name, openeojob.job_id) 
        return createOutput('error', message, constants.DTERROR)
        
def registerOperation():
     return ResampleSpatial()