from openeooperation import *
from operationconstants import *
from constants import constants

# for the moment unclear how to call this operation

class ArrayConcat(OpenEoOperation):
    def __init__(self):
        self.loadOpenEoJsonDef('array_concat.json')
        self.kind = constants.PDPREDEFINED
                        

    def prepare(self, arguments):
        self.runnable = False
        if 'serverChannel' in arguments:
                toServer = arguments['serverChannel']
                job_id = arguments['job_id']

        self.targetRaster = None                
        self.sourceRaster = None 
        self.resampleNeeded = False

        if 'array1' in arguments:
            list1 = arguments['array1']['resolved']
            list2 = arguments['array2']['resolved']
        else:
            self.handleError(toServer, job_id, 'data','Not an array(like) object', 'ProcessParameterInvalid')

        if not (isinstance(list1, list) and isinstance(list2, list)):
             self.handleError(toServer, job_id, 'array1 or array2','Not an array(like) object', 'ProcessParameterInvalid')
        if len(list1) == 0 or len(list2) == 0:
            self.handleError(toServer, job_id, 'data','array must have a size or else the used datatype is unknown', 'ProcessParameterInvalid')

        self.targetBandIndex = 0
        self.rasterCase = isinstance(list2[0], RasterData)
        if self.rasterCase:
            if len(list2[0]['eo:bands']) != 1:
                self.handleError(toServer, job_id, 'source size',"source may only contain  one band", 'ProcessParameterInvalid')

            self.targetRaster = list2[0]
            
            if isinstance(list1[0], RasterData):
                idx = self.args2bandIndex(toServer, job_id, list1, arguments )
                if idx == -1:
                    self.handleError(toServer, job_id, 'band label or index',"label or index can't be found", 'ProcessParameterInvalid')
                self.sourceRaster = list1                
                self.sourceBandIndex = idx
                self.rastersEqualSize = self.checkSpatialDimensions([self.targetRaster, self.sourceRaster[0]])
                rd1  = self.targetRaster['temporalExtent']
                rd2  = self.sourceRaster[0]['temporalExtent']
                if common.temporalOverlap(rd1, rd2):
                   self.handleError(toServer, job_id, 'time values',"time ranges overlap, No append possible", 'ProcessParameterInvalid') 
            else:
                self.handleError(toServer, job_id, 'data types',"data types incompatible", 'ProcessParameterInvalid')  
            self.createExtra(list2, 0)                                                                 

        else:                          
            self.array2 = list2
            self.array1 = list1
                  
        self.runnable = True           

    def run(self,openeojob, processOutput, processInput):
        if self.runnable:
            if self.rasterCase:
                outputRasters = []
                self.logStartOperation(processOutput, openeojob)
                ##for lists and numpy arrays same method
                if self.targetRaster != None and self.sourceRaster != None:
                    targetRaster = self.targetRaster.index2band(self.targetBandIndex)['rasterImplementation']
                    cpTarget = targetRaster.clone()
                    for source in self.sourceRaster:
                        sourceRaster = source.index2band(self.sourceBandIndex)['rasterImplementation']
                        zsize = sourceRaster.size().zsize
                        for idx in range(zsize):
                            iter = sourceRaster.band(idx)
                            cpTarget.addBand(ilwis.constants.rUNDEF, iter)

                outputRasters.extend(self.setOutput([cpTarget], self.extra))
                self.logEndOperation(processOutput,openeojob)
                return createOutput(constants.STATUSFINISHED, [outputRasters], constants.DTRASTER)
            else:
                a = self.array1 + self.array2
                self.logEndOperation(processOutput,openeojob)
                return createOutput(constants.STATUSFINISHED, a, constants.DTLIST)


        return createOutput('error', "operation no runnable", constants.DTERROR )  

def registerOperation():
   return ArrayConcat()                                         
