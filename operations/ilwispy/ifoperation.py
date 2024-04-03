from openeooperation import *
from operationconstants import *
from constants import constants

class IfOperation(OpenEoOperation):
        def __init__(self):
                self.loadOpenEoJsonDef('if.json')
                
                self.kind = constants.PDPREDEFINED

        def checkData(self, data):

                if type(data) is RasterData:
                        extra = self.constructExtraParams(data, data.temporalExtent, 0)
                        raster = data.getRaster().rasterImp()
                        return {'raster' : raster, 'extra' : extra}

        def checkArgument(self, idata):
                parmValue = None
                if isinstance(idata, tuple):
                        data = list(idata)
                if isinstance(data, list):
                        dataList = []
                        for data in p1:
                                dataList.append(self.checkData(data)  )
                        parmValue = dataList
                else:
                        parmValue = self.checkData(data) 

                return parmValue                                       

        def prepare(self, arguments):
                ##TODO
                self.runnable = False
                if 'serverChannel' in arguments:
                        toServer = arguments['serverChannel']
                        job_id = arguments['job_id']
                condition = arguments['value']['resolved']
                p1 = arguments['accept']['resolved']
                p2 = arguments['reject']['resolved']
                self.parmValue1 = self.checkArgument(p1)
                self.parmValue2 = self.checkArgument(p2)
        

        def run(self,openeojob, processOutput, processInput):
                if self.runnable:
                        result = False
                        if isinstance(self.parmValue1, list):
                                if len(self.parmValue1) == len(self.parmValue2):
                                        iter2 = iter(self.parmValue2)
                                        for data in self.parmValue1:
                                                if data != next(iter2):
                                                        result = False
                                                        break       
                        return result
        
                return createOutput('error', "operation no runnable", constants.DTERROR)  

def registerOperation():
     return IfOperation()    