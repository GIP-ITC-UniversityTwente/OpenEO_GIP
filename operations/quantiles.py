from openeooperation import *
from operationconstants import *
from constants import constants
import numpy
import math

class Quantiles(OpenEoOperation):
    def __init__(self):
        self.loadOpenEoJsonDef('quantiles.json')
        self.kind = constants.PDPREDEFINED
                        

    def prepare(self, arguments):
        self.runnable = False
        if 'serverChannel' in arguments:
            toServer = arguments['serverChannel']
            job_id = arguments['job_id']

        self.array = self.getMandatoryParam(toServer, job_id, arguments, 'data')
        self.probabilities = self.getMandatoryParam(toServer, job_id, arguments,'probabilities')
        checkN = -1000
        if isinstance(self.probabilities, list):
            for v in self.probabilities:
                if v > 1 or v < 0:
                    self.handleError(toServer, job_id, "probabilities", "list must be and ordered list between 0 and 1",'ProcessParameterInvalid')                                
                if checkN == -1000:
                    checkN = v
                else:
                    if v <= checkN:
                        self.handleError(toServer, job_id, "probabilities", "list must be and ordered list between 0 and 1",'ProcessParameterInvalid')                

        self.ignore_nodata = True
        if 'ignore_nodata' in arguments:
            self.ignore_nodata = arguments['ignore_nodata']['resolved']
     
        self.runnable = True           

    def run(self,openeojob, processOutput, processInput):
        if self.runnable:
            self.logStartOperation(processOutput, openeojob)
            arr = []
            undefFound = False
            for v in self.array:
                if v == None:
                    v = math.nan
                    undefFound = True
                arr.append(v) 
            if undefFound and self.ignore_nodata == False:                                   
                result = [None] * (self.probabilities - 1)
            else:                
                if isinstance(self.probabilities, list):                        
                    arr = numpy.nanquantile(arr, self.probabilities) 
                    if math.isnan(arr):
                        result = [None] * len(self.probabilities)
                    else:                        
                        result = arr.toList()     
                else:
                    quantiles = numpy.linspace(0, 1, self.probabilities + 1)
                    arr = numpy.nanquantile(arr, quantiles)  
                    result = []
                    for idx in range(1, len(arr) - 1):
                        result.append(arr[idx])                               
                                        

            self.logEndOperation(processOutput,openeojob)
            return createOutput(constants.STATUSFINISHED, result, self.type2type(result))



        return createOutput('error', "operation no runnable", constants.DTERROR )  

def registerOperation():
   return Quantiles()                                         
