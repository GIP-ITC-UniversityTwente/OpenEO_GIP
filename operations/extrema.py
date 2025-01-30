from openeooperation import *
from operationconstants import *
from constants import constants
import numpy
import math

class Extrema(OpenEoOperation):
    def __init__(self):
        self.loadOpenEoJsonDef('extrema.json')
        self.kind = constants.PDPREDEFINED
                        

    def prepare(self, arguments):
        self.runnable = False
        toServer, job_id = self.getDefaultArgs(arguments) 
        self.logStartPrepareOperation(job_id)                  
        self.data = arguments['data']['resolved']
        self.ignorenodata = arguments['ignore_nodata']['resolved']
        if not isinstance(self.data, list):
            self.handleError(toServer, job_id, 'Input data',"Input data must be a array/list", 'ProcessParameterInvalid')
        self.runnable = True
        self.logEndPrepareOperation(job_id)            

    def run(self,openeojob, processOutput, processInput):
        if self.runnable:
            self.logStartOperation(processOutput, openeojob)
            vmin = constants.UNDEFNUMBER
            vmax = -constants.UNDEFNUMBER
            for v in self.data:
                if v == None or math.isnan(v) or (v == constants.UNDEFNUMBER and self.ignorenodata):
                    continue
                if v > vmax:
                    vmax = v
                if v < vmin:
                    vmin = v                        
            self.logEndOperation(processOutput,openeojob)                      
            return createOutput(constants.STATUSFINISHED, str([vmin, vmax]), constants.DTLIST)
        
        return createOutput('error', "operation no runnable", constants.DTERROR )  

def registerOperation():
   return Extrema()   