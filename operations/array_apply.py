from openeooperation import *
from operationconstants import *
from constants import constants
import numpy

# for the moment unclear how to call this operation

class ArrayApply(OpenEoOperation):
    def __init__(self):
        self.loadOpenEoJsonDef('array_apply.json')
        self.kind = constants.PDPREDEFINED
                        

    def prepare(self, arguments):
        self.runnable = False
        toServer, job_id = self.getDefaultArgs(arguments) 
        self.logStartPrepareOperation(job_id)                
        data = arguments['data']['resolved']
        if not isinstance(data, list) and not isinstance(data, numpy.array):
             self.handleError(toServer, job_id, 'data','Not an array(like) object', 'ProcessParameterInvalid')
        self.data = data
        self.value = arguments['process-graph']['resolved'] 
        self.runnable = True           

    def run(self,openeojob, processOutput, processInput):
        if self.runnable:
            self.logStartOperation(processOutput, openeojob)
            ##for lists and numpy arrays same method
            self.logEndOperation(processOutput,openeojob)
            return createOutput(constants.STATUSFINISHED, [self.data], constants.DTARRAY | constants.DTLIST)
        return createOutput('error', "operation no runnable", constants.DTERROR )  


#def registerOperation():
#   return ArrayApply()    