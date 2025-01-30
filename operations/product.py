from openeooperation import *
from operationconstants import *
from constants import constants

# for the moment unclear how to call this operation

class ArrayProduct(OpenEoOperation):
    def __init__(self):
        self.loadOpenEoJsonDef('product.json')
        self.kind = constants.PDPREDEFINED
                        

    def prepare(self, arguments):
        self.runnable = False
        toServer, job_id = self.getDefaultArgs(arguments) 
        self.logStartPrepareOperation(job_id)    

        self.array = arguments['data']['resolved']
        self.ignore_nodata = True
        if 'ignore_nodata' in arguments:
            self.ignore_nodata = arguments['ignore_nodata']['resolved']
     
        self.runnable = True 
        self.logEndPrepareOperation(job_id)                    

    def run(self,openeojob, processOutput, processInput):
        if self.runnable:
            self.logStartOperation(processOutput, openeojob)
            result = None
            for v in self.array:
                            
                if v == None and self.ignore_nodata:
                    continue
                if v == None:
                    result = None
                    break
                elif result == None:
                    result = v 
                    result = result * v                            

                self.logEndOperation(processOutput,openeojob)
                return createOutput(constants.STATUSFINISHED, result, self.type2type(result))



        return createOutput('error', "operation no runnable", constants.DTERROR )  

def registerOperation():
   return ArrayProduct()                                         
