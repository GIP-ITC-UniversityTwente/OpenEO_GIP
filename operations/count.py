from openeooperation import *
from operationconstants import *
from constants import constants
from globals import getOperation
from workflow.processGraph import ProcessGraph
import copy

##unclear how to use this process

class Count(OpenEoOperation):
    def __init__(self):
        self.loadOpenEoJsonDef('count.json')
        self.kind = constants.PDPREDEFINED
                        

    def prepare(self, arguments):
        self.runnable = False
        if 'serverChannel' in arguments:
                toServer = arguments['serverChannel']
                job_id = arguments['job_id']
        self.data = arguments['data']['resolved']
        if not isinstance(self.data, list):
            self.handleError(toServer, job_id, 'Input data',"Input data must be a array/list", 'ProcessParameterInvalid')
        self.condition = arguments['condition']['resolved']
            
        self.runnable = True           

    def run(self,openeojob, processOutput, processInput):
        if self.runnable:
            self.logStartOperation(processOutput, openeojob)
            count = 0
            useGraph = isinstance(self.condition, dict)
            if useGraph:
                copyPg = copy.deepcopy(self.condition)
                matchingName = 'data'
                first = next(iter(copyPg))
                args = copyPg[first]['arguments']
                for item in args.items():
                    if isinstance(item[1], dict):
                        if 'from_parameter' in item[1] and item[1]['from_parameter'] == 'element':
                            matchingName = item[0]
                process = ProcessGraph(copyPg, [], getOperation)                                            

            for i in range(len(self.data)):
                if self.data[i] == None:
                    continue    
                if self.condition != '':
                    if useGraph:
                        process.addLocalArgument(matchingName,  {'base' : '?', 'resolved' :self.data[i]}) 
                        oInfo = process.run(openeojob, processOutput, processInput) 
                        outputs = oInfo['value'] 
                        if not outputs:
                            continue
                    else:
                        if self.data[i] != self.condition:
                            continue
                count = count + 1

            self.logEndOperation(processOutput,openeojob)                      
            return createOutput(constants.STATUSFINISHED, count, DTINTEGER)
        
        return createOutput('error', "operation no runnable", constants.DTERROR )  

def registerOperation():
   return Count()   