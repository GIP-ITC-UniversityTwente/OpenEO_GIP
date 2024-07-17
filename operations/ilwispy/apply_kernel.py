from openeooperation import *
from operationconstants import *
from constants import constants



class ApplyKernel(OpenEoOperation):
    def __init__(self):
        self.loadOpenEoJsonDef('apply_kernel.json')
        
        self.kind = constants.PDPREDEFINED

    def prepare(self, arguments):
        self.runnable = True
     
        if 'serverChannel' in arguments:
            toServer = arguments['serverChannel']
            job_id = arguments['job_id']
        self.data = arguments['data']['resolved']
        self.kernel = arguments['kernel']['resolved']
        self.replaceInvalid = arguments['replace_invalid']['resolved']
        self.factor = arguments['factor']['resolved']
        self.border = arguments['border']['resolved']
        for rd in self.data:
            if not isinstance(rd, RasterData):
                self.handleError(toServer, job_id, "data", "All data must be rasterdata", 'ProcessParameterInvalid')
        if len(self.kernel) % 2 != 1:
            self.handleError(toServer, job_id, "kernel", "Kernel column size must be odd", 'ProcessParameterInvalid')
        self.lenRows = -1
        self.lenColumns = len(self.kernel)
        for arr in self.kernel:
            lr = len(arr)
            if lr % 2 != 1:
                self.handleError(toServer, job_id, "kernel", "Kernel row size must be odd", 'ProcessParameterInvalid')
            if self.lenRows == -1:
                self.lenRows = lr
            else:
                if self.lenRows != lr:
                  self.handleError(toServer, job_id, "kernel", "Kernel row sizes must be equal", 'ProcessParameterInvalid')                  
        self.rasterSizesEqual = True
        self.createExtra(self.data[0], False) 
             


    def run(self,openeojob, processOutput, processInput):
        if self.runnable:
            self.logStartOperation(processOutput, openeojob)

            values = []
            for yarr in self.kernel:
    	            for v in yarr:
                        values.append(v)

            numbers = str(values)[1:-1]
            numbers = numbers.replace(", ", " ")
            code = "code=" + str(self.lenColumns) + "," + str(self.lenRows) + "," + numbers + "," + str(self.factor) + "," + str(self.border)
            outputRasters = []
            for rd in self.data:
                ilwRasters = []
                for ilwRaster in rd[constants.DATAIMPLEMENTATION].values():
                    rc = ilwis.do("linearrasterfilter", ilwRaster, code)
                    ilwRasters.append(rc)
                common.registerIlwisIds(ilwRasters)                       
                outputRasters.extend(self.setOutput(ilwRasters, self.extra))

            self.logEndOperation(processOutput,openeojob, outputRasters)                      
            return createOutput(constants.STATUSFINISHED, outputRasters, constants.DTRASTER)
        
        return createOutput('error', "operation no runnable", constants.DTERROR)
        
def registerOperation():
     return ApplyKernel()