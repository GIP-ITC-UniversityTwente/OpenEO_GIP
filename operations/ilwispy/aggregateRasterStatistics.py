from constants import constants
from operations.ilwispy.BaseAggregatestats import BaseAggregateData
import numpy

class MaxOperation(BaseAggregateData):
    def __init__(self):
        self.loadOpenEoJsonDef('max.json')
      
        self.kind = constants.PDPREDEFINED

    def prepare(self, arguments):
        try:
            toServer, job_id = self.getDefaultArgs(arguments) 
            self.logStartPrepareOperation(job_id) 
            it = iter(arguments)
            p1 = arguments[next(it)]['resolved']
            self.method = 'max'
            if isinstance(p1, list):
                self.base_prepareRaster(arguments)
                self.method = 'max'
                self.aggFunc = numpy.max
            self.runnable = True
            self.logEndPrepareOperation(job_id) 
        except Exception as ex:
            return ""

        return ""

    def run(self,openeojob, processOutput, processInput):
        return self.base_run(openeojob, processOutput, processInput)
    
class MeanOperation(BaseAggregateData):
    def __init__(self):
        self.loadOpenEoJsonDef('mean.json')
      
        self.kind = constants.PDPREDEFINED

    def prepare(self, arguments):
        try:
            toServer, job_id = self.getDefaultArgs(arguments) 
            self.logStartPrepareOperation(job_id) 
            self.method = 'mean'
            self.aggFunc = numpy.mean            
            self.base_prepareRaster(arguments)
 
            self.runnable = True
            self.logEndPrepareOperation(job_id)  

        except Exception as ex:
            return ""

        return ""

    def run(self,openeojob, processOutput, processInput):
        return self.base_run(openeojob, processOutput, processInput) 

class MedianOperation(BaseAggregateData):
    def __init__(self):
        self.loadOpenEoJsonDef('median.json')
      
        self.kind = constants.PDPREDEFINED

    def prepare(self, arguments):
        toServer, job_id = self.getDefaultArgs(arguments) 
        self.logStartPrepareOperation(job_id)  
        self.method = 'median' 
        self.aggFunc = numpy.median       
        self.base_prepareRaster(arguments)
    
        self.runnable = True
        self.logEndPrepareOperation(job_id) 


    def run(self,openeojob, processOutput, processInput):
        return self.base_run(openeojob, processOutput, processInput) 

class MinOperation(BaseAggregateData):
    def __init__(self):
        self.loadOpenEoJsonDef('min.json')
      
        self.kind = constants.PDPREDEFINED

    def prepare(self, arguments):
        try:
            toServer, job_id = self.getDefaultArgs(arguments) 
            self.logStartPrepareOperation(job_id)             
            self.method = 'min'
            self.aggFunc = numpy.min            
            self.base_prepareRaster(arguments)
 
            self.runnable = True
            self.logEndPrepareOperation(job_id) 

        except Exception as ex:
            return ""

        return ""

    def run(self,openeojob, processOutput, processInput):
        return self.base_run(openeojob, processOutput, processInput) 

class SumOperation(BaseAggregateData):
    def __init__(self):
        self.loadOpenEoJsonDef('sum.json')
      
        self.kind = constants.PDPREDEFINED

    def prepare(self, arguments):
        try:
            toServer, job_id = self.getDefaultArgs(arguments) 
            self.logStartPrepareOperation(job_id)             
            self.method = 'sum'
            self.aggFunc = numpy.sum
            self.base_prepareRaster(arguments)
   
            self.runnable = True
            self.logEndPrepareOperation(job_id)             

        except Exception as ex:
            return ""

        return ""

    def run(self,openeojob, processOutput, processInput):
        return self.base_run(openeojob, processOutput, processInput)             
class VarianceOperation(BaseAggregateData):
    def __init__(self):
        self.loadOpenEoJsonDef('variance.json')
      
        self.kind = constants.PDPREDEFINED

    def prepare(self, arguments):
        try:
            toServer, job_id = self.getDefaultArgs(arguments) 
            self.logStartPrepareOperation(job_id) 
            self.method = 'variance'
            self.aggFunc = numpy.var            
            self.base_prepareRaster(arguments)
            self.runnable = True
            self.logEndPrepareOperation(job_id) 

        except Exception as ex:
            return ""

        return ""

    def run(self,openeojob, processOutput, processInput):
        return self.base_run(openeojob, processOutput, processInput)
    
class StandardDevOperation(BaseAggregateData):
    def __init__(self):
        self.loadOpenEoJsonDef('sd.json')
      
        self.kind = constants.PDPREDEFINED

    def prepare(self, arguments):
        try:
            toServer, job_id = self.getDefaultArgs(arguments) 
            self.logStartPrepareOperation(job_id)               
            self.method = 'standarddev'
            self.aggFunc = numpy.std
            self.base_prepareRaster(arguments)
            self.runnable = True
            self.logEndPrepareOperation(job_id) 

        except Exception as ex:
            return ""

        return ""

    def run(self,openeojob, processOutput, processInput):
        return self.base_run(openeojob, processOutput, processInput)    
    
def registerOperation():
    funcs = []     
    funcs.append(MaxOperation())
    funcs.append(MeanOperation())
    funcs.append(MedianOperation()) 
    funcs.append(MinOperation())           
    funcs.append(SumOperation()) 
    funcs.append(VarianceOperation())           
    funcs.append(StandardDevOperation())           


    return funcs