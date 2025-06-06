from openeooperation import *
from operationconstants import *
from constants import constants
import math
from datacube import DataCube
import openeologging

class BaseUnarymapCalc(OpenEoOperation):
    def base_prepare(self, arguments, oper):
        self.runnable = False
        self.rasterSizesEqual = True
                
        toServer, job_id = self.getDefaultArgs(arguments) 
        self.logStartPrepareOperation(job_id) 
      
        p1 = self.getMandatoryParam(toServer, job_id, arguments, 'x|p')
        if isinstance(p1, list):
            rasterList = []
            for ras in p1:
                if type(ras) is DataCube:
                    setWorkingCatalog(ras, self.name)
                    self.createExtra(ras,False, self.name)
                    raster = ras.getRaster()
                    rasterList.append(raster)
            self.rasters = rasterList                        
                    
        else:
            if math.isnan(p1):
                self.handleError(toServer, job_id, 'Input data',"the parameter a is not a number", 'ProcessParameterInvalid')
            self.rasters = p1                        
        self.operation = oper
        self.dimension = self.getOptionalParam(toServer, job_id, arguments, 'dimension')
        if self.operation in ['pow']:
            self.operation = 'power'
        self.runnable = True 
        self.logEndPrepareOperation(job_id)                                              
                

    def base_run(self,openeojob, processOutput, processInput):
        if self.runnable:
            self.logStartOperation(processOutput, openeojob)
            ##put2Queue(processOutput, {'progress' : 0, 'job_id' : openeojob.job_id, 'status' : 'running'})
            if isinstance(self.rasters, list):
                outputRasters = []   
                ilwRasters = []                              
                for raster in self.rasters:
                    oper = self.operation + '(@1)'
                    outputRc = ilwis.do('mapcalc', oper, raster)
                    ilwRasters.append(outputRc)
                common.registerIlwisIds(openeojob.job_id, ilwRasters)                      
                outputRasters.extend(self.setOutput(openeojob.job_id, ilwRasters, self.extra))
                out =  createOutput(constants.STATUSFINISHED, outputRasters, constants.DTRASTER)
                self.logEndOperation(processOutput,openeojob,outputRasters)                
            else:
                c = eval('math.' + self.operation + '(' + self.rasters + ')')
                out = createOutput(constants.STATUSFINISHED, c, constants.DTNUMBER)
                self.logEndOperation(processOutput,openeojob)
            ##put2Queue(processOutput,{'progress' : 100, 'job_id' : openeojob.job_id, 'status' : 'finished'}) 
            return out
        message = openeologging.notRunnableError(self.name, openeojob.job_id)   
        return createOutput('error', message, constants.DTERROR)
    
class BaseBinarymapCalcBase(OpenEoOperation):
    def base_prepare(self, arguments, oper):
        toServer, job_id = self.getDefaultArgs(arguments) 
        self.logStartPrepareOperation(job_id)         
        self.runnable = False
        self.rasterSizesEqual = True
        if 'serverChannel' in arguments:
            toServer = arguments['serverChannel']
            job_id = arguments['job_id']

        if len(arguments) != 4: ##x,y,serverchannel, job_id
            self.handleError(toServer,job_id, 'band math', "number of parameters is not correct in operation", 'ProcessParameterInvalid')
        it = iter(arguments)
        self.p1 = arguments[next(it)]['resolved']
        self.p2 = arguments[next(it)]['resolved']
        self.ismaps1 = isinstance(self.p1, list)
        self.ismaps2 = isinstance(self.p2, list)
        if self.ismaps1:
            self.rasters1 = self.p1[0]
            self.createExtra(self.rasters1, basename=self.name) 
        if self.ismaps2:            
            self.rasters2 = self.p2[0]
            self.createExtra(self.rasters2, basename=self.name) 

        if not self.ismaps1: 
            if math.isnan(self.p1):
                self.handleError(toServer,job_id, 'band math', "the parameter a is not a number in operation", 'ProcessParameterInvalid')
        if not self.ismaps2:
            if math.isnan(self.p2):
                self.handleError(toServer,job_id, 'band math', "the parameter a is not a number in operation", 'ProcessParameterInvalid')
        
        self.runnable = True
        self.operation = oper
        self.logEndPrepareOperation(job_id)                            
            
        
    def base_prepare2(self, arguments, oper):
        try:
            self.runnable = False
            self.rasterSizesEqual = True
            if 'serverChannel' in arguments:
                toServer = arguments['serverChannel']
                job_id = arguments['job_id']

            if len(arguments) != 4: ##x,y,serverchannel, job_id
                self.handleError(toServer,job_id, 'band math', "number of parameters is not correct in operation", 'ProcessParameterInvalid')
            it = iter(arguments)
            self.p1 = arguments[next(it)]['resolved']
            self.p2 = arguments[next(it)]['resolved']
            self.ismaps1 = isinstance(self.p1, list)## maps are always in lists; numbers not
            self.ismaps2 = isinstance(self.p2, list)

            if not self.ismaps1: 
                if math.isnan(self.p1):
                    self.handleError(toServer,job_id, 'band math', "the parameter a is not a number in operation", 'ProcessParameterInvalid')
            if not self.ismaps2:
                if math.isnan(self.p2):
                    self.handleError(toServer,job_id, 'band math', "the parameter a is not a number in operation", 'ProcessParameterInvalid')
            self.runnable = True

            if self.ismaps1:
                self.rasters1 = self.extractRasters(self.p1)
            if self.ismaps2 :
                self.rasters2 = self.extractRasters(self.p2)

            self.operation = oper                           
                
        except Exception as ex:
            return ""
   
    def base_run(self,openeojob, processOutput, processInput):
           if self.runnable:
            self.logStartOperation(processOutput, openeojob)

            outputRasters = [] 
            oper = '@1' + self.operation + '@2' 
            outputs = []                               
            if self.ismaps1 and self.ismaps2:
                rasters1 = list(self.rasters1.getRasters())
                rasters2 = list(self.rasters2.getRasters())
                for idx in len(rasters1):
                    outputRc = ilwis.do("mapcalc", oper, rasters1[idx],rasters2[idx])
                    outputs.append(outputRc)
            elif self.ismaps1 and not self.ismaps2:
                    for raster in self.rasters1.getRasters():
                        outputRc = ilwis.do("mapcalc", oper, raster,self.p2)
                        outputs.append(outputRc)
            elif not self.ismaps1 and self.ismaps2:
                    for raster in self.rasters2.getRasters():
                        outputRc = ilwis.do("mapcalc", oper, self.p1,raster)
                        outputs.append(outputRc)  
            else:
                output = None
                if self.operation in ['+','-', '/', '*','>','<','<=', '>=', '==', 'or','and', 'xor']:
                    expr = str(self.p1) + self.operation + str(self.p2)
                    output = eval(expr)
                elif self.operation in ['log']:
                    expr = 'math.' + self.operation + '(' + str(self.p1)+ ',' +  str(self.p2) + ')'
                    output = eval(expr)
                out = createOutput(constants.STATUSFINISHED, output, constants.DTNUMBER) 

            if self.ismaps1 or self.ismaps2:
                common.registerIlwisIds(openeojob.job_id, outputs)  
                outputRasters.extend(self.makeOutput(outputs, self.extra))
                out =  createOutput(constants.STATUSFINISHED, outputRasters, constants.DTRASTER)                
              
            self.logEndOperation(processOutput,openeojob, outputs=outputRasters)

            return out

    def base_run2(self,openeojob, processOutput, processInput):
        if self.runnable:
            self.logStartOperation(processOutput, openeojob)
            ##put2Queue(processOutput, {'progress' : 0, 'job_id' : openeojob.job_id, 'status' : 'running'})

            outputRasters = [] 
            oper = '@1' + self.operation + '@2' 
            outputs = []                               
            if self.ismaps1 and self.ismaps2:
                for idx in len(self.rasters1):
                    outputRc = ilwis.do("mapcalc", oper, self.rasters1[idx],self.rasters2[idx])
                    outputs.append(outputRc)
            elif self.ismaps1 and not self.ismaps2:
                    for idx in range(len(self.rasters1)):
                        outputRc = ilwis.do("mapcalc", oper, self.rasters1[idx],self.p2)
                        outputs.append(outputRc)
            elif not self.ismaps1 and self.ismaps2:
                    for idx in range(len(self.rasters2)):
                        outputRc = ilwis.do("mapcalc", oper, self.p1,self.rasters2[idx])
                        outputs.append(outputRc)  
            else:
                output = None
                if self.operation in ['+','-', '/', '*', '<=', '>=', '==', 'or','and', 'xor']:
                    expr = str(self.p1) + self.operation + str(self.p2)
                    output = eval(expr)
                elif self.operation in ['log']:
                    expr = 'math.' + self.operation + '(' + str(self.p1)+ ',' +  str(self.p2) + ')'
                    output = eval(expr)
                out = createOutput(constants.STATUSFINISHED, output, constants.DTNUMBER) 

            if self.ismaps1 or self.ismaps2:
                outputRasters.extend(self.setOutput(openeojob.job_id, outputs, self.extra))
                out =  createOutput(constants.STATUSFINISHED, outputRasters, constants.DTRASTER)                
              
                
            self.logEndOperation(processOutput,openeojob, outputs=outputRasters)
            ##put2Queue(processOutput,{'progress' : 100, 'job_id' : openeojob.job_id, 'status' : 'finished'}) 
            return out
        openeologging.notRunnableError(openeojob.job_id)   
        return createOutput('error', "operation no runnable", constants.DTERROR)                        