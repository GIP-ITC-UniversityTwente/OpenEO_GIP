from openeooperation import *
from operationconstants import *
from constants import constants
from common import openeoip_config
import re


class SaveResultOperation(OpenEoOperation):
    def __init__(self):
        self.loadOpenEoJsonDef('save_result.json')
        
        self.kind = constants.PDPREDEFINED

    def prepare(self, arguments):
        toServer, job_id = self.getDefaultArgs(arguments) 
        self.logStartPrepareOperation(job_id)
        self.runnable = False
        self.format = arguments['format']['resolved']
        self.data = arguments['data']['resolved']
        self.options = arguments['options']['resolved']
        self.runnable = True
        self.logEndPrepareOperation(job_id) 

              

    def run(self,openeojob , processOutput, processInput):
        if self.runnable:
            self.logStartOperation(processOutput, openeojob)
            filePath = openeoip_config['data_locations']['root_user_data_location']
            filePath = filePath['location'] + '/' + str(openeojob.job_id) 
            if not os.path.exists(filePath):   
                os.makedirs(filePath)

            env = ilwis.Envelope()
            count = 1
            if self.data != None:
                if isinstance(self.data, list):
                    for d in self.data:
                        if isinstance(d, RasterData):
                            name = d['title'] 
                            name = name.replace('_ANONYMOUS', 'raster')                    
                            for raster in d.getRasters():
                                outpath = filePath + '/' + name + "_"+ str(count)
                                raster.store("file://" + outpath,self.format, "gdal")
                                envTemp = raster.envelope()
                                if not env:
                                    env = envTemp
                                else:
                                    env.add(envTemp)
                                count = count + 1                        
                    

                            ext = ('.tif','.dat','.mpr','.tiff','.jpg', '.png')
                            file_names = [f for f in os.listdir(filePath) if f.endswith(ext)]
                            files = []
                            for filename in file_names:
                                fn = filePath + "/"  + filename
                                files.append(fn)
                        self.logEndOperation(processOutput,openeojob)
                        outputInfo =  createOutput(constants.STATUSFINISHED, files, constants.DTRASTERLIST)
                else:
                    outputInfo = createOutput(constants.STATUSFINISHED, self.data, constants.DTNUMBER)                       
            if env:
                parts = re.split("[\s,]+", str(env))
                outputInfo['spatialextent'] = parts
            return outputInfo
        message = common.notRunnableError(self.name, openeojob.job_id) 
        return createOutput('error', message, constants.DTERROR)
        
def registerOperation():
     return SaveResultOperation()