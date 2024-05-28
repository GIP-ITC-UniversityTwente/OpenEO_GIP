from openeooperation import *
from operationconstants import *
from constants import constants
from rasterdata import RasterData

class FilterBands(OpenEoOperation):
    def __init__(self):
        self.loadOpenEoJsonDef('filter_bands.json') 

    def prepare(self, arguments):
        self.runnable = False 
        self.bandsByWavelength = []    
        self.bandsByName = []   
        if 'serverChannel' in arguments:
            toServer = arguments['serverChannel']
            job_id = arguments['job_id']
        self.inpData = arguments['data']['resolved']
        if len(self.inpData) == 0:
            message =  "invalid input. Number of rasters is 0 in operation:" + self.name
            common.logMessage(logging.ERROR, message,common.process_user)
            self.handleError(toServer, job_id, 'Input raster',message, 'ProcessParameterInvalid')
            return message         
        if isinstance(self.inpData[0], RasterData):
            if 'bands' in arguments:
                requestedBands = arguments['bands']['resolved']
                if len(requestedBands) > 0:
                    foundCount = 0
                    for item in self.inpData:
                        for bandItem in item['eo:bands'].items():
                            if bandItem[1]['name'] in requestedBands or bandItem[1]['commonbandname'] in requestedBands:
                                self.bandsByName.append(item)
                                foundCount = foundCount + 1
                    if foundCount != len(requestedBands):
                        message =  'Band list doesn match available bands'
                        common.logMessage(logging.ERROR, message,common.process_user)
                        self.handleError(toServer, job_id, 'bands',message, 'ProcessParameterInvalid')                   
            if 'wavelengths' in arguments:
                wavelengths = arguments['wavelengths']['resolved']
                if len(wavelengths) > 0:
                    for subset in wavelengths:
                        if len(subset) != 2:
                            self.handleError(toServer, job_id, 'filter bands', 'All subset of wavelength values must have 2 values(min, max)', 'ProcessParameterInvalid')
                        minv = subset[0]
                        maxv = subset[1]
                        isinstance(minv, (int, float))
                        if isinstance(minv, (int, float)) and isinstance(maxv, (int, float)): 
                            if minv > maxv:
                                self.handleError(toServer, job_id, 'filter bands', 'All subset of wavelength values must be ordered min..max', 'ProcessParameterInvalid')                                    
                        else:
                            self.handleError(toServer, job_id, 'filter bands', 'Wavelength values must be numerical', 'ProcessParameterInvalid') 
                                                                                                                                       
                        for item in self.inpData:
                            for bandItem in item['eo:bands'].items():
                                cwavelength = bandItem[1].getDetail(RasterBand.CENTER_WAVELENGTH)
                                if cwavelength >= minv  and maxv >= cwavelength:
                                    self.bandsByWavelength.append(item)
            self.runnable = True                                 
                

    def run(self,openeojob , processOutput, processInput):
        if self.runnable:
            self.logStartOperation(processOutput, openeojob)
            outData = []
            for item in self.bandsByName:
                outData.append(item)
            for item in self.bandsByWavelength:
                found = False
                for existingItem in outData:
                    if existingItem['id'] == item['id']:
                        found = True
                        break
                if not found:
                    outData.append(item)

            self.logEndOperation(processOutput,openeojob)
            return createOutput(constants.STATUSFINISHED, outData, constants.DTRASTER)
        common.notRunnableError(self.name, openeojob.job_id) 
        return createOutput('error', "operation not runnable", constants.DTERROR)   

def registerOperation():
    return FilterBands()               