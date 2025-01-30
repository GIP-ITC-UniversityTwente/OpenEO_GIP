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
        selectedBands = []  
        toServer, job_id = self.getDefaultArgs(arguments) 
        self.logStartPrepareOperation(job_id)              
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
                    for rasterItem in self.inpData:
                        bands = rasterItem.getBands()
                        for bandItem in bands:
                            if bandItem['name'] in requestedBands or bandItem['commonbandname'] in requestedBands:
                                selectedBands.append(bandItem)
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
                                                                                                                                       
                        for rasterItem in self.inpData:
                            bands = rasterItem.getBands()
                            for bandItem in bands:
                                cwavelength = bandItem.getDetail(RasterBand.CENTER_WAVELENGTH)
                                if cwavelength >= minv  and maxv >= cwavelength:
                                    selectedBands.append(bandItem)
            self.selectedBands = []
            names = []
            for sb in selectedBands:
                name = sb['name']
                if not name in names:
                    self.selectedBands.append(sb)
            self.runnable = True 
            self.logEndPrepareOperation(job_id)                                 
                

    def run(self,openeojob , processOutput, processInput):
        if self.runnable:
            self.logStartOperation(processOutput, openeojob)
            outData = []
            for rasterItem in self.inpData:
                outData.append(rasterItem.createRasterDatafromBand( self.selectedBands))

            self.logEndOperation(processOutput,openeojob, outData)
            return createOutput(constants.STATUSFINISHED, outData, constants.DTRASTER)
        common.notRunnableError(self.name, openeojob.job_id) 
        return createOutput('error', "operation not runnable", constants.DTERROR)   

def registerOperation():
    return FilterBands()               