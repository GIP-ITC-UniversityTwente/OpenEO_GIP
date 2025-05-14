from openeooperation import *
from operationconstants import *
from constants import constants
from common import openeoip_config
import re
import openeologging


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
        """
        Executes the save result operation.

        Args:
            openeojob: The OpenEO job object.
            processOutput: The output object for logging and communication.
            processInput: The input object for the operation.

        Returns:
            A dictionary containing the output information.
        """        
        if self.runnable:
            self.logStartOperation(processOutput, openeojob)
            filePath = self._createOutputDirectory(openeojob)

            env = ilwis.Envelope()
      
            if self.data is not None:
                    if isinstance(self.data, list):
                        outputInfo = self._processRasterData(filePath, env)
                    else:
                        outputInfo = createOutput(constants.STATUSFINISHED, self.data, constants.DTNUMBER)

                    if env:
                        outputInfo['spatialextent'] =  re.split(r"[\s,]+", str(env))

                    self.logEndOperation(processOutput, openeojob)
                    return outputInfo

        message = openeologging.notRunnableError(self.name, openeojob.job_id)
        return createOutput('error', message, constants.DTERROR)
    
    def _processRasterData(self, filePath, env):
        """
        Processes raster data and stores it in the output directory.

        Args:
            filePath: The path to the output directory.
            env: The spatial envelope to update.

        Returns:
            A dictionary containing the output information.
        """
        count = 1
        files = []

        for data_item in self.data:
            if isinstance(data_item, DataCube):
                files.extend(self._storeRasterData(filePath, data_item, env, count))
                count += len(data_item.getRasters())

        return createOutput(constants.STATUSFINISHED, files, constants.DTRASTERLIST)
    
    def _createOutputDirectory(self, openeojob):
        """
        Creates the output directory for storing results.

        Args:
            openeojob: The OpenEO job object.

        Returns:
            The path to the output directory.
        """
        filePath = openeoip_config['data_locations']['root_user_data_location']
        filePath = filePath['location'] + '/' + str(openeojob.job_id)
        if not os.path.exists(filePath):
            os.makedirs(filePath)
        return filePath

    def _storeRasterData(self, filePath, rasterData, env, count):
            """
            Stores raster data in the output directory.

            Args:
                filePath: The path to the output directory.
                rasterData: The raster data to store.
                env: The spatial envelope to update.
                count: The starting count for naming files.

            Returns:
                A list of file paths for the stored raster data.
            """
            files = []
            name = rasterData['title'].replace('_ANONYMOUS', 'raster')

            for raster in rasterData.getRasters():
                outpath = f"{filePath}/{name}_{count}"
                raster.store(f"file://{outpath}", self.format, "gdal")

                envTemp = raster.envelope()
                if not env:
                    env = envTemp
                else:
                    env.add(envTemp)

                count += 1
            ext = ('.tif', '.dat', '.mpr', '.tiff', '.jpg', '.png')
            files.extend([os.path.join(filePath, f) for f in os.listdir(filePath) if f.endswith(ext)])
            return files    
        
def registerOperation():
     return SaveResultOperation()