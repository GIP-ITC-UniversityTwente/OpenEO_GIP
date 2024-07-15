from tests import basetests

import openeo
import sys
import openeo.rest.datacube
from openeo.processes import ProcessBuilder, apply_kernel, apply
import constants.constants as cc

conn = basetests.openConnection()

cube_s2 = conn.load_collection(
    cc.TESTFILENAME1 ,
    spatial_extent =  {"west": -5, "south": 26, "east":  65, "north":67},
    temporal_extent =  ["2020-11-03", "2020-11-07"],
    bands = ['TB01']
)

   
def execApplyKernel(operation, kern=[], wvl=[], s=True):
    name = operation + ".tif"
    b2 = cube_s2.apply_kernel(kern, border='reflect')
    if s:
        b2.download(name)
        basetests.testCheckSumSingle('filter_bands', operation, name)
    else:
        result = b2.save_result("GTiff")
        job = result.create_job()
        job.start_and_wait()
        job.get_results().download_files(operation) 
        basetests.testCheckSumMulti('filter_bands', operation)       
   


class TestFilterBands(basetests.BaseTest):

    def test_01_filter_bands_element(self): 
        self.prepare(sys._getframe().f_code.co_name)

        basetests.testExceptionCondition1(self, True, lambda r1 : execApplyKernel(operation=r1, kern=[[1,2,3],[4,5,6],[7,8,9]]), 'filterbasicname',"filter on one bands")