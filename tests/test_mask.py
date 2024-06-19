from tests import basetests

import openeo
import sys
import openeo.rest.datacube
from openeo.processes import ProcessBuilder, mask, apply
import constants.constants as cc

conn = basetests.openConnection()

cube_s2 = conn.load_collection(
    cc.TESTFILENAME2 ,
    spatial_extent =  {"west": -5, "south": 26, "east":  65, "north":67},
    temporal_extent =  ["2020-11-03", "2020-11-07"])

cube_s3 = conn.load_collection(
    cc.TESTFILENAME3 ,
    spatial_extent =  {"west": -5, "south": 26, "east":  65, "north":67},
    temporal_extent =  ["2020-11-03", "2020-11-07"])

   
def execAddDimension(operation):
    name = operation + ".tif"
    b2 = cube_s2.apply(lambda : mask(cube_s2, cube_s3, 100))
    result = b2.save_result("GTiff")
    job = result.create_job()
    job.start_and_wait()
    job.get_results().download_files(name) 
    basetests.testCheckSumMulti('mask', name)       
   


class TestMask(basetests.BaseTest):

    def test_01_mask(self): 
        self.prepare(sys._getframe().f_code.co_name)

        basetests.testExceptionCondition1(self, True, lambda r1 : execAddDimension(operation=r1), 'mask1','simpel mask')
