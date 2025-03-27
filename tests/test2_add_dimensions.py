from tests import basetests

import openeo
import sys
import openeo.rest.datacube
from openeo.processes import ProcessBuilder, add_dimension, apply
import constants.constants as cc

conn = basetests.openConnection()

cube_s2 = conn.load_collection(
    cc.TESTFILENAME2 ,
    spatial_extent =  {"west": -5, "south": 26, "east":  65, "north":67},
    temporal_extent =  ["2020-11-03", "2020-11-07"])

cube_s3 = conn.load_collection(
    cc.TESTFILENAME_NO_LAYERS ,
    spatial_extent =  {"west": -5, "south": 26, "east":  65, "north":67})

   
def execAddDimension(operation, nm, cube=cube_s2):
    name = operation + ".tif"
    b2 = cube.apply(lambda : add_dimension(cube,nm, [nm + '_label']))
    result = b2.save_result("GTiff")
    job = result.create_job()
    job.start_and_wait()
    job.get_results().download_files(operation) 
    basetests.testCheckSumMulti('add_dimension', operation) 
    
          
   


class TestAddDimensions(basetests.BaseTest):

    def test_01_add_dimension(self): 
        self.prepare(sys._getframe().f_code.co_name)

        basetests.testExceptionCondition1(self, True, lambda r1 : execAddDimension(operation=r1, nm='bleep'), 'adddimensionb',"add a simpel dimension")
        basetests.testExceptionCondition1(self, False, lambda r1 : execAddDimension(operation=r1, nm='t'), 'adddimensionc',"add a existing dimenions; fails")
        basetests.testExceptionCondition1(self, True, lambda r1 : execAddDimension(operation=r1, nm='t', cube=cube_s3), 'adddimensiond',"add a well known dimenions")