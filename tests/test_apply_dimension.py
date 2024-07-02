from tests import basetests

import openeo
import sys
import openeo.rest.datacube
from openeo.processes import all
import constants.constants as cc
from openeo.processes import power, ProcessBuilder, absolute, apply_dimension, power

conn = basetests.openConnection()

cube_s2 = conn.load_collection(
    cc.TESTFILENAME2 ,
    spatial_extent =  {"west": -5, "south": 26, "east":  65, "north":67},
    temporal_extent =  ["2020-11-03", "2020-11-07"],
    bands = ['TB01'])

   
def execFilterBands(operation, pr):
    pr = eval(pr)
    job = conn.create_job(pr)
    job.start_and_wait()
    result = job.download_result()
  
    basetests.testCheckSumSingle('apply_dimension', operation, result.name)
  
   


class TestApplyDimension(basetests.BaseTest):

    def test_01_applydimension(self): 
        self.prepare(sys._getframe().f_code.co_name)

        basetests.testExceptionCondition1(self, True, lambda r1 : execFilterBands(operation=r1, pr = 'apply_dimension(cube_s2, dimension="t", process=absolute)'), 'allapplydim',"basic")
 