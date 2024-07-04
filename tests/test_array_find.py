from tests import basetests

import openeo
import sys
import openeo.rest.datacube
from openeo.processes import all
import constants.constants as cc
from openeo.processes import power, ProcessBuilder, array_find

conn = basetests.openConnection()

cube_s2 = conn.load_collection(
    cc.TESTFILENAME2 ,
    spatial_extent =  {"west": -5, "south": 26, "east":  65, "north":67},
    temporal_extent =  ["2020-11-03", "2020-11-07"],
    bands = ['TB01'])

   
def execArrayFind(operation, pr):
    p = [4,7,19,7,0]
    pr = array_find(p, 19)
    job = conn.create_job(pr)
    job.start_and_wait()
    z = job.get_results()
    cc = z.get_metadata()
    

class TestArrayFind(basetests.BaseTest):

    def test_01_arrrayfind(self): 
        self.prepare(sys._getframe().f_code.co_name)

        basetests.testExceptionCondition1(self, True, lambda r1 : execArrayFind(operation=r1, pr = ''), 'allarrayfind',"basic")
 