from tests import basetests

import openeo
import sys
import openeo.rest.datacube
from openeo.processes import all
import constants.constants as cc
from openeo.processes import power, ProcessBuilder,dimension_labels

conn = basetests.openConnection()

cube_s2 = conn.load_collection(
    cc.TESTFILENAME2 ,
    spatial_extent =  {"west": -5, "south": 26, "east":  65, "north":67},
    temporal_extent =  ["2020-11-03", "2020-11-07"],
    bands = ['TB01'])

   
def execDimensionLabels(operation, pr):
    pr = dimension_labels(cube_s2,pr)
    job = conn.create_job(pr)
    job.start_and_wait()
    z = job.get_results()
    cc = z.get_metadata()
    print(cc)
    

class TestArrayFind(basetests.BaseTest):

    def test_01_arrrayfind(self): 
        self.prepare(sys._getframe().f_code.co_name)

        basetests.testExceptionCondition1(self, True, lambda r1 : execDimensionLabels(operation=r1, pr = 't'), 'dimlabels1',"basic")
        basetests.testExceptionCondition1(self, False, lambda r1 : execDimensionLabels(operation=r1, pr = 'nope'), 'dimlabels2, not exist',"basic2")