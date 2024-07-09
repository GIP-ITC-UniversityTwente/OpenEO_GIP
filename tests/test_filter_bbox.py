import constants.constants
from tests import basetests

import constants.constants as cons
import sys
import openeo.rest.datacube
from openeo.processes import all
import constants.constants as cc
from openeo.processes import power, ProcessBuilder,filter_bbox

conn = basetests.openConnection()

cube_s2 = conn.load_collection(
    cc.TESTFILENAME2 ,
    spatial_extent =  {"west": -5, "south": 26, "east":  65, "north":67},
    temporal_extent =  ["2020-11-03", "2020-11-07"],
    bands = ['TB01'])

   
def execCount(operation, d, ext):
    pr = filter_bbox(data=d, extent=ext )
    job = conn.create_job(pr)
    job.start_and_wait()
    job.get_results().download_files(operation) 
    basetests.testCheckSumMulti('filter_bands', operation)   
 
    
   

class TestExtrema(basetests.BaseTest):

    def test_01_count(self):
        bbox = {"west": -4, "south": 25, "east": 64, "north": 66, "crs": 4326} 
        self.prepare(sys._getframe().f_code.co_name)
        basetests.testExceptionCondition1(self, True, lambda r1 : execCount(operation=r1, d =cube_s2, ext=bbox), 'filter_bbox1',"basic dict format")
        basetests.testExceptionCondition1(self, True, lambda r1 : execCount(operation=r1, d =cube_s2, ext=[-5,23,66,60]), 'filter_bbox2',"basic list format")
        basetests.testExceptionCondition1(self, False, lambda r1 : execCount(operation=r1, d =cube_s2, ext=[-5,-20,66,-10]), 'filter_bbox3',"no overlap,fail")