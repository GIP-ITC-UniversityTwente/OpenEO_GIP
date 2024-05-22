import openeo.processes
from tests import basetests

import openeo
import sys
import openeo.rest.datacube
from openeo.processes import ProcessBuilder, sum
import constants.constants as cc


conn = openeo.connect("http://127.0.0.1:5000")
conn.authenticate_basic("tester", "pwd") 
##conn = openeo.connect("http://cityregions.roaming.utwente.nl:5000")  

cube_s1 = conn.load_collection(
    cc.TESTFILENAME1 ,
    spatial_extent =  {"west": -5, "south": 26, "east":  65, "north":67},
    temporal_extent =  ["2020-11-03", "2020-11-07"],
    bands = ['TB01'])

cube_s2 = conn.load_collection(
    cc.TESTFILENAME2 ,
    spatial_extent =  {"west": -5, "south": 26, "east":  65, "north":67},
    temporal_extent =  ["2020-11-03", "2020-11-07"],
    bands = ['TB01', 'TB02'])

  
def execAgg(operation, sdata):
    cube_s2 = sdata.reduce_dimension(dimension="t", reducer=operation )
    name = operation + ".tif"
    cube_s2.download(name)
    basetests.testCheckSum('aggregate', operation, name)

   
class TestAggregateStats(basetests.BaseTest):

    def test_01_agg_raster(self): 
        self.prepare(sys._getframe().f_code.co_name)
        cubedata = cube_s2
        basetests.testExceptionCondition1(self, False, lambda r1 : execAgg(operation=r1, sdata=cubedata), 'sum',"two result, one download. must fail. sum")
       
        cubedata =cube_s1
        basetests.testExceptionCondition1(self, True, lambda r1 : execAgg(operation=r1, sdata=cubedata), 'sum',"Aggregate stats. sum")
        basetests.testExceptionCondition1(self, True, lambda r1 : execAgg(operation=r1, sdata=cubedata), 'mean',"Aggregate stats. mean")
        basetests.testExceptionCondition1(self, True, lambda r1 : execAgg(operation=r1, sdata=cubedata), 'max',"Aggregate stats. max")
        basetests.testExceptionCondition1(self, True, lambda r1 : execAgg(operation=r1, sdata=cubedata), 'min',"Aggregate stats. min")
        basetests.testExceptionCondition1(self, True, lambda r1 : execAgg(operation=r1, sdata=cubedata), 'median',"Aggregate stats. median")
        basetests.testExceptionCondition1(self, True, lambda r1 : execAgg(operation=r1, sdata=cubedata), 'sd',"Aggregate stats. standard deviation")
        basetests.testExceptionCondition1(self, True, lambda r1 : execAgg(operation=r1, sdata=cubedata), 'variance',"Aggregate stats. variance")
        basetests.testExceptionCondition1(self, False, lambda r1 : execAgg(operation=r1, sdata=cubedata), 'dummy',"Aggregate stats. operation doesnt exist")        

     

      

      
