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

def getCube1():
    cube_s1 = conn.load_collection(
        "Sentinel2TimeSeriesData" ,
        spatial_extent =  {"west": -119.2201, "south": 35.959, "east":  -119.0861, "north": 36.0574},
        temporal_extent =  ["2018-11-18", "2019-10-24"],
        bands = ['B02']
    )
    return cube_s1

def getCube2():
    cube_s2 = conn.load_collection(
        "Sentinel2TimeSeriesData" ,
        spatial_extent =  {"west": -119.2201, "south": 35.959, "east":  -119.0861, "north": 36.0574},
        temporal_extent =  ["2018-11-18", "2019-10-24"],
        bands = ['B02','B04']
    )
    return cube_s2

   
def execAgg(operation, sdata):
    cube_s2 = sdata.reduce_dimension(dimension="t", reducer=operation )
    cube_s2.download("bbb3.tif")
  
class TestAggregateStats(basetests.BaseTest):

    def test_01_agg_raster(self): 
        self.prepare(sys._getframe().f_code.co_name)

        cubedata=getCube2()
        basetests.testExceptionCondition1(self, True, lambda r1 : execAgg(operation=r1, sdata=cubedata), 'sum',"Aggregate stats 2 bands. sum")
        basetests.testExceptionCondition1(self, True, lambda r1 : execAgg(operation=r1, sdata=cubedata), 'mean',"Aggregate stats 2 bands. mean")
        basetests.testExceptionCondition1(self, True, lambda r1 : execAgg(operation=r1, sdata=cubedata), 'max',"Aggregate stats 2 bands. max")
        basetests.testExceptionCondition1(self, True, lambda r1 : execAgg(operation=r1, sdata=cubedata), 'min',"Aggregate stats 2 bands. min")
        basetests.testExceptionCondition1(self, True, lambda r1 : execAgg(operation=r1, sdata=cubedata), 'median',"Aggregate stats 2 bands. median")
        basetests.testExceptionCondition1(self, True, lambda r1 : execAgg(operation=r1, sdata=cubedata), 'sd',"Aggregate stats 2 bands. standard deviation")
        basetests.testExceptionCondition1(self, True, lambda r1 : execAgg(operation=r1, sdata=cubedata), 'variance',"Aggregate stats 2 bands. variance")
        basetests.testExceptionCondition1(self, False, lambda r1 : execAgg(operation=r1, sdata=cubedata), 'dummy',"Aggregate stats 2 bands. operation doesnt exist")

        cubedata=getCube1()
        basetests.testExceptionCondition1(self, True, lambda r1 : execAgg(operation=r1, sdata=cubedata), 'sum',"Aggregate stats. sum")
        basetests.testExceptionCondition1(self, True, lambda r1 : execAgg(operation=r1, sdata=cubedata), 'mean',"Aggregate stats. mean")
        basetests.testExceptionCondition1(self, True, lambda r1 : execAgg(operation=r1, sdata=cubedata), 'max',"Aggregate stats. max")
        basetests.testExceptionCondition1(self, True, lambda r1 : execAgg(operation=r1, sdata=cubedata), 'min',"Aggregate stats. min")
        basetests.testExceptionCondition1(self, True, lambda r1 : execAgg(operation=r1, sdata=cubedata), 'median',"Aggregate stats. median")
        basetests.testExceptionCondition1(self, True, lambda r1 : execAgg(operation=r1, sdata=cubedata), 'sd',"Aggregate stats. standard deviation")
        basetests.testExceptionCondition1(self, True, lambda r1 : execAgg(operation=r1, sdata=cubedata), 'variance',"Aggregate stats. variance")
        basetests.testExceptionCondition1(self, False, lambda r1 : execAgg(operation=r1, sdata=cubedata), 'dummy',"Aggregate stats. operation doesnt exist")

      
