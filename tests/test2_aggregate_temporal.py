import openeo.processes
from tests import basetests

import openeo
import sys
import openeo.rest.datacube
from openeo.processes import ProcessBuilder, sum
import constants.constants as cc


conn = basetests.openConnection()


cube_s1 = conn.load_collection(
    cc.TESTFILENAME1 ,
    spatial_extent =  {"west": -5, "south": 26, "east":  65, "north":67},
    temporal_extent =  ["2020-11-03", "2020-11-07"],
    bands = ['TB01'])

cube_s2 = conn.load_collection(
    cc.TESTFILENAME_MORE_LAYERS ,
    spatial_extent =  {"west": -5, "south": 26, "east":  65, "north":67},
    temporal_extent =  ["2020-11-01", "2020-12-10"],
    bands = ['TB01', 'TB02'])

cube_smany = conn.load_collection(
    cc.TESTFILENAME_MANY_LAYERS ,
    spatial_extent =  {"west": -5, "south": 26, "east":  65, "north":67},
    temporal_extent =  ["2020-11-01", "2021-02-28"],
    bands = ['TB01', 'TB02'])

def execAgg(operation, ti, sdata):
    if sdata == []:
        cube_s3 = cube_s2.aggregate_temporal( ti, reducer = 'mean')
    else:
        cube_s3 = cube_s2.aggregate_temporal( ti, reducer = 'mean', labels = sdata)        
    result = cube_s3.save_result("GTiff")
    job = result.create_job()
    job.start_and_wait()
    job.get_results().download_files(operation) 
    basetests.testCheckSumMulti('aggregate_temporal', operation)

def execAgg2(operation, ti, sdata):
    if sdata == []:
        cube_s3 = cube_smany.aggregate_temporal_period( ti, reducer = 'mean')
    else:
        cube_s3 = cube_smany.aggregate_temporal( ti, reducer = 'mean', labels = sdata)        
    result = cube_s3.save_result("GTiff")
    job = result.create_job()
    job.start_and_wait()
    job.get_results().download_files(operation) 
    basetests.testCheckSumMulti('aggregate_temporal', operation)                          

class TestAggregateSTemp(basetests.BaseTest):

    def test_01_agg_temp_period(self):
        basetests.testExceptionCondition1(self, True, lambda r1 : execAgg2(operation=r1, ti='dekad', sdata=[]), 'aggtempperiod1',"Aggregate stats period.") 

    def test_02_agg_temp(self): 
        self.prepare(sys._getframe().f_code.co_name)

        basetests.testExceptionCondition1(self, True, lambda r1 : execAgg(operation=r1, ti= [["2020-11-03", "2020-11-07"], ['2020-11-13', '2020-11-23']], sdata=[]), 'aggtemp1',"Aggregate stats.") 
        basetests.testExceptionCondition1(self, True, lambda r1 : execAgg(operation=r1, ti= [["2020-11-03", "2020-11-07"], ['2020-11-13', '2020-11-23']], sdata=['t1', 't2']), 'aggtemp2',"Aggregate stats, labels match.") 
        basetests.testExceptionCondition1(self, False, lambda r1 : execAgg(operation=r1, ti= [["2020-11-03", "2020-11-07"]], sdata=['t1', 't2']), 'aggtemp3',"Aggregate stats, no match labels. fail")         
        basetests.testExceptionCondition1(self, False, lambda r1 : execAgg(operation=r1, ti= [["2000-11-03", "2000-11-07"], ['2000-11-13', '2000-11-23']], sdata=[]), 'aggtemp4',"Aggregate stats. no overlap intervals, fail")         
        basetests.testExceptionCondition1(self, False, lambda r1 : execAgg(operation=r1, ti= [], sdata=[]), 'aggtemp5',"empty intervals")                 

