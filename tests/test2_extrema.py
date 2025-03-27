import constants.constants
from tests import basetests

import constants.constants as cons
import sys
import openeo.rest.datacube
from openeo.processes import all
import constants.constants as cc
from openeo.processes import power, ProcessBuilder,extrema

conn = basetests.openConnection()

cube_s2 = conn.load_collection(
    cc.TESTFILENAME2 ,
    spatial_extent =  {"west": -5, "south": 26, "east":  65, "north":67},
    temporal_extent =  ["2020-11-03", "2020-11-07"],
    bands = ['TB01'])

   
def execCount(operation, d, cond, res):
    pr = extrema(data=d, ignore_nodata=cond)
    job = conn.create_job(pr)
    job.start_and_wait()
    v = basetests.getAssetValue(job)
    
    if int(v[0]) != res[0]:
        raise Exception( 'min is not ' + str(res) + ' but ' + str(v[0]))
    if int(v[1]) != res[1]:
        raise Exception( 'max is not ' + str(res) + ' but ' + str(v[1]))
    
   

class TestExtrema(basetests.BaseTest):

    def test_01_count(self): 
        self.prepare(sys._getframe().f_code.co_name)
        basetests.testExceptionCondition1(self, True, lambda r1 : execCount(operation=r1, d = [3,6,1,67], cond = False, res=[1,67]), 'extrema1',"extre,a basic")
        basetests.testExceptionCondition1(self, True, lambda r1 : execCount(operation=r1, d = [3,6,None,67], cond = False, res=[3,67]), 'extrema2',"extrama with none")        
        basetests.testExceptionCondition1(self, True, lambda r1 : execCount(operation=r1, d = [], cond = False, res=[cons.UNDEFNUMBER,-cons.UNDEFNUMBER]), 'extrema3',"extrema empty")                
        basetests.testExceptionCondition1(self, True, lambda r1 : execCount(operation=r1, d = [3,6,None,67], cond = False, res=[3,67]), 'extrema4',"extrema igonre undef false but None") 
        basetests.testExceptionCondition1(self, True, lambda r1 : execCount(operation=r1, d = [3,6,cons.UNDEFNUMBER,67], cond = True, res=[3,67]), 'extrema5',"extrema with true undef")         
        basetests.testExceptionCondition1(self, True, lambda r1 : execCount(operation=r1, d = [3,6,cons.UNDEFNUMBER,67], cond = False, res=[3,cons.UNDEFNUMBER]), 'extrema6',"extrema with true undef and not ignore")                 