from tests import basetests

import openeo
import sys
import openeo.rest.datacube
from openeo.processes import all
import constants.constants as cc
from openeo.processes import power, ProcessBuilder,count

conn = basetests.openConnection()

cube_s2 = conn.load_collection(
    cc.TESTFILENAME2 ,
    spatial_extent =  {"west": -5, "south": 26, "east":  65, "north":67},
    temporal_extent =  ["2020-11-03", "2020-11-07"],
    bands = ['TB01'])

   
def execCount(operation, d, cond, res):
    pr = count(data=d, condition=cond)
    job = conn.create_job(pr)
    job.start_and_wait()
    v = basetests.getAssetValue(job)
    if int(v) != res:
        raise Exception( v + ' not equal to ' + str(res))
   

class TestArrayFind(basetests.BaseTest):

    def test_01_count(self): 
        self.prepare(sys._getframe().f_code.co_name)
        basetests.testExceptionCondition1(self, True, lambda r1 : execCount(operation=r1, d = [3,6,1,67], cond = '', res=4), 'dimlabels1',"count basic")
        basetests.testExceptionCondition1(self, True, lambda r1 : execCount(operation=r1, d = [3,6,None, 67], cond = '', res=3), 'dimlabels2',"count with None values")
        basetests.testExceptionCondition1(self, True, lambda r1 : execCount(operation=r1, d = [True,'maybe',None, True], cond = 'maybe', res=1), 'dimlabels3',"Different types") 
        basetests.testExceptionCondition1(self, False, lambda r1 : execCount(operation=r1, d = "Faulty", cond = 'maybe', res=1), 'dimlabels3',"No list used, fail")         
        basetests.testExceptionCondition1(self, True, lambda r1 : execCount(operation=r1, d = [0,1,2,3,4,5,None], cond = {'gt':{"process_id":"gt","arguments":{"x":{"from_parameter":"element"},"y":2},"result":True}}, res=3), 'dimlabels4',"using process graph, fail")                 
              
