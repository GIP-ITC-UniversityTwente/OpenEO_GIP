from tests import basetests

import openeo
import sys
import openeo.rest.datacube
from openeo.processes import all
import constants.constants as cc
from openeo.processes import power, ProcessBuilder,is_nan

conn = basetests.openConnection()

def execCount(operation, d, res):
    pr = is_nan(x=d)
    job = conn.create_job(pr)
    job.start_and_wait()
    v = basetests.getAssetValue(job)
    p = str(res)
    if v != p:
        raise Exception( v + ' not equal to ' + str(res))
   

class TestIsNan(basetests.BaseTest):

    def test_01_count(self): 
        self.prepare(sys._getframe().f_code.co_name)
        basetests.testExceptionCondition1(self, True, lambda r1 : execCount(operation=r1, d = 5, res=False), 'isnan1',"Is nan basic, false")
        # apparently 'nan' values can't be jsonfied so not sure how to test this
        #basetests.testExceptionCondition1(self, True, lambda r1 : execCount(operation=r1, d = float('nan'), res=True), 'isnan2',"Is nan basic, true")
   