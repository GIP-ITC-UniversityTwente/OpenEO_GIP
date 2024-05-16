import openeo.processes
from tests import basetests

import openeo
import sys
import openeo.rest.datacube
from openeo.processes import ProcessBuilder, add, multiply, divide, subtract
import openeo.processes as oeo
import constants.constants as cc


conn = openeo.connect("http://127.0.0.1:5000")
conn.authenticate_basic("tester", "pwd") 
##conn = openeo.connect("http://cityregions.roaming.utwente.nl:5000")  
'''
cube_s1 = conn.load_collection(
    "Sentinel2TimeSeriesData" ,
    spatial_extent =  {"west": -119.2201, "south": 35.959, "east":  -119.0861, "north": 36.0574},
    temporal_extent =  ["2018-11-18", "2019-10-24"],
    bands = ['B02','B04']
)
'''

cube_s1 = conn.load_collection(
    "SYNTHETIC_DATA1" ,
    spatial_extent =  {"west": -5, "south": 26, "east":  65, "north":67},
    temporal_extent =  ["2020-11-03", "2020-11-07"],
    bands = ['TB01']
)
band_selection = 'cube_s1.band("TB01")'

def execUnaryMathRaster(oper, name):
    expr = 'oeo.' + oper + '(' + band_selection + ')'
    cube_s2 = cube_s1.apply(lambda : eval(expr))
    name = name + ".tif"
    cube_s2.download(name)
    basetests.testCheckSum('math', name, name)

def execBinaryMathRasterAlt(oper, name):
    expr = oper + '(' + band_selection + ', 10)'
    new_cube = cube_s1.apply(lambda : eval(expr))
    name = name + ".tif"
    new_cube.download(name)
    basetests.testCheckSum('math', oper, name)

def execBinaryMathRaster(oper, name):
    expr = band_selection + oper + '10' 
    cube_s2 = eval(expr)
    name = name + ".tif"
    cube_s2.download(name)
    basetests.testCheckSum('math', name, name)

class TestMathRaster(basetests.BaseTest):
    def test_01_binary_math_raster(self): 
        self.prepare(sys._getframe().f_code.co_name)
             
        basetests.testExceptionCondition1(self, True, lambda r1: execBinaryMathRaster('+', 'add'), 'add',"band math. add int")
        basetests.testExceptionCondition1(self, True, lambda r1: execBinaryMathRasterAlt('add', 'addalt'), 'add alt',"band math. add alt int")

        basetests.testExceptionCondition1(self, True, lambda r1: execBinaryMathRaster('*', 'multiply'), 'multiply',"band math. multiply int")
        basetests.testExceptionCondition1(self, True, lambda r1: execBinaryMathRasterAlt('multiply', 'multiplyalt'), 'mulitiply alt',"band math. multiply alt int")

        basetests.testExceptionCondition1(self, True, lambda r1: execBinaryMathRaster('-', 'subtract'), 'subtract',"band math. subtract int")
        basetests.testExceptionCondition1(self, True, lambda r1: execBinaryMathRasterAlt('subtract', 'subtractalt'), 'subtract alt',"band math. subtract alt int")

        basetests.testExceptionCondition1(self, True, lambda r1: execBinaryMathRaster('/', 'divide'), 'divide',"band math. divide int")
        basetests.testExceptionCondition1(self, True, lambda r1: execBinaryMathRasterAlt('divide', 'dividealt'), 'divide alt',"band math. divide alt int")                        

    def test_02_unary_math_raster(self): 
        self.prepare(sys._getframe().f_code.co_name) 

        basetests.testExceptionCondition1(self, True, lambda r1: execUnaryMathRaster('sin', 'sin'), 'sin',"band math. sinus")
        basetests.testExceptionCondition1(self, True, lambda r1: execUnaryMathRaster('cos', 'cos'), 'cos',"band math. cosinus")
        basetests.testExceptionCondition1(self, True, lambda r1: execUnaryMathRaster('tan', 'tan'), 'tan',"band math. tangent")        
        basetests.testExceptionCondition1(self, True, lambda r1: execUnaryMathRaster('exp', 'exp'), 'exp',"band math. exp")
        basetests.testExceptionCondition1(self, True, lambda r1: execUnaryMathRaster('arcsin', 'arcsin'), 'arcsin',"band math. arcsinus")        
        basetests.testExceptionCondition1(self, True, lambda r1: execUnaryMathRaster('arccos', 'arccos'), 'arccos',"band math. arccos")
        basetests.testExceptionCondition1(self, True, lambda r1: execUnaryMathRaster('arctan', 'arctan'), 'arctan',"band math. arctan")
        basetests.testExceptionCondition1(self, True, lambda r1: execUnaryMathRaster('arsinh', 'arsinh'), 'arcsinh',"band math. arcsinush")        
        basetests.testExceptionCondition1(self, True, lambda r1: execUnaryMathRaster('arcosh', 'arcosh'), 'arccosh',"band math. arccosh")
        basetests.testExceptionCondition1(self, True, lambda r1: execUnaryMathRaster('artanh', 'artanh'), 'arctanh',"band math. arctanh")
        basetests.testExceptionCondition1(self, True, lambda r1: execUnaryMathRaster('ceil', 'ceil'), 'ceil',"band math. ceil")
        basetests.testExceptionCondition1(self, True, lambda r1: execUnaryMathRaster('int', 'int'), 'int',"band math. int")
        basetests.testExceptionCondition1(self, True, lambda r1: execUnaryMathRaster('round', 'round'), 'round',"band math. round") 
        basetests.testExceptionCondition1(self, True, lambda r1: execUnaryMathRaster('ln', 'ln'), 'ln',"band math. ln") 
        basetests.testExceptionCondition1(self, True, lambda r1: execUnaryMathRaster('sqrt', 'sqrt'), 'sqrt',"band math. sqrt") 

     

      

      
