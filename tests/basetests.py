import unittest
import configglobals
from zlib import adler32
import shutil
import os
import json


CUSTOM_EX = 'CUSTOM_EX@'

def customError(regular_message, custom_message):
     if not custom_message.find(CUSTOM_EX) == -1:
          parts = custom_message.split('@')
          regular_message = regular_message + "," + parts[1]
     return regular_message              
    
def testExceptionCondition6(p, expected,func, parm1, parm2, parm3, parm4, parm5, parm6, message):
      try:
           obj = func(parm1, parm2, parm3, parm4, parm5, parm6)
           p.isTrue(expected, message)
           return obj
      except Exception as ex:
           message = customError(message, str(ex))
           p.isTrue(not expected, message)

def testExceptionCondition5(p, expected, func, parm1, parm2, parm3, parm4, parm5, message):
      try:
           obj = func(parm1, parm2, parm3, parm4, parm5)
           p.isTrue(True, expected)
           return obj
      except Exception as ex:
           message = customError(message, str(ex))
           p.isTrue(True, not expected)
           return False

def testExceptionCondition4(p, expected, func, parm1, parm2, parm3, parm4, message):
      try:
           obj = func(parm1, parm2, parm3, parm4)
           p.isTrue(expected, message)
           return obj
      except Exception as ex:
           message = customError(message, str(ex))
           p.isTrue(not expected, message)
           return None

def testExceptionCondition3(p, expected, func, parm1, parm2, parm3, message):
      try:
           obj = func(parm1, parm2, parm3)
           p.isTrue(expected, message)
           return obj
      except Exception as ex:
           message = customError(message, str(ex))
           p.isTrue(not expected, message)
           return None

def testExceptionCondition2(p, expected, func, parm1, parm2, message):
      try:
           obj = func(parm1, parm2)
           p.isTrue(expected, message)
           return obj
      except Exception as ex:
           message = customError(message, str(ex))
           p.isTrue(not expected, message)

def testExceptionCondition1(p, expected, func, parm1, message):
      try:
           obj = func(parm1)
           p.isTrue(expected, message)
           return obj
      except Exception as ex:
           message = customError(message, str(ex))
           p.isTrue(not expected, message)
           return None 

def checksum(p): 
     BLOCKSIZE=256*1024*1024
     asum = 1
     with open(p,mode="rb") as f:
          while True:
               data = f.read(BLOCKSIZE)
               if not data:
                    break
               asum = adler32(data, asum)
               if asum < 0:
                    asum += 2**32

               return hex(asum)[2:10].zfill(8).lower()   

def testCheckSum(group, operation, name):
     testdir = configglobals.testdir
     #group = 'aggregate'
     testdiragg = testdir + "/" + group
     if os.path.exists(name):
          file_stats = os.stat(name) # sometimes if an error occurs openeo still creates an empty result file
          if file_stats.st_size == 0:
               raise Exception('file not found')

          if not os.path.exists(testdiragg):
               os.makedirs(testdiragg)
          checksumfile = testdir + "/checksums.txt"
          data = {}
          if os.path.exists(checksumfile):
               file_stats = os.stat(checksumfile)
               if file_stats.st_size > 0:
                    with open(checksumfile, 'r') as fp:
                         data = json.load(fp)
               else: # probably rest of an aborted test
                    os.remove(checksumfile)               
          testfile = testdiragg + "/"+ name        
          if os.path.exists(testfile):        
               os.remove(testfile)
          shutil.move(name, testdiragg)
          cc = checksum(testfile)

          key = group + operation
          if key in data:
               if not data[key]['checksum'] == cc:
                    raise Exception(CUSTOM_EX + 'checksum error')
          else:
               entry = {'group' : group, 'operation' : operation, 'checksum' : cc}
               data[key] = entry
               with open(checksumfile, 'w') as fp:
                    json.dump(data, fp)     
     else:
          raise Exception('file not found')

class BaseTest(unittest.TestCase):
    
     def prepare(self, testdir):
         print('started:' + testdir)
         

     def decorateFunction(self, mod, fn) :
        self.decoration = mod + " ==> " + fn 
        print("\n" + self.decoration + "\n")


     def isEqual(self, str1, str2, msg):
        cls = configglobals.TestManager()
        cls.incTestCount()
        result = 'SUCCESS'
        if (str1 != str2):
           cls.addErrorNumber(cls.testCount())
           result = 'FAIL'

        print(f'{cls.testCount():5} {msg:65}  {result}')

     def isAlmostEqualNum(self, num1, num2, delta, msg) :
        cls = configglobals.TestManager()
        cls.incTestCount()
        result = 'SUCCESS'
        if (abs(num1 - num2) > delta):
           cls.addErrorNumber(cls.testCount())
           result = 'FAIL'

        print(f'{cls.testCount():5} {msg:65}  {result}')

 
     def isTrue(self, b, msg):
        cls = configglobals.TestManager()
        cls.incTestCount() 
        result = 'FAIL'
        if (b):
           result = 'SUCCESS'
        else:
            cls.addErrorNumber(cls.testCount())         
        print(f'{cls.testCount():5} {msg:65}  {result}')

     def isFalse(self, b, msg):
        cls = configglobals.TestManager()
        cls.incTestCount() 
        result = 'SUCCESS'
        if (b):
            cls.addErrorNumber(cls.testCount())  
            result = 'FAIL'
        print(f'{cls.testCount():5} {msg:65}  {result}')
        

   


    


           

