from operations.ilwispy.BaseMapCalc import BaseBinarymapCalcBase
from constants import constants

class AddOperation(BaseBinarymapCalcBase):
    def __init__(self):
        self.kind = constants.PDPREDEFINED
        self.loadOpenEoJsonDef('add.json')

    def prepare(self, arguments):
        self.base_prepare(arguments, '+')
       

    def run(self,openeojob, processOutput, processInput):
        return self.base_run(openeojob, processOutput, processInput)

class DivideOperation(BaseBinarymapCalcBase):
    def __init__(self):
        self.kind = constants.PDPREDEFINED
        self.loadOpenEoJsonDef('divide.json')

    def prepare(self, arguments):
        self.base_prepare(arguments, '/')
       

    def run(self,openeojob, processOutput, processInput):
        return self.base_run(openeojob, processOutput, processInput)
    
class MultiplyOperation(BaseBinarymapCalcBase):
    def __init__(self):
        self.kind = constants.PDPREDEFINED
        self.loadOpenEoJsonDef('multiply.json')

    def prepare(self, arguments):
        self.base_prepare(arguments, '*')
       

    def run(self,openeojob, processOutput, processInput):
        return self.base_run(openeojob, processOutput, processInput) 
     
class SubtractOperation(BaseBinarymapCalcBase):
    def __init__(self):
        self.kind = constants.PDPREDEFINED
        self.loadOpenEoJsonDef('subtract.json')

    def prepare(self, arguments):
        self.kind = constants.PDPREDEFINED
        self.base_prepare(arguments, '-')
       

    def run(self,openeojob, processOutput, processInput):
        return self.base_run(openeojob, processOutput, processInput) 

class LogNOperation(BaseBinarymapCalcBase):
    def __init__(self):
        self.kind = constants.PDPREDEFINED
        self.loadOpenEoJsonDef('log.json')

    def prepare(self, arguments):
        self.base_prepare(arguments, 'log')
       

    def run(self,openeojob, processOutput, processInput):
        return self.base_run(openeojob, processOutput, processInput) 

class GTOperation(BaseBinarymapCalcBase):
    def __init__(self):
        self.kind = constants.PDPREDEFINED
        self.loadOpenEoJsonDef('gt.json')

    def prepare(self, arguments):
        self.base_prepare(arguments, '>')
       

    def run(self,openeojob, processOutput, processInput):
        return self.base_run(openeojob, processOutput, processInput) 

class GTEOperation(BaseBinarymapCalcBase):
    def __init__(self):
        self.kind = constants.PDPREDEFINED
        self.loadOpenEoJsonDef('gte.json')

    def prepare(self, arguments):
        self.base_prepare(arguments, '>=')
       

    def run(self,openeojob, processOutput, processInput):
        return self.base_run(openeojob, processOutput, processInput) 

class LTOperation(BaseBinarymapCalcBase):
    def __init__(self):
        self.kind = constants.PDPREDEFINED
        self.loadOpenEoJsonDef('lt.json')

    def prepare(self, arguments):
        self.base_prepare(arguments, '<')
       

    def run(self,openeojob, processOutput, processInput):
        return self.base_run(openeojob, processOutput, processInput) 

class LTEOperation(BaseBinarymapCalcBase):
    def __init__(self):
        self.kind = constants.PDPREDEFINED
        self.loadOpenEoJsonDef('lte.json')

    def prepare(self, arguments):
        self.base_prepare(arguments, '<=')
       

    def run(self,openeojob, processOutput, processInput):
        return self.base_run(openeojob, processOutput, processInput) 

class EqOperation(BaseBinarymapCalcBase):
    def __init__(self):
        self.kind = constants.PDPREDEFINED
        self.loadOpenEoJsonDef('eq.json')

    def prepare(self, arguments):
        self.base_prepare(arguments, '==')
       

    def run(self,openeojob, processOutput, processInput):
        return self.base_run(openeojob, processOutput, processInput) 

class OrOperation(BaseBinarymapCalcBase):
    def __init__(self):
        self.kind = constants.PDPREDEFINED
        self.loadOpenEoJsonDef('or.json')

    def prepare(self, arguments):
        self.base_prepare(arguments, 'or')
       

    def run(self,openeojob, processOutput, processInput):
        return self.base_run(openeojob, processOutput, processInput) 

class AndOperation(BaseBinarymapCalcBase):
    def __init__(self):
        self.kind = constants.PDPREDEFINED
        self.loadOpenEoJsonDef('and.json')

    def prepare(self, arguments):
        self.base_prepare(arguments, 'and')
       

    def run(self,openeojob, processOutput, processInput):
        return self.base_run(openeojob, processOutput, processInput) 

class XorOperation(BaseBinarymapCalcBase):
    def __init__(self):
        self.kind = constants.PDPREDEFINED
        self.loadOpenEoJsonDef('xor.json')

    def prepare(self, arguments):
        self.base_prepare(arguments, 'xor')
       

    def run(self,openeojob, processOutput, processInput):
        return self.base_run(openeojob, processOutput, processInput) 

class PowerOperation(BaseBinarymapCalcBase):
    def __init__(self):
        self.kind = constants.PDPREDEFINED
        self.loadOpenEoJsonDef('power.json')

    def prepare(self, arguments):
        self.base_prepare(arguments, 'pow')
       

    def run(self,openeojob, processOutput, processInput):
        return self.base_run(openeojob, processOutput, processInput)                                               
    
def registerOperation():
    funcs = []     
    funcs.append(AddOperation())
    funcs.append(DivideOperation())
    funcs.append(MultiplyOperation()) 
    funcs.append(SubtractOperation()) 
    funcs.append(LogNOperation()) 
    funcs.append(GTOperation()) 
    funcs.append(GTEOperation()) 
    funcs.append(LTOperation()) 
    funcs.append(LTEOperation()) 
    funcs.append(EqOperation()) 
    funcs.append(OrOperation())
    funcs.append(AndOperation()) 
    funcs.append(XorOperation()) 
    funcs.append(PowerOperation())                                       


    return funcs