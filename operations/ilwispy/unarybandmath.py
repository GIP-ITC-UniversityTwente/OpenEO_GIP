from operations.ilwispy.BaseMapCalc import BaseUnarymapCalc

class ArcCosOperation(BaseUnarymapCalc):
    def __init__(self):
        self.loadOpenEoJsonDef('arccos.json')

    def prepare(self, arguments):
        self.base_prepare(arguments, 'acos')
        return ""

    def run(self,openeojob, processOutput, processInput):
        return self.base_run(openeojob, processOutput, processInput)
    
class ArcCosHOperation(BaseUnarymapCalc):
    def __init__(self):
        self.loadOpenEoJsonDef('arcosh.json')

    def prepare(self, arguments):
        self.base_prepare(arguments, 'acosh')
        return ""

    def run(self,openeojob, processOutput, processInput):
        return self.base_run(openeojob, processOutput, processInput) 

class CosHOperation(BaseUnarymapCalc):
    def __init__(self):
        self.loadOpenEoJsonDef('cosh.json')

    def prepare(self, arguments):
        self.base_prepare(arguments, 'cosh')
        return ""

    def run(self,openeojob, processOutput, processInput):
        return self.base_run(openeojob, processOutput, processInput)         
    
class CosOperation(BaseUnarymapCalc):
    def __init__(self):
        self.loadOpenEoJsonDef('cos.json')

    def prepare(self, arguments):
        self.base_prepare(arguments, 'cos')
        return ""

    def run(self,openeojob, processOutput, processInput):
        return self.base_run(openeojob, processOutput, processInput)    

class ASinOperation(BaseUnarymapCalc):
    def __init__(self):
        self.loadOpenEoJsonDef('arcsin.json')

    def prepare(self, arguments):
        self.base_prepare(arguments, 'asin')
        return ""

    def run(self,openeojob, processOutput, processInput):
        return self.base_run(openeojob, processOutput, processInput)
    
class ASinHOperation(BaseUnarymapCalc):
    def __init__(self):
        self.loadOpenEoJsonDef('arsinh.json')

    def prepare(self, arguments):
        self.base_prepare(arguments, 'asinh')
        return ""

    def run(self,openeojob, processOutput, processInput):
        return self.base_run(openeojob, processOutput, processInput)    
        
class SinOperation(BaseUnarymapCalc):
    def __init__(self):
        self.loadOpenEoJsonDef('sin.json')

    def prepare(self, arguments):
        self.base_prepare(arguments, 'sin')
        return ""

    def run(self,openeojob, processOutput, processInput):
        return self.base_run(openeojob, processOutput, processInput)
    
class SinHOperation(BaseUnarymapCalc):
    def __init__(self):
        self.loadOpenEoJsonDef('sinh.json')

    def prepare(self, arguments):
        self.base_prepare(arguments, 'sinh')
        return ""

    def run(self,openeojob, processOutput, processInput):
        return self.base_run(openeojob, processOutput, processInput)    

class TanOperation(BaseUnarymapCalc):
    def __init__(self):
        self.loadOpenEoJsonDef('tan.json')

    def prepare(self, arguments):
        self.base_prepare(arguments, 'tan')
        return ""

    def run(self,openeojob, processOutput, processInput):
        return self.base_run(openeojob, processOutput, processInput)
    
class TanHOperation(BaseUnarymapCalc):
    def __init__(self):
        self.loadOpenEoJsonDef('tanh.json')

    def prepare(self, arguments):
        self.base_prepare(arguments, 'tanh')
        return ""

    def run(self,openeojob, processOutput, processInput):
        return self.base_run(openeojob, processOutput, processInput)    

class ATanOperation(BaseUnarymapCalc):
    def __init__(self):
        self.loadOpenEoJsonDef('arctan.json')

    def prepare(self, arguments):
        self.base_prepare(arguments, 'atan')
        return ""

    def run(self,openeojob, processOutput, processInput):
        return self.base_run(openeojob, processOutput, processInput) 

class AbsOperation(BaseUnarymapCalc):
    def __init__(self):
        self.loadOpenEoJsonDef('absolute.json')

    def prepare(self, arguments):
        self.base_prepare(arguments, 'abs')
        return ""

    def run(self,openeojob, processOutput, processInput):
        return self.base_run(openeojob, processOutput, processInput)                  
    
class CeilOperation(BaseUnarymapCalc):
    def __init__(self):
        self.loadOpenEoJsonDef('ceil.json')

    def prepare(self, arguments):
        self.base_prepare(arguments, 'ceil')
        return ""

    def run(self,openeojob, processOutput, processInput):
        return self.base_run(openeojob, processOutput, processInput) 

class FloorOperation(BaseUnarymapCalc):
    def __init__(self):
        self.loadOpenEoJsonDef('floor.json')

    def prepare(self, arguments):
        self.base_prepare(arguments, 'floor')
        return ""

    def run(self,openeojob, processOutput, processInput):
        return self.base_run(openeojob, processOutput, processInput)

class IntOperation(BaseUnarymapCalc):
    def __init__(self):
        self.loadOpenEoJsonDef('int.json')

    def prepare(self, arguments):
        self.base_prepare(arguments, 'int')
        return ""

    def run(self,openeojob, processOutput, processInput):
        return self.base_run(openeojob, processOutput, processInput) 

class RoundOperation(BaseUnarymapCalc):
    def __init__(self):
        self.loadOpenEoJsonDef('round.json')

    def prepare(self, arguments):
        self.base_prepare(arguments, 'round')
        return ""

    def run(self,openeojob, processOutput, processInput):
        return self.base_run(openeojob, processOutput, processInput) 

class LnOperation(BaseUnarymapCalc):
    def __init__(self):
        self.loadOpenEoJsonDef('ln.json')

    def prepare(self, arguments):
        self.base_prepare(arguments, 'ln')
        return ""

    def run(self,openeojob, processOutput, processInput):
        return self.base_run(openeojob, processOutput, processInput)                       
    
class PowerOperation(BaseUnarymapCalc):
    def __init__(self):
        self.loadOpenEoJsonDef('power.json')

    def prepare(self, arguments):
        self.base_prepare(arguments, 'pow')
        return ""

    def run(self,openeojob, processOutput, processInput):
        return self.base_run(openeojob, processOutput, processInput)  

class SqrtOperation(BaseUnarymapCalc):
    def __init__(self):
        self.loadOpenEoJsonDef('sqrt.json')

    def prepare(self, arguments):
        self.base_prepare(arguments, 'sqrt')
        return ""

    def run(self,openeojob, processOutput, processInput):
        return self.base_run(openeojob, processOutput, processInput) 

class ExpOperation(BaseUnarymapCalc):
    def __init__(self):
        self.loadOpenEoJsonDef('exp.json')

    def prepare(self, arguments):
        self.base_prepare(arguments, 'exp')
        return ""

    def run(self,openeojob, processOutput, processInput):
        return self.base_run(openeojob, processOutput, processInput) 

class NotOperation(BaseUnarymapCalc):
    def __init__(self):
        self.loadOpenEoJsonDef('not.json')

    def prepare(self, arguments):
        self.base_prepare(arguments, 'not')
        return ""

    def run(self,openeojob, processOutput, processInput):
        return self.base_run(openeojob, processOutput, processInput)                  

def registerOperation():
     funcs = []
     funcs.append(ArcCosOperation())
     funcs.append(ArcCosHOperation())     
     funcs.append(CosOperation())     
     funcs.append(SinOperation()) 
     funcs.append(ASinOperation())
     funcs.append(ASinHOperation())      
     funcs.append(TanOperation()) 
     funcs.append(ATanOperation()) 
     funcs.append(AbsOperation()) 
     funcs.append(CeilOperation())                                     
     funcs.append(IntOperation())      
     funcs.append(RoundOperation()) 
     funcs.append(CosHOperation())          
     funcs.append(SinHOperation())      
     funcs.append(TanHOperation())
     funcs.append(LnOperation())             
     funcs.append(PowerOperation())
     funcs.append(SqrtOperation())    
     funcs.append(ExpOperation())     
     funcs.append(NotOperation())  

     return funcs