from workflow.estimationnode import EstimationNode
from openeooperation import *
from constants import constants
from workflow.nodeexecution import NodeExecution
from graphoptimize import analyzeGraph


class ProcessNode :
    constants.UNDEFINED = 0
    OPERATION = 1
    JUNCTION = 2
    CONDITION = 3

    def __init__(self, parentProcessGraph, nodeDict, nodeName):
        self.nodeName = nodeName ## just for easy identification; doesnt play a role on this level
        self.parentProcessGraph = parentProcessGraph # the owner of this process Node; might be a sub process
        for key, pValue in nodeDict.items():
            if key == 'process_id':
                self.process_id = pValue
            elif key == 'arguments':
                self.localArguments = {}
                for key, value in pValue.items():
                    self.localArguments[key] = { 'base' : value, 'resolved' : None}

            elif key == 'description':
                self.description = pValue
            elif key == 'result':
                self.result = pValue
               
            self.nodeType = ProcessNode.OPERATION 
            self.nodeValue = None
# the container for the actual process graph. A process graph is  split up into a sequence of
# nodes. One node is marked as the output node which creates the outptu of the graph. Process graphs are 
# strictly linear, no loops no branches
class ProcessGraph(OpenEoOperation):

    def __init__(self, source_graph, arguments, getOperation, subgraph=False):
        self.processGraph = {}
        self.outputNodes = []
        self.sourceGraph = analyzeGraph(source_graph,  subgraph)
        self.localArguments = {}
        self.processArguments = arguments        
        self.getOperation = getOperation
        self.startNode = None
        self.title = ''
        if source_graph:
            for processKey,processValues in self.sourceGraph.items():
                grNode = ProcessNode(self, processValues, processKey)
                self.processGraph[processKey] = grNode

                self.determineOutputNodes(self.processGraph)
    
          
    # helper function for the validatgraph method
    def addLocalArgument(self, key, value, index = 0):
          self.outputNodes[index][1].localArguments[key] = value

    def clearLocalArgument(self, index = 0):
        if len(self.outputNodes) > 0 and len(self.outputNodes) > index and len(self.outputNodes[index]) > 1:
            self.outputNodes[index][1].localArguments = {}
    
    def validateNode(self, node):
        errors = []
        for arg in node.localArguments.items():
                if ( arg[1]['resolved'] == None): # input needed from other node
                    base = arg[1]['base']
                    if isinstance(base, dict):
                        if 'from_node' in base:
                            fromNodeId = base['from_node']
                            backNode = self.id2node(fromNodeId)
                            if backNode[0] is not node.process_id: # prevent inf recursion
                                errors = errors + self.validateNode(backNode[1])
                   
        processObj = self.getOperation(node.process_id)
        if processObj == None:
             errors.append("missing \'operation\' " + node.process_id  )

        return errors             
    
    # validates a graph. this is basically indetical to running the graph without actually executing
    # any operations in it. As such it is limited to the input values as given when starting the process.
    def validateGraph(self):
            errors = []
            for node in self.outputNodes:
                errors = errors + self.validateNode(node[1])
            return errors                
                   
    def prepare(self, arguments):
        return ""
    
    # estimates the costs of a running a graph. atm the moment this is only a skeleton implemenations
    # as the sematics of costs have yet to be defined
    def estimate(self):
        try:
            for node in self.outputNodes:
                self.startNode = EstimationNode(node,self)
                return self.startNode.estimate()

        except Exception as ex:
            return createOutput(False, str(ex), constants.DTERROR)

    # executes the graph. Note in practice there is only one outputNode but basically it can handle multiple
    # output nodes
    def run(self,openeojob, toServer, fromServer ):
        for key, processNode in self.outputNodes:
            # a node execution instance starts 'parsing' the information stored in the process node
            # and tries to fill out all the unknowns in there
            self.startNode = NodeExecution(processNode,self)
            self.startNode.run(openeojob, toServer, fromServer)
            return self.startNode.outputInfo

    # stops the running of the process graph    
    def stop(self):
        if self.startNode != None:
            self.startNode.stop()

    def processGraph(self):
        return self.sourceGraph
    
    # translates the a given id to an actual graphNode. All nodes have a unique id (for this graph)
    def id2node(self, id):
        if isinstance(id, list):
            if  len(id) == 0:
                return None
            nodes = []
            for node in self.processGraph.items():
                if node[0] == id:
                    nodes.append(node)
            return node
        
        for node in self.processGraph.items():
            if node[0] == id:
                return node
        return None            
    # an output node is identified by havingf a 'result' attribute
    def determineOutputNodes(self, nodes):
        self.outputNodes = []
        for node in nodes.items():
            k = node[1]
            pp = k.__dict__.keys()
            for name in pp:
                if name == 'result':
                    self.outputNodes.append(node)
                    self.startNode = node
        if len(self.outputNodes) != 1:
            self.outputNodes = []
            self.startNode = None
        return len(self.outputNodes) == 1

    def resolveParameter(self, parmKey):
        if parmKey in self.processArguments:
            return {'resolved': self.processArguments[parmKey]['resolved']}
        #assume its the process builder key/name which is unknown to us as its a client something
        return {'resolved': None}


