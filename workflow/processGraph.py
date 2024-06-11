from estimationnode import EstimationNode
from openeooperation import *
from constants import constants
import copy
import customexception
import rasterdata


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

    def __init__(self, source_graph, arguments, getOperation):
        self.processGraph = {}
        self.outputNodes = []
        self.sourceGraph = source_graph
        self.processArguments = arguments
        self.localArguments = {}
        self.getOperation = getOperation
        self.startNode = None
        self.title = ''
        for processKey,processValues in source_graph.items():
            grNode = ProcessNode(self, processValues, processKey)
            self.processGraph[processKey] = grNode

        self.determineOutputNodes(self.processGraph)

    # helper function for the validatgraph method
    def addLocalArgument(self, key, value, index = 0):
          self.outputNodes[index][1].localArguments[key] = value
    
    def validateNode(self, node):
        errors = []
        for arg in node.localArguments.items():
                if ( arg[1]['resolved'] == None): # input needed from other node
                    base = arg[1]['base']
                    if isinstance(base, dict):
                        if 'from_node' in base:
                            fromNodeId = base['from_node']
                            backNode = self.id2node(fromNodeId)
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
        for node in self.processGraph.items():
            if node[0] == id:
                return node
        return None            
    # an output node is identified by havingf a 'result' attribute
    def determineOutputNodes(self, nodes):
        for node in nodes.items():
            if hasattr(node[1], 'result'):
                self.outputNodes.append(node) 

    def resolveParameter(self, parmKey):
        if parmKey in self.processArguments:
            return {'resolved': self.processArguments[parmKey]['resolved']}
        #assume its the process builder key/name which is unknown to us as its a client something
        return {'resolved': None}

class NodeExecution :

    def __init__(self, processNode, processGraph):
        self.processNode = processNode
        self.processGraph = processGraph
        self.outputInfo = None
        self.indirectKeys = ['from_parameter', 'from_node']

    def handleError(self, openeojob, message, type):
        common.logMessage(logging.ERROR, message, openeojob.user.username )
        raise customexception.CustomException(constants.ERROROPERATION, self.processNode.process_id,  type, message)        

    # this method together with  the support method resolveNode do the heavy lifting for executing
    # the process graph. One can see an execution node as a sub process with a input(s) and one output of the 
    # whole graph. Each input maybe 'resolved' (has an actual value) or not. If it is not resolved resolvedNode 
    # method will be used to find its values (see there for logic). Based on the form of the (unresolved) parameter 
    # in the 'base' definition a ptah will be chosen how to resolve the (unresolved) parameter. This is a recursive
    # process as the unresolved parameter often comes from a previous, yet unexecuted, ExecutioNode. In this way the 
    # execution trickles up to chain until it arrives at a node from which all the parameters are resolved. It
    # can then execute the node and make the parameter 'resolved' with an actual value. Now the previous node in 
    # the chain can execute. etc..
    def run(self,openeojob, toServer, fromServer):
        args = self.processNode.localArguments
        for key, parmDef in args.items():
            if parmDef['resolved'] == None:
                parmDefinition = parmDef['base']
                if isinstance(parmDefinition, dict):
                   # the unresolved parameter is complex and of dict form. We now must try understand
                   # what is defined in the dict
                   for item in parmDefinition.items():
                        # an item in the dict maybe of the indirect form; meaning they refer to a previous calculated
                        # or set value. this refers to the from_parameter and from_node form
                        if item[0] in self.indirectKeys:
                            resolvedValue = self.resolveNode(openeojob, toServer, fromServer, item)
                        else: # mostely direct value. The reason why the direct value isn't set directly is
                            # that if the value is a complex form the resolveNode will still calculate its value
                            # correctly. In practice though this will often be single direct value           
                            resolvedValue = self.resolveNode(openeojob, toServer, fromServer, (key, parmDefinition)) 
                else:
                    resolvedValues = []
                    resolvedValue = []
                    # the unresolved parameter is complex and of list form. We now must try understand
                    # what is defined in the list. Often this is simply a list of direct values
                    if isinstance(parmDefinition, list):
                        for elem in parmDefinition:
                            if isinstance(elem, dict) and not isinstance(elem, RasterData):
                                # similar to the earlier 'dict' case but now the calculated values
                                # are aggregated into a list
                                for item in elem.items():
                                    if item[0] in self.indirectKeys:
                                        rv = self.resolveNode(openeojob, toServer, fromServer, item)
                                        resolvedValues.append(rv)
                                    else:            
                                        rv = self.resolveNode(openeojob, toServer, fromServer, (key, parmDefinition))   
                                        resolvedValues.append(rv)
                                resolvedValue = resolvedValues                                                              
                            else: # direct value; see comments in the dict branch as this is a similar case                                   
                                resolvedValue = self.resolveNode(openeojob, toServer, fromServer, (key, parmDefinition))  

                    else: # a direct value
                        resolvedValue = self.resolveNode(openeojob, toServer, fromServer, (key, parmDefinition))  
                     
                args[key]['resolved'] = resolvedValue
        # if we arrive at this stage all parameters have now a value(resolved) and we can execute the node
        # each node has a process_id which should be the name of a defined process on the server. e.g. load_collection.
        # if not the executions stops and the process graph fails
        processObj = self.processGraph.getOperation(self.processNode.process_id)
        if  processObj != None:
            # we make a deep copy of the predefined process as we are going to fill in values and we don't
            # want the system definition of the process to be poluted
            executeObj =  copy.deepcopy(processObj)
            # adding some 'system' parameters. They are not strictly needed for the process but facilitate
            # communication services and error handling with the rest of the server
            args['serverChannel'] = toServer
            args['job_id'] = openeojob.job_id
            # the prepare checks if all the parameters are valid. A resolved value might still be nonsense within
            # the semantics of the process. This also makes the run only concentrate on 'running' the node and not
            # to worry about validity of the input.
            executeObj.prepare(args)
            # the result of prepare should be that a process is now runnable
            if  executeObj.runnable:
                self.outputInfo = executeObj.run(openeojob, toServer, fromServer) 
        else: # we couldn't find the operation. Execution of the process graph will stop
            message = 'unknow operation ' + str(self.processNode.process_id + ". This operation is not implemented on the server")
            self.handleError(openeojob, message, 'operation')

    # resolve node does the actual 'resolving'. There are three case here
    # 1) It is a 'from_node' case. in this case the requested value comes from another node
    #    as it is a referrer the (referred) node will not be calculated else it would have been a direct value
    #    so we simply locate the node and execute it.
    # 2) It is a parameter of the (sub)process. That means the value must be present in the 'parent' (sub)process
    #    as that process calls this (sub) process. It maybe resolved or not. If resolved we can return it if not
    #    we simply resolve the parameter and return the result
    # 3) direct value. No resolves needed
    # Note that the system has been setup in this way to be flexible in handling different cases/uses of process graphs. 
    # process graphs are not only the one that starts an openeo operartion but can also be parameters of individual 
    # sub processes which first have to execute (and give a resolved value) before a sub process can execute
    def resolveNode(self,openeojob, toServer, fromServer, parmKeyValue):
        if 'from_node' in parmKeyValue: # value is value of another node
            referredNodeName = parmKeyValue[1]
            referredNode = self.processGraph.id2node(referredNodeName) # find the node
            if referredNode != None:
                if referredNode[1].nodeValue == None:
                    # create a new execution node based on the found node and run it to get a resolved value
                    # in this way the nodeVlaue will be filled and subsequent calls will use tha already 
                    # calcualted value
                    refExecutionNode = NodeExecution(referredNode[1], self.processGraph)
                    refExecutionNode.run(openeojob, toServer, fromServer)
                    referredNode[1].nodeValue = refExecutionNode.outputInfo
                return referredNode[1].nodeValue['value']
            else: # should never happen, but anyway
                self.handleError(openeojob, "Node can not be found", 'resolved node')
        # a value that has been set for this sub process. The previous case refers to another node, 
        # this case the actual value refers to a value present in the calling process (which might be resolved or not)
        elif 'from_parameter' in parmKeyValue: 
                # get the value 
                refNode = self.processNode.parentProcessGraph.resolveParameter(parmKeyValue[1])
                # if it is resolved we are done
                if refNode['resolved'] != None:
                    return refNode['resolved'] 
                # if not we cant resolve it
                return None #self.resolveNode(openeojob, toServer, fromServer, refNode)  
       
        else: # direct value case; no indirections
            return parmKeyValue[1]                                              