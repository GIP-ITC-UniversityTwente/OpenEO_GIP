from estimationnode import EstimationNode
from openeooperation import *
from constants import constants
import copy
import customexception
import rasterdata

mathKeys2 = {'add' : '+', 'divide' : '/', 'multiply' : '*', 'subtract' : '-', 'exp' : 'exp', 'sin': 'sin', 'cos' : 'cos',
             'arcsin': 'asin', 'arccos' : 'acos', 'pow': 'pow', 'sqrt' : 'sqrt', 'sqr': 'sqr','arctanh' : 'atanh', 
             'arccosh' : 'acosh', 'arcsinh' : 'asinh', 'floor' : 'floor', 'ceil' : 'ceil', 'log' : 'log10', 'ln' : 'ln', 'abs' : 'abs'
             , 'max' : 'max', 'min' : 'min', 'round' : 'round'}

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
        self.sourceGraph = self.analyzeGraph(source_graph,  subgraph)
        self.localArguments = {}
        self.processArguments = arguments        
        self.getOperation = getOperation
        self.startNode = None
        self.title = ''
        for processKey,processValues in self.sourceGraph.items():
            grNode = ProcessNode(self, processValues, processKey)
            self.processGraph[processKey] = grNode

        self.determineOutputNodes(self.processGraph)
    

    def analyzeGraph(self, sourceGraph, subgraph):
        if not subgraph:
            return sourceGraph
      
        k = list(sourceGraph.values())[-1]
        lastKey = list(sourceGraph)[-1]
        if isinstance(k, dict):
            expr, usedNodes = self.analyzeProcessGraph(k, sourceGraph,lastKey) 
            if len(usedNodes) > 0 and expr != '':
                graph = {} 
                from_nodes = []                           
                for nodeKey in sourceGraph:
                    if not nodeKey in usedNodes:
                        graph[nodeKey] = sourceGraph[nodeKey]
                    if expr.find(nodeKey) != -1:
                        from_nodes.append(nodeKey)                        
                graph['rastercalc1'] = {'process_id' : 'rastercalc', 'arguments' : { 'expression' : expr, 'v' : {'from_node' : from_nodes}}, 'result' : True}
                return graph
        return sourceGraph
    
    def analyzeProcessGraph(self, node, processGraph, nodeName):
        expr = ''
        usedNodes = []
        args = None
        if 'process_id' in node:
            if node['process_id'] in mathKeys2:
                args = node['arguments']
                oper = mathKeys2[node['process_id']]
            if args != None:
                usedNodes.append(nodeName)
                for argkey in args:
                    arg = args[argkey]
                    if isinstance(arg, dict):
                        v = self.argValue(arg)
                        gnode = processGraph[v]
                        v1, e = self.analyzeProcessGraph(gnode, processGraph,v)
                        v = v1 = v if v1 == '@' else v1
                        usedNodes.extend(e)
                    else:
                        v = arg

                    if len(args) == 1:
                        expr = expr + oper + '(' + str(v) + ')'
                    else:                             
                        expr = expr + str(v) + oper
                    oper = '' # is used, don't use again
                if len(expr) > 0:
                    expr = '('+ expr + ')'
            else:
                expr = '@' 

        return expr, usedNodes
    
    def argValue(self, arg):
        if isinstance(arg, dict):
            k = next(iter(arg))
            v = next(iter(arg.values()))
            if k == 'from_node':
                return v
        else:
            return arg
        
        return None            
    # helper function for the validatgraph method
    def addLocalArgument(self, key, value, index = 0):
          self.outputNodes[index][1].localArguments[key] = value

    def clearLocalArgument(self, index = 0):
        if len(self.outputNodes) > 0 and len(self.outputNodes[index]) > 1:
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
    def run(self, openeojob, toServer, fromServer):
        """
        Executes the process node by resolving its parameters and running the associated operation.

        Args:
            openeojob: The OpenEO job object.
            toServer: The server object for communication.
            fromServer: The server object for receiving responses.
        """
        self._resolveParameters(openeojob, toServer, fromServer)
        # if we arrive at this stage all parameters have now a value(resolved) and we can execute the node
        # each node has a process_id which should be the name of a defined process on the server. e.g. load_collection.
        # if not the executions stops and the process graph fails        
        self._executeNode(openeojob, toServer, fromServer)


    def _resolveParameters(self, openeojob, toServer, fromServer):
        """
        Resolves all parameters of the process node.

        Args:
            openeojob: The OpenEO job object.
            toServer: The server object for communication.
            fromServer: The server object for receiving responses.
        """
        args = self.processNode.localArguments
        for key, parmDef in args.items():
            # if a graph is called multiple times job_id and serverChannel may be in the list of already
            # present parameters. They are 'hidden' parameters and can be ignored here. They are plain values not dicts            
            if not isinstance(parmDef, dict):
                continue
            if parmDef['resolved'] is None:
                parmDefinition = parmDef['base']
                resolvedValue = self._resolveParameter(openeojob, toServer, fromServer, key, parmDefinition)
                args[key]['resolved'] = resolvedValue

    def _resolveParameter(self, openeojob, toServer, fromServer, key, parmDefinition):
        """
        Resolves a single parameter based on its definition.

        Args:
            openeojob: The OpenEO job object.
            toServer: The server object for communication.
            fromServer: The server object for receiving responses.
            key: The parameter key.
            parmDefinition: The parameter definition.

        Returns:
            The resolved value of the parameter.
        """


        if isinstance(parmDefinition, dict):
            # the unresolved parameter is complex and of dict form. We now must try understand
            # what is defined in the dict
            return self._resolveComplexParameter(openeojob, toServer, fromServer, key, parmDefinition)
        elif isinstance(parmDefinition, list):
            return self._resolveListParameter(openeojob, toServer, fromServer, key, parmDefinition)
        else:
            return self._resolveNode(openeojob, toServer, fromServer, (key, parmDefinition))
        
    def _resolveComplexParameter(self, openeojob, toServer, fromServer, itemkey, parmDefinition):
        """
        Resolves a complex parameter defined as a dictionary.

        Args:
            openeojob: The OpenEO job object.
            toServer: The server object for communication.
            fromServer: The server object for receiving responses.
            parmDefinition: The parameter definition.

        Returns:
            The resolved value of the parameter.
        """
        for item in parmDefinition.items():
            # an item in the dict maybe of the indirect form; meaning they refer to a previous calculated
            # or set value. this refers to the from_parameter and from_node form            
            if item[0] in self.indirectKeys:
                return self._resolveNode(openeojob, toServer, fromServer, item)
            else: # mostely direct value. The reason why the direct value isn't set directly is
                  # that if the value is a complex form the resolveNode will still calculate its value
                  # correctly. In practice though this will often be single direct value 
                return self._resolveNode(openeojob, toServer, fromServer, (itemkey, parmDefinition))
            
    def _resolveListParameter(self, openeojob, toServer, fromServer, key, parmDefinition):
        """
        Resolves a parameter defined as a list.

        Args:
            openeojob: The OpenEO job object.
            toServer: The server object for communication.
            fromServer: The server object for receiving responses.
            key: The parameter key.
            parmDefinition: The parameter definition.

        Returns:
            A list of resolved values.
        """
        resolvedValues = []
        resolvedValue = []
        for elem in parmDefinition:
            if isinstance(elem, dict) and not isinstance(elem, rasterdata.RasterData):
                # similar to the earlier 'dict' case but now the calculated values
                # are aggregated into a list
                for item in elem.items():
                    if item[0] in self.indirectKeys:
                        resolvedValues.append(self._resolveNode(openeojob, toServer, fromServer, item))
                    else:
                        resolvedValues.append(self._resolveNode(openeojob, toServer, fromServer, (key, parmDefinition)))
                resolvedValue = resolvedValues  						
            else: # direct value; see comments in the dict branch as this is a similar case 
               resolvedValue = self._resolveNode(openeojob, toServer, fromServer, (key, parmDefinition))
        return resolvedValue 

    def _executeNode(self, openeojob, toServer, fromServer):
        """
        Executes the process node after all parameters have been resolved.

        Args:
            openeojob: The OpenEO job object.
            toServer: The server object for communication.
            fromServer: The server object for receiving responses.
        """
        processObj = self.processGraph.getOperation(self.processNode.process_id)
        if processObj is not None:
            # we make a deep copy of the predefined process as we are going to fill in values and we don't
            # want the system definition of the process to be poluted            
            executeObj = copy.deepcopy(processObj)

            self._prepareAndRunNode(executeObj, openeojob, toServer, fromServer)
        else: # we couldn't find the operation. Execution of the process graph will stop
            message = f"Unknown operation {self.processNode.process_id}. This operation is not implemented on the server."
            self.handleError(openeojob, message, 'operation')   

    def _prepareAndRunNode(self, executeObj, openeojob, toServer, fromServer):
        """
        Prepares and runs the process node.

        Args:
            executeObj: The process object to execute.
            openeojob: The OpenEO job object.
            toServer: The server object for communication.
            fromServer: The server object for receiving responses.
        """
        args = self.processNode.localArguments
        args['serverChannel'] = toServer
        args['job_id'] = openeojob.job_id

        executeObj.prepare(args)
         # the result of prepare should be that a process is now runnable
        if executeObj.runnable:
            self.outputInfo = executeObj.run(openeojob, toServer, fromServer)
    
  
    def _resolveNode(self, openeojob, toServer, fromServer, parmKeyValue):
        """
        resolve node does the actual 'resolving'. There are three case here
        1) It is a 'from_node' case. in this case the requested value comes from another node
            as it is a referrer the (referred) node will not be calculated else it would have been a direct value
            so we simply locate the node and execute it.
        2) It is a parameter of the (sub)process. That means the value must be present in the 'parent' (sub)process
            as that process calls this (sub) process. It maybe resolved or not. If resolved we can return it if not
            we simply resolve the parameter and return the result
        3) direct value. No resolves needed
            Note that the system has been setup in this way to be flexible in handling different cases/uses of process graphs. 
            process graphs are not only the one that starts an openeo operartion but can also be parameters of individual 
            sub processes which first have to execute (and give a resolved value) before a sub process can execute

        Args:
            openeojob: The OpenEO job object.
            toServer: The server object for communication.
            fromServer: The server object for receiving responses.
            parmKeyValue: A tuple containing the parameter key and its value.

        Returns:
            The resolved value of the node.
        """
        if 'from_node' in parmKeyValue:
            return self._resolveFromNode(openeojob, toServer, fromServer, parmKeyValue[1])
        elif 'from_parameter' in parmKeyValue:
            return self._resolveFromParameter(parmKeyValue[1])
        else:
            return parmKeyValue[1]  # Direct value, no resolution needed

    def _resolveFromNode(self, openeojob, toServer, fromServer, referredNodeName):
        """
        Resolves a value from another node through instantation of an ExecutionNode to backtrace and construct its implied value.

        Args:
            openeojob: The OpenEO job object.
            toServer: The server object for communication.
            fromServer: The server object for receiving responses.
            referredNodeName: The name of the referred node.

        Returns:
            The resolved value of the referred node.
        """
        #wrap it in a list if needed to get same handling for cases.
        refvalues = referredNodeName if isinstance(referredNodeName, list) else [referredNodeName]
        nodeValues = {}

        for refvalue in refvalues:
            referredNode = self.processGraph.id2node(refvalue)
            if referredNode:
                if referredNode[1].nodeValue is None:
                    # Execute the referred node to resolve its value
                    refExecutionNode = NodeExecution(referredNode[1], self.processGraph)
                    refExecutionNode.run(openeojob, toServer, fromServer)
                    referredNode[1].nodeValue = refExecutionNode.outputInfo
                nodeValues[refvalue] = referredNode[1].nodeValue['value']
            else:
                self.handleError(openeojob, "Node cannot be found", 'resolved node')

        return next(iter(nodeValues.values())) if len(nodeValues) == 1 else nodeValues

    def _resolveFromParameter(self, parameterName):
        """
        Resolves a value from a parameter in the parent process graph.

        Args:
            parameterName: The name of the parameter to resolve.

        Returns:
            The resolved value of the parameter, or None if it cannot be resolved.
        """
        refNode = self.processNode.parentProcessGraph.resolveParameter(parameterName)
        return refNode['resolved'] if refNode['resolved'] is not None else None

