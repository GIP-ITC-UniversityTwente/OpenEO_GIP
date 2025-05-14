import common
import copy
import customexception
import datacube
import logging
import openeologging
from constants import constants

class NodeExecution :

    def __init__(self, processNode, processGraph):
        self.processNode = processNode
        self.processGraph = processGraph
        self.outputInfo = None
        self.indirectKeys = ['from_parameter', 'from_node']

    def handleError(self, openeojob, message, type):
        openeologging.logMessage(logging.ERROR, message, openeojob.user.username )
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
            if isinstance(elem, dict) and not isinstance(elem, datacube.DataCube):
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
        if refNode is None:
            return None
        # Check if the parameter is resolved
        return refNode['resolved'] if refNode['resolved'] is not None else None
