
mathKeys2 = {'add' : '+', 'divide' : '/', 'multiply' : '*', 'subtract' : '-', 'exp' : 'exp', 'sin': 'sin', 'cos' : 'cos',
             'arcsin': 'asin', 'arccos' : 'acos', 'pow': 'pow', 'sqrt' : 'sqrt', 'sqr': 'sqr','arctanh' : 'atanh', 
             'arccosh' : 'acosh', 'arcsinh' : 'asinh', 'floor' : 'floor', 'ceil' : 'ceil', 'log' : 'log10', 'ln' : 'ln', 'abs' : 'abs'
             , 'max' : 'max', 'min' : 'min', 'round' : 'round'}

def argValue(arg):
    if isinstance(arg, dict):
        k = next(iter(arg))
        v = next(iter(arg.values()))
        if k == 'from_node':
            return v
    else:
        return arg
        
def analyzeProcessGraph(node, processGraph, nodeName):
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
                    v = argValue(arg)
                    gnode = processGraph[v]
                    v1, e = analyzeProcessGraph(gnode, processGraph,v)
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

def analyzeGraph(sourceGraph, subgraph):
        if not subgraph:
            return sourceGraph
      
        k = list(sourceGraph.values())[-1]
        lastKey = list(sourceGraph)[-1]
        if isinstance(k, dict):
            expr, usedNodes = analyzeProcessGraph(k, sourceGraph,lastKey) 
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
    
