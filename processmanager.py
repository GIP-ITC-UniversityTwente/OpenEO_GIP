import threading
from multiprocessing import Process, Queue
from datetime import datetime
from constants import constants
import common
import pickle
from pathlib import Path
import os
from authenticationdatabase import authenticationDB

lockLogger = threading.Lock()

def linkSection(begin, end):
        return {
                "href" :  begin + "/" + end,
                "rel" : 'self',
                "type" : "application/json"
            }
def makeBaseResponseDict(job_id, status, code, baseurl = None, message=None) :
    if status == constants.STATUSUNKNOWN:
        process = globalProcessManager.getProcess(None, job_id)
        if  process != None:
            status = process.status

    res = { "id" : job_id,
            "code" : code,
            "status" : status,
            "submitted" : str(datetime.now()),
        }
    if baseurl != None:
        res['links'] = linkSection(baseurl, job_id)
        
    if message != None:
        res['message'] = message
    return res

def ErrorResponse(id, code, message):
        return { "id" : id, "code" : code, "message" : message}

def worker(openeoprocess, outputQueue):
    openeoprocess.status = constants.STATUSRUNNING
    openeoprocess.run(outputQueue)
    print("done")

class OutputInfo:
    def __init__(self, eoprocess):
        self.eoprocess = eoprocess
        self.pythonProcess = None
        self.progress = 0
        self.last_updated = datetime.now()
        self.output = None
        self.status = constants.STATUSQUEUED
        self.logs = []
        self.code = ''
        self.message = ''
        self.availableStart = None
        self.current_operation = '?'

    def isFinished(self):
        return self.progress == 1

class ProcessManager:
    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(ProcessManager, cls).__new__(cls)
        return cls.instance
      
    def __init__(self):
        self.lockProcessQue = threading.Lock()
        self.lockOutput = threading.Lock()
        self.lockLogger = threading.Lock()
        self.processQueue  = []
        self.outputs = {}
        self.outputQueue = Queue()
        self.running = True


    def addProcess(self, eoproces):
        with self.lockProcessQue:
            self.processQueue.append( eoproces)

    def createNewEmptyOutput(self, eoprocess):
        with self.lockOutput:
            self.outputs[str(eoprocess.job_id)] = OutputInfo(eoprocess)

    def addLog4job(self, job_id, logCount, level, message, data=None, usage=None, links=None):
            self.outputQueue.put({'type' : 'loggingevent', 'job_id': job_id, 'id' : logCount, 'level' : level, 'message' : message, 'time' : str(datetime.now())})

    def setOutput(self, id, output):
        with self.lockOutput:
            self.outputs[str(id)].output = output

    def addOutputProgress(self, id, progress):
        with self.lockOutput:
            self.outputs[str(id)].progress = progress

    def queueJob(self, user, job_id):
         with self.lockProcessQue:
            idx = -1
            for i in range(len(self.processQueue)):
                if str(self.processQueue[i].job_id) == job_id:
                   idx = i
                   break
                   
            if idx != -1:
                if self.processQueue[i].user == user:
                    if self.processQueue[i].status == constants.STATUSCREATED:                    
                        self.processQueue[i].status = constants.STATUSQUEUED
                        return "Job " + str(job_id) + " is queued",""
                    else:
                        return "Job doesnt have correct status :"  + self.processQueue[i].status,constants.CUSTOMERROR
                else:
                    return "Job is owned by a different user", constants.CUSTOMERROR
            else: # its no longer in the processqueue so it might have shifted to the output list
                for jobb_id, item in self.outputs: 
                    if str(jobb_id) == job_id: #if its there the client shouldn't ask for queuing as it is already running/done/canceled
                        return "Job doesnt have correct status :"  + item.status, constants.CUSTOMERROR
                    
            return "Job isn't present in the system","JobNotFound"
                     
    def stopJob(self, job_id, user):
        with self.lockOutput:
            for key,value in self.outputs.items():
                if value.eoprocess.user == user:
                    if job_id == str(value.eoprocess.job_id):
                        value.eoprocess.stop()
                        self.outputs[job_id].pythonProcess.terminate()
                        self.outputs[job_id].status = constants.STATUSSTOPPED 
                        return
                            

    def makeEstimate(self, user, job_id):
        eoprocess = self.getProcess(user, job_id)
        if eoprocess != None:
            return (eoprocess.estimate(user), 200)
        return ({'id' : job_id, 'code' : 'job not found'}, 400)

    def removedCreatedJob(self, job_id):
        for i in range(len(self.processQueue)):
            if str(self.processQueue[i].job_id) == job_id:
                if self.processQueue[i].status == constants.STATUSCREATED:
                    self.processQueue.pop(i)
                    return constants.STATUSCREATED
                else:
                    return constants.STATUSQUEUED
        return constants.STATUSUNKNOWN                
    
    def getProcess(self, user, job_id):
        for i in range(len(self.processQueue)):
            if str(self.processQueue[i].job_id) == job_id:
                return self.processQueue[i]
        for jobb_id, item in self.outputs: 
            if str(jobb_id) == job_id:
                return item.eoprocess
        return None            

    # creates a dict which contains meta infortmation for a certain job. This function is used by the
    # http API (jobs/{id}) to report status to python client (or any client)
    def allJobsMetadata4User(self, user, job_id, baseurl):
        with self.lockOutput:
            processes = []  
            for key,value in self.outputs.items():
                if value.eoprocess.user == user or user.username == 'undefined':
                    if job_id == None or ( job_id == str(value.eoprocess.job_id)):
                        dict = value.eoprocess.toDict( job_id == None)
                        dict['haserror'] = False
                        ## the operation actually running is piggybagged in the progess string as
                        ## this string is in practice less usefull as it is on a polling timer so progress may be 'lost' as message
                        if value.current_operation != '?':
                           dict['progress'] = value.current_operation
                        else:                            
                            dict['progress'] = value.progress
                        dict['updated'] = str(value.last_updated)
                        dict['status'] = value.status
                        if value.status == constants.STATUSJOBDONE:
                            dict['status'] = 'finished'
                            dict['progress'] = 'Job done'
                        if value.status == constants.STATUSERROR:
                            dict['status'] = 'finished'
                            dict['progress'] = 'Job done'
                            dict['haserror'] = True
                            dict['message'] = value.message
                            dict['code'] = value.code
                        dict['job_id'] = value.eoprocess.job_id
                        dict['submitted'] = value.eoprocess.submitted
                        dict["links"]  = {
                            "href" :  baseurl + "/" + value.eoprocess.job_id,
                            "rel" : 'self',
                            "type" : "application/json"
                            }
                        processes.append(dict)
            if job_id == None: ## case were a list of metadata is requested 
                return processes 
            if len(processes) == 1:                                       
                return processes[0] # case were only on job is queried   
            return None            

    def alllogs4job(self, user, jobid):
        with self.lockOutput:

            for key,value in self.outputs.items():
                if value.eoprocess.user == user:
                    if  jobid == str(value.eoprocess.job_id):
                        logs = []
                        for log in value.logs:
                            logEntry = {}
                            logEntry['timestamp'] = log.timestamp
                            logEntry['message'] = log.message
                            logEntry['level'] = log.level
                            logs.append(logEntry) 
                        return logs                           
        return []                    
    
    
    def stop(self):
        self.running = False

    def removeFromOutputs(self, key, whenTimer):
        endTimer = datetime.now()
        delta2 = endTimer - self.outputs[key].last_updated
        if delta2.seconds > whenTimer:
            out = self.outputs.pop(key)
            out.cleanUp()

    def reduceLogFile(self):
        lockLogger.acquire()
        logpath = os.path.join(os.path.dirname(__file__), 'log')
        filepath = "{0}/{1}.log".format(logpath, 'openeoserver' )
        lineCount = 0
        with open(filepath, 'r') as fp:
            lines = fp.readlines()
            lineCount = len(lines)
        if lineCount > 1000:         
            with open(filepath, 'w') as fp:
                limit  = 500
                for number, line in enumerate(lines):
                    if number > limit:
                        fp.write(line) 
        lockLogger.release()                           

    # this is the main function that handels process graph management. It starts, communicates with and stops
    # the process graphs that are requested. When created processes are put on a queue and started when there
    # is 'room' to do so. At the moment this is always, but might be more constrained in the future.
    def startProcesses(self):
        self.loadProcessTables()
        startCheckRemoveOutput = startTimerCheckTokens = startTimerDump = datetime.now()            
        while self.running:
            eoprocess = None
            # get a requested process of the process queue
            with self.lockProcessQue:
                if not len(self.processQueue) == 0:
                    for p in self.processQueue:
                        if p.status == constants.STATUSQUEUED:
                            eoprocess = self.processQueue.pop()
                            break
            if eoprocess != None:
                # create a new process objects and create its output representation. Once the process
                # is started basicall all info about the running process is in the output object. This includes
                # log information, errors and ofc output products
                p = Process(target=worker, args=(eoprocess,self.outputQueue))
                self.createNewEmptyOutput(eoprocess)
                # the process gets now the status of running, before it was queued
                self.outputs[str(eoprocess.job_id)].status = constants.STATUSRUNNING
                self.outputs[str(eoprocess.job_id)].pythonProcess = p
                # start the process
                p.start()
            if self.outputQueue.qsize() > 0:
                # see if there is any communication from processes. This is stored in 'items'
                # which contain the necessary info about what the information is to which process]
                # it belongs. one by one they are removed from the outputque and used to
                # update the output objects
                item = self.outputQueue.get()
                self.changeOutputStatus(item)
            endTimer = datetime.now()
            delta = endTimer - startTimerDump
            if delta.seconds > 120:
                # for the moment not relevant
                self.dumpProcessTables()
                startTimerDump = endTimer
            delta = endTimer - startTimerCheckTokens
            # next section will remove security tokens if they are too long in the outputs
            if delta.days > 1:
                authenticationDB.clearOutOfDateTokes()
                startTimerCheckTokens = endTimer
            delta = endTimer - startCheckRemoveOutput
            if delta.seconds > 60 * 60: 
                ## as we are going to delete items in the outputs we iterate over a copy to prevent undef behavior 
                for key,value in list(self.outputs.items()):
                    if self.outputs[key].status == constants.STATUSSTOPPED:
                        self.removeFromOutputs(key, 60*30)
                    if self.outputs[key].status == constants.STATUSERROR:
                        self.removeFromOutputs(key,60*60*24 )
                    if self.outputs[key].status == constants.STATUSFINISHED:
                        self.removeFromOutputs(key,60*60*24*4 )   
                startCheckRemoveOutput = endTimer 
                    

    def loadProcessTables(self):
        path = common.openeoip_config['data_locations']['system_files']['location']
        path1 = Path(path + '/processqueue.bin')
        if path1.is_file():
            with open(path1, 'rb') as f:
                self.processQueue = pickle.load(f)
        path2 = Path(path + '/processoutputs.bin')
        if os.path.exists(path2):
            file_stats = os.stat(path2)
            if file_stats.st_size > 0:
                if path2.is_file() :
                    with open(path2, 'rb') as f:
                        
                        dump = pickle.load(f)
                        for output in dump.items():
                            if output[1].status == constants.STATUSJOBDONE:
                                self.outputs[output[1].eoprocess.job_id] = output[1]
                            elif output[1].status == constants.STATUSQUEUED:
                                self.processQueue.append(output[1].eoprocess)
                            elif output[1].status == constants.STATUSRUNNING:
                                self.processQueue.append(output[1].eoprocess)                        

    # based on the information from the processes the values in the outputs list will be updated
    # note that other API calls (http api that is) do use this table (outputs) to figure out what 
    # the current status of a certain process is
    def changeOutputStatus(self, item):
        with self.lockOutput:
            job_id = item['job_id']
            if job_id in self.outputs:
                type = item['type']
                if type == 'progressevent':
                    self.outputs[job_id].progress = item['progress']
                    self.outputs[job_id].last_updated = datetime.now()
                    self.outputs[job_id].current_operation = item['current_operation']
                    if item['status'] == constants.STATUSJOBDONE:
                        self.outputs[job_id].status = constants.STATUSJOBDONE
                    if item['status'] == constants.STATUSERROR:
                        self.outputs[job_id].status = constants.STATUSERROR  
                        if 'message' in item:
                            self.outputs[job_id].message = item['message']
                        if 'code' in item :                           
                            self.outputs[job_id].code = item['code']
                if type == 'logginevent':
                    del item['type']
                    self.outputs[job_id].logs.append(item)

    def dumpProcessTables(self):
        #for the moment disabled
        """
        path = common.openeoip_config['data_locations']['system_files']['location']
        if len(self.processQueue) > 0:
            path1 = path + '/processqueue.bin'
            with open(path1, 'wb') as f:
                pickle.dump(self.processQueue, f)
        if len(self.outputs) > 0:                
            path2 = path + '/processoutputs.bin'
            with open(path2, 'wb') as f:
                pickle.dump(self.outputs, f) """

globalProcessManager  = ProcessManager()

