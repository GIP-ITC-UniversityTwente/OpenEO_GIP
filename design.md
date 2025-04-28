1. [OpenEO for the ITC](#openeo-for-the-itc)
   1. [OpenEO API](#openeo-api)
   2. [Python Implementation](#python-implementation)
      1. [Flask Pattern](#flask-pattern)
      2. [API handlers](#api-handlers)
   3. [Backend structures](#backend-structures)
      1. [raster\_database](#raster_database)
      2. [ProcessManager](#processmanager)
         1. [registering jobs](#registering-jobs)
         2. [Start a registered Job](#start-a-registered-job)
         3. [Communication](#communication)
      3. [openip\_config](#openip_config)
      4. [OpenEOProcess](#openeoprocess)
      5. [Operations](#operations)
   4. [Processing Backend](#processing-backend)
      1. [ProcessGraph](#processgraph)
         1. [NodeExecution](#nodeexecution)
      2. [DataCube](#datacube)
      3. [Raster Iterators](#raster-iterators)
      4. [Memory for rasters](#memory-for-rasters)
      5. [Raster Processing](#raster-processing)

# OpenEO for the ITC

## OpenEO API
The following part of the OpenEO web API(https://api.openeo.org/) is implemented.

*table 2: Implemented API calls*
| API Call                                  | Method | Implementation | Description |
|-------------------------------------------|--------|-----|-------------|
| `/collections`                            | GET    | opencollections.py | Lists available collections with at least the required information. This endpoint is compatible with STAC API 0.9.0 and later OGC API - Features 1.0. STAC API extensions and STAC extensions can be implemented in addition to what is documented here |
| `/collections/{collection_id}`            | GET    | opencollections.py | Lists all information about a specific collection specified by the identifier `collection_id` |
| `/collections/{col_id}/queryables`        | GET    | -  |  |
| `/processes`                              | GET    | openeoprocessdiscovery.py | Lists all predefined processes and returns detailed process descriptions, including parameters and return values |
| `/process_graphs`                         | GET    | openeoprocessgraphs | Lists all user-defined processes (process graphs) of the authenticated user stored at the back end |
| `/process_graphs/{process_graph_id}`      | GET    | openeoproccessgraph.py | Lists all information about a user-defined process, including its process graph |
| `/validation`                             | POST   | openeovalidate.py | Validates a user-defined process without executing it. |
| `/process_graphs/{process_graph_id}`      | PUT    | openeoproccessgraph.py | Stores a provided user-defined process with a process graph that can be reused in other processes |
| `/process_graphs/{process_graph_id}`      | DELETE | -  | Deletes a process graph from the server |
| `/file_formats`                           | GET    | openeofileformats.py | Lists supported input and output file formats. Current implementation is a limited list though the server has support for a much wider range of formats (C) |
| `/files`                                  | GET    |openeofiles.py | List all files in the workspace. (E) |
| `/files/{path}`                           | GET    | -  | Download a file from the workspace |
| `/files/{path}`                           | PUT    | -  | Uploads a new file to the given path or updates an existing file if a file at the path exists |
| `/files/{path}`                           | DELETE | -  | Deletes an existing user-uploaded file specified by its path |
| `/process_graphs/{process_graph_id}`      | PUT    | openeoprocessgraphs.py | Stores a provided user-defined process with a process graph that can be reused in other processes |
| `/result`                                 | POST   | openeoresult.py | Executes a user-defined process directly (synchronously) and the result will be downloaded in the format specified in the process graph. The process is defined in the POST parameter JSON |
| `/jobs`                                   | GET    | openeojobs.py | Lists all batch jobs submitted by a user |
| `/jobs`                                   | POST   | openeojobs.py | Creates a new batch processing task (job) from one or more (chained) processes at the back end. The process is defined in the POST parameter |
| `/jobs/{job_id}`                          | PATCH  | -  | Modifies an existing job at the back end but maintains the identifier. Only possible for non-queued jobs |
| `/jobs/{job_id}/estimate`                 | GET    |openeojobs.py | Calculates an estimate for a batch job. Back-ends can decide to either calculate the duration, the costs, the size, or a combination of them. (A) |
| `/jobs/{job_id}/logs`                     | GET    | openeologs.py | Lists log entries for the batch job. (B) |
| `/jobs/{job_id}/results`                  | GET    | openeojobs.py | Lists signed URLs pointing to the processed files, usually after the batch job has finished |
| `/jobs/{job_id}/results`                  | POST   | openeojobs.py | Adds a batch job to the processing queue to compute the results. The POST parameter(s) describe the job that needs to be run |
| `/jobs/{job_id}/results`                  | DELETE | openeojobs.py | Cancels all related computations for this job at the back-end and removes result data |
| `/credentials/oidc`                       | GET    | -  | Lists the supported OpenID Connect providers (OP). OpenID Connect Providers MUST support OpenID Connect Discovery. (D) |
| `/credentials/basic`                      | GET    | authentication.py  | Checks the credentials provided through HTTP Basic Authentication according to RFC 7617 and returns an access token for valid credentials. (D) |
| `/me`                                     | GET    | -  | Lists information about the authenticated user, e.g., the user id. (D) |

## Python Implementation
### Flask Pattern
Each call to the API is mapped to a specific class which implements set of methods which corresponds to the actual to the requests method (GET, POST, PUT, DELETE, PATCH).

### API handlers
The following classes implement the API

*table 3: Flask Implementation*
| Name                        | Endpoint                                  | description |
|----------------------------|-------------------------------------------|---------------------|
| [OpenEOIPCollections](####openeoipcollections)        | /collections                              | Implements the get() method which loads for a specific collection (jsonifyed version) and return a collection to the client wrapped in [openeo terms](https://api.openeo.org/#tag/EO-Data-Discovery/operation/describe-collection). |
| OpenEOIPCollection         | /collections/<string:name>                | Implements the get() method which loads for a specific collection (jsonifyed version) and return a collection to the client wrapped in [openeo terms](https://api.openeo.org/#tag/EO-Data-Discovery/operation/describe-collection). |
| OpenEOIPCapabilities       | /                                         | Implements the get() method which return the available capabilities of the server. [openeo terms](https://api.openeo.org/#tag/Capabilities). For the moment the capabilities are a fixed json structure. |
| OpenEOIPProcessDiscovery   | /processes                                |Implements the get() method which loads the metadata from all operations in [openeo terms](https://api.openeo.org/#tag/Process-Discovery). The metadata of an operation is available in the operation/metadata folder and each operation knows how to access it. The function wraps them in a list and makes a response to send back to the client. |
| OpenEOIPResult             | /result                                   | A synchronus running of a process-graph through the post() method ( see [openeo terms](https://api.openeo.org/#tag/Data-Processing/operation/compute-result) ). There will be no feedback from the server until the operations is finished and the result is downloaded. |
| OpenEOIPFileFormats        | /file_formats                             | Queries the system which output formats are available ( see [openeo terms](https://api.openeo.org/#tag/Capabilities/operation/list-file-types) ). For the moment it is fixed to GTiff though the underlying library can do much more. Will change in the future|
| OpenEOIPServices           | /services                                 | Lists all secondary web services submitted by a user. For the moment not implemented |
| OpenEOIPServiceTypes       | /service_types                            | Lists supported secondary web service protocols such as OGC WMS, OGC WCS,.... For the moment there are no secondary services |
| OpenEOIPJobs               | /jobs                                     | Creates a new batch processing task (job) from one or more (chained) processes at the back-end ( see [openeo terms](https://api.openeo.org/#tag/Batch-Jobs/operation/create-job)) using the post() method. The job is not started but added to the ProcessManager. The process is added to the run queue (and is elegible for running) if an explicit command comes from the client. A job_id is returned to the client as unique identifier that must be used when subsequent communication with server takes place.|
| OpenEOMetadata4JobById     | /jobs/<string:job_id>                     | Lists all information about a submitted batch job. . The metadata will be in json format|
| OpenEOJobResults           | /jobs/<string:job_id>/results             | Moves the previous created job to the actual job queue to be ready for processing. ( see [openeo terms](https://api.openeo.org/#tag/Batch-Jobs/operation/start-job) ) The id used is the formentioned job_id as returned by OpenEOIPJobs.post(). Before this there is no processing for this job. This method also enusre that communication of progress can be communicated to the client|
| OpenEOIJobByIdEstimate     | /jobs/<string:job_id>/estimate            | Calculates an estimate for a batch job ( see [openeo terms](https://api.openeo.org/#tag/Batch-Jobs/operation/estimate-job) )). Estimates are implemented empty at the moment until a clear model exist what an estimate is |
| OpenEOProcessGraphs        | /process_graphs                           | Lists all user-defined processes (process graphs) of the authenticated user. see [openeo terms](https://api.openeo.org/#tag/Process-Discovery/operation/list-custom-processes) ). The json is the abbreviated form. The long form is delivered by /process_graphs/<string:name>  |
| OpenEOProcessGraph         | /process_graphs/<string:name>             | Lists of a user-defined processes (process graphs) of the authenticated user. see [openeo terms](https://api.openeo.org/#tag/Process-Discovery/operation/describe-custom-process ). The json is the long form. |
| OpenEOIPLogs               | /jobs/<string:job_id>/logs                | Lists log entries for the batch job, usually for debugging purposes.  see [openeo terms](https://api.openeo.org/#tag/Data-Processing/operation/debug-job ) |
| OpenEOIPValidate           | /validation                               | |
| OpenEOUdfRuntimes          | /udf_runtimes                             | |
| OpenEODownloadFile         | /files/<string:filepath>                  | |
| OpenEODataDownload         | /download/<token>                         | |
| Authenitication            | /credentials/basic                        | |
| OpenEOUploadFile           | /files/<path>                             | |

## Backend structures
In a nutshell the server follows the pattern below.

![masterflow](https://github.com/user-attachments/assets/66d6fea2-8dea-4dae-adb7-3592bc71e1d2)

The first lane is the Flask framework itself. The second lane is API handlers (see table 3). The third lane is the [ProcessManager](#processmanager). Entry into the server begins when 'Handle request' is called in the first lane. It instantiates an appropriate class and passes the request to it. Based on the class it uses backend resources to process and communicate the requests parameters. The process manager starts a seperate process when processesing starts and creates the fourth lane.

*table 4: Key data sources*
| Structure                        | Description                                  |
|----------------------------|-------------------------------------------|
| raster_database | A database that contains all raster datasets (objects of type RasterData) |
| ProcessManager | A class that registers all 'jobs' that are entered into the system |
| openeoip_config | general locations and settings needed to run the server |
| OpenEOProcess | A class that encapsulates a 'job' that has entered the systm |
| operations | A list of all openeo operations available in the system |

### raster_database
A simple dictionary that links that (internal)raster identifier to the id2filename.table. This file contains a json descriptions of the raster data sets. The location of this file is linked to openeoip_config['data_locations']['system_files']. Note that openeo describes a flat structure of files/data. Meaning that it easy to generate id conflicts ( duplicates). 

### ProcessManager
A class with only one instance that manages all registered jobs. The class has a number of responsibilities 
- register a user defined progress graph ( a job)
- start a registered job
- register any communcation from running jobs and pass it (if needed) to the client side.
- remove a finished job and its adimistration after a set time ( see ...)

*table 5: Data managed by the ProcessManager*
| member| description|
|----------------------------|-------------------------------------------|
|outputs| A container for all information(see table 6) about jobs that registered on the server| 
|processQueue|A queue containing all jobs that are eligible to run. Once running they are removed from this queue|
|outputQueue| Using the python inter-process class Queue it facilitates communication between child process started by the ProcessManager|

The class runs a infinite loop in which it checks the status of the various responsibilities it is tasked to. 

#### registering jobs
This creates an 'output' object. An 'output' object is a container for all information about the newly created job. It also creates an object on the process queue. The output list and process queue are somewhat similar but server different purposes. The process queue is very temporary while the output queue lasts longer. The newly created object has a status, STATUSCREATED and thus will not run. Only objects with status STATUSQUEUED will be considered by the ProcessManager to be 'started'. 

*table 6: Structure output list elements*
| member| description|
|----------------------------|-------------------------------------------|
| eoprocess| an instance of the OpenEOProcess. A wrapper class for the process graph that is the core of openeo processing. Note that the EOProcess on this side of the process boundary and the instance on the other side are different instances and basically unreachable for each other. The instance on this side exists for administrative purposes|
| pythonProcess| Each EoProcess is started(passed to) as a Python process. For communication reasons we want to have access to this instance|
| progress| a number between 0 and 1 marking the current progress. Note that atm this has onnly the state 0 or 1 as the actual progress is in openeo terms a rather questionable number|
| last_updated| marks the last moment communication was accepted from the running OpenEOProcess instance (the 'other' process)|
| status| the current state of the job: STATUSQUEUED, STATUSCREATED, STATUSRUNNING, STATUSSTOPPED, STATUSFINISHED, STATUSJOBDONE, STATUSUNKNOWN, STATUSERROR, CUSTOMERROR|
| logs| accumulated logs ( a list) of all log messages commin from the 'other' process|
| message| last message from the 'other' process|
| code| code classifying the last message|
| output| references to the generate outputs of the process. Usually locations on the server where outputs are stored|

The content of these fields is constantly modified and queried by the various requests/services running on the server thread. 

#### Start a registered Job
The ProcessManager changes the status of the job to STATUSQUEUED. This causes the object the picked up by the infinite loop. If it is picked up, it will be removed from the queue and a process will be started with an OpenEOProcess as parameter (and thus split of OpenEOProcess in the 'outputs' list).

#### Communication
The ProcessManager maintains an instance of type Queue. Which is in python an inter-process structure in which processes can push objects that are (in this case) shared with the parent process. The server (ProcessManager) pops object of the queue and stores its information with the relevant element in the output list.

*table 7: Information elements*
| member| description|
|----------------------------|-------------------------------------------|
|type | type of information 'event' |
|job_id | who send this message |
|progress| progress information, usually a string|
|last_updated|time marker of this message|
|status| job status|
|current_operation|in case of processing information it contains the now running node|

![flowprocessmanager](https://github.com/user-attachments/assets/e0575726-71d6-4d56-971a-ab67947c691d)

### openip_config
A simple dictionary that is loaded from config.json file that is located in the {root_project}/config folder.
```
{
    "version" : "0",
    "data_locations" :{
            "root_data_location" : {  
                    "description" : "root of the location where generic non user dependent dat is stored", 
                    "location": "{some root location0}/openeodata"
            },
            "root_user_data_location" : {
                    "description" : "root of the location where the data uploaded or generated by a specific user is stored",
                    "location" : "{some root location1}/userdata"
            },
            "system_files" : {
                "description" : "place were the system files are stored",
                "location" : "{some root location2}/properties"
            },            
            "udf_locations" : [{
                "description": "testing udf locations", 
                "location" : "{some root location3}/properties/udf"
            }],
            "cached_data" : {
                "description" : "place were we store intermediate results",
                "location" : "{some root location4}"
            }
            
    },
    "stac_version" : "1.0",
    "links" : [{"rel" : "Some general info", "href" : "https://some/website", "title" : "OpenEO catalog for ITC"}],
    "udf_runtimes" : [ {"runtime" : "python3", "version" : "3.10", "description" : "Python interpreter"} ],
    "openeo_gip_root" : "http://127.0.0.1:5000/"
}
```
Note that the some root locationXXX may be the same locations.

### OpenEOProcess
A wrapper class for the process graph that is the core of openeo processing. It provides services (interface) to the rest of the system to query and run the process graph. 
- run the process process graph
- stop the process graph
- end point and packaging of the output of a process graph
  - manage error messaging from the graph
  - regular information of the graph(e.g. progress, status)
- translate the graph to json format that can be saved or send

The class splits the processing part( the graph) of a client processing request from the other metadata wrapped in the request. The graph is passed to an instance of the ProgressGraph class which handles the actual processing. The table below describes the actual metadata members that are handled by OpenEOProcess. Note that often not all of these members are filled in or only have a default value (e.g. summary). 

*table 8: Openeo attributes*
| member| description|
|----------------------------|-------------------------------------------|
| job_id | A guid like id generated by this class is instantiated with a request|
| processGraph | In instance of the class ProcessGraph which handles processing|
| submitted | Time of creation|
| updated | last time an update message came from processing |
| status | the current state of the job: STATUSQUEUED, STATUSCREATED, STATUSRUNNING, STATUSSTOPPED, STATUSFINISHED, STATUSJOBDONE, STATUSUNKNOWN, STATUSERROR, CUSTOMERROR|
| id | A text id of the graph. Usually empty and its role will be filled by job_id. |
| summary | a brief description of the graph |
| process_description | A full description of the graph |
| parameters | the input parameters of this process |
| categories | A list of categories. |
| deprecated | True or False |
| experimental | True or False |
| exceptions | allowed exceptions for this graph |
| returns | description of valid return values |
| examples | Examples of usage |
| links | relevant links for this process/graph |

The client is usually polling the server at regular intervals, that increase the longer the process takes, for information. This happens in the server process. The server process than asks the OpenEOProcess linked to the job_id to generate an appropriate response based on the state/value of its members. These members are filled initially by construction the instance, but are constantly updated by processing information from the inter-process Queue. Note that at the processing side of things(a seperate Process) a similar instance of OpenEOProcess exists. The Queue ensure that the state of these two instances remains consistent.

### Operations
A list of operations that is discovered by the RegisterOperation method that **must** be implemented by every operation implementation. The operations must be located in the ./operations folder under the root of the project or any subfolder of that. At startup the system checks every module in the folder for the existence of the RegisterOperation function.If present, it creates an instance of the operation which is used for metadata purposes. The metadata of an operation is available in the operations/metadata folder and each operation knows how to access it. It is a json file which is a copy of the actual [json metadata file(s)](https://github.com/Open-EO/openeo-processes) on the openeo github
An instance of the operation class does the actual processing. It is mandatory implements three methods:
- Prepare method. 
  - Unwraps the data (if any) from the input parameter(s). The actual rasters are wrapped in a RasteData structure which contains a lot of metadata and a description of the structure of the data. The prepare method unwraps it and organize it in such a way the the Run method(see below) doesn't have to bother with administrative details
  - Unmarshal data if needed. Usualy data is passed in IlwisObjects format and doesn't need unmarshalling but is not strictly needed.
  - resample data if needed if the raster data of the parameters is of different geometry
  - Unpack the other parameters and make them easily available for the Run method
- Run Method
  - Does the actual processing
  - wraps the output with sufficient metadata and passes it to the system.
- RegisterOperation


## Processing Backend

### ProcessGraph
The class ProcessGraph analyzes the graph and
- determine its end-point. The end point is the only node with a 'return' attribute. As running the graph is a back tracing algorithm, this will be were the back tracing begins.
- optimize the graph. The graph, as comming from the client, contains all the actions needed to calculate the end result. It is not always efficiently organized. Depending on the processing back-end the graph often can be reorganized  to run more efficiently.
- Mapping the nodes the optimized graph ( which is a dict) to the ProcessNode class which maps the items of various dicts to appropriate metadata/data for the ProcessNode for easier access. A ProcessNode represents a 'single' operation to be executed. Single is relative notion here as to execute it we might have to run other operations to resolve the parameters needed for this operation.
- executing the graph. This is handled by an instance of the NodeExecution class which is handed the end-point node at the start from which it starts its back tracing.
- offering sum support interface for the rest of the system to query properties of the graph (validity, estimations)
 
For the moment optimizing the incomming graph is limited to amalgamating consecutive nodes which together represent a raster calc expression to one expression that can be executed at once.

#### NodeExecution
The NodeExecution wraps a ProcessNode and tries to execute the node based on the available parameters( called localParameters). A ProcessNode has a number of parameters as defined by the process graph. A parameter is a simpel dict with two items
- 'base' : This is the value as described in the process graph. It might be an actual direct value, a reference to another ProcessNode or a sub process graph.
- 'resolved' : Default None. Will have the value to which the 'base' points to but now a resolved form. A 'real' value.  

The trick is to be able to resolve all the values. Values are either a direct value (e.g. the value 3.1415927) or a referred value. A referred value can be a reference to a 'graph' value or a reference to another node in the graph. A 'graph' value can be seen a global value of the current graph. If the referrence is another node, this node has be queried for its resolved values, which in turn can query other nodes etc... leading to a recursive back tracing path through the graph resolving all unknowns. If a resolve fails an error is generated.

![resolveparameters](https://github.com/user-attachments/assets/0262c614-fb15-4365-8450-c78b2257bac7)

after all unknowns have been resolved the node can be executed. Not that if you look in the sub diagram 'resolve referred node' you'll see that can create another execution node, in this way execution propegates backwards through the graph. The id of the node matches the name of the operation that should be executed. This id is in the metadata of each operation and based on this the approriate instance of the operation is fetched. Each operation has a prepare and run method. The prepare is called with the resolved parameters and if the prepare succeeds the run is called.

![nodeexecution](https://github.com/user-attachments/assets/7a3003ba-81a6-435c-a9bf-15c0f1fba00d)

### DataCube
The class (or instance of) that holds the metadata and data of a collection. For the moment it is based on raster data. The class has the following responsibilities
- give access and storing metadata
- give access and storing data
The data members are not meant to be accessed publicly (though nothing in Python prevents that) as the internal organisation might change in feature versions. Th facilitate access(read/write) a number of functions exists.
Spectral bands and Temporal layers are the main organization of data. In the text when 'band' is used it refers to 'Spectral band' and when 'layer' is used to 'temporal layer' is meant

*table 9: Datacube attributes*
| member| description|
|----------------------------|-------------------------------------------|
| temporal extent | total temporal extent of this data set. It is an array of two members [first, last] with format year-month-day(e.g. 2011-09=03)|
| proj | either an epsg number or, where epsg is not available, a proj4 definition |
| spatial extent | total spatial extent in latlon. It is formatted as an array [min, miny, maxx, maxy] |
| summaries | object (STAC Summaries (Collection Properties)) Collection properties from STAC extensions (e.g. EO, SAR, Satellite or Scientific) or even custom extensions. Summaries are either a unique set of all available values, statistics or a JSON Schema| 
| eo:cloud_cover | percentage of cloud cover | 
| nodata | the value that reprents the uknown value in the data set | 
| dimensions | the root of the metadata for a description of the dimensional structure of the data. At this moment the data is max 4D. X, Y, Spectral Band, Temporal Layer. Not necessarily in this order, nor do all members need to be there. The actual data is 'hidden' inside the appropriate level of the dimension(s). This is a reflection of how the actual data is organized |
| dimensions: spectral band| see below table 10|
| dimensions: temporal layer| see below table 11|
| implementation | describes the order how bands and layers are organized in the actual data |
| | |


*table 10: Band attributes*
| member| description|
|----------------------------|-------------------------------------------|
| common band name| a commonly understood name for a spectral band. E.g. in sentinel 2 the band name 'B02' has as common name 'blue'. If done correctly these names can be compared between different formats but it should be understood that the spectral range maybe somewhat different between formats(though probably somewhat similar). Outside primary sattelite formats these names have less meaning |
| name | The name in the context of the dataset |
| details | |
| bandIndex | 0 based index number for the band |
| type | data type of the band float, integer, byte | 
| data | reference to the actual data belonging to this band | 
| label | not similar to the name but a description of the value range of the band. The notion is a bit vague |

*table 11: Layer attributes*
| member| description|
|----------------------------|-------------------------------------------|
| | |

### Raster Iterators
### Memory model for rasters
Though the following description is about the internal organization of memory and rasters and resides at the C++ side of the IlwisPy library it is (can be) important to understand this as some of the parameters of this organization can be tweaked from the Python side.
- movement of the focus of processing works through iterators which can move in any of the 3 axis of a 3D raster. It can use steps(default is 1) and can be moved around at 'random' if the algorithm requires it. By default movement it is XYZ; first all X's are done until the end of the bounding box, then Y is increased and again all X's are done for the new Y until the Y also reaches the end of the bounding box. Z is increased and process repeats. The other organization is ZXY which has the vertical column as primary movement.  
- an iterator runs in a bounding box and not outside it. By default this is the whole (3d) raster but it might be a subsection(potential 3d).
- to diminish memory use as rasters can quickly eat up memory each raster is divided into blocks. 

![memorymodel](https://github.com/user-attachments/assets/87486f37-9ca8-42c8-a202-7ea17970238c)


### Raster Processing
