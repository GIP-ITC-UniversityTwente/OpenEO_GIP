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

The first lane is the Flask framework itself. The second lane is API handlers (see table 3). The third lane is the [ProcessManager](####processmanager). Entry into the server begins when 'Handle request' is called in the first lane. It instantiates an appropriate class and passes the request to it. Based on the class it uses backend resources to process and communicate the requests parameters. The process manager starts a seperate process when processesing starts and creates the fourth lane.

*table 4: Key data sources*
| Structure                        | Description                                  |
|----------------------------|-------------------------------------------|
| raster_database | A database that contains all raster datasets (objects of type RasterData) |
| ProcessManager | A class that registers all 'jobs' that are entered into the system |
| openeoip_config | general locations and settings needed to run the server |
| OpenEOProcess | A class that encapsulates a 'job' that has entered the systm |
| operations | A list of all openeo operations available in the system |

#### raster_database
A simple dictionary that links that (internal)raster identifier to the id2filename.table. This file contains a json descriptions of the raster data sets. The location of this file is linked to openeoip_config['data_locations']['system_files']. Note that openeo describes a flat structure of files/data. Meaning that it easy to generate id conflicts ( duplicates). 

#### ProcessManager
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

##### registering jobs
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

##### Start a registered Job
The ProcessManager changes the status of the job to STATUSQUEUED. This causes the object the picked up by the infinite loop. If it is picked up, it will be removed from the queue and a process will be started with an OpenEOProcess as parameter (and thus split of OpenEOProcess in the 'outputs' list).

##### Communication
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

#### openip_config
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

#### OpenEOProcess
A wrapper class for the process graph that is the core of openeo processing. It provides services (interface) to the rest of the system to query and run the process graph. 
- run the process process graph
- stop the process graph
- end point and packaging of the output of a process graph
  - manage error messaging from the graph
- translate the graph to json format that can be saved or send

#### operations
A list of operations that is discovered by the RegisterOperation method that must be implemented by every operation implementation. The metadata of an operation is available in the operation/metadata folder and each operation knows how to access it. 


## Processing Backend