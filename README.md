This project is a backend for OpenEo clients(https://openeo.org/). It facilitates processing of geo spatial data (for the moment raster data) in a cloud enviroment. The goal of the project is to eventually realize a backend that can be used(and deployed) by typical users in a academical enviroment for research, teaching and projects.

## Setup
### Installation.
 - pull the repository from github to a suitable location on your server
 - make sure the packages below are installed
 
 *table 1: Installation packages*   
| Package            | Installation Command                          |
|--------------------|----------------------------------------------|
| OpenEO           | `pip install openeo`                         |
| Flask           | `pip install -U Flask`                        |
| Flask-RESTful   | `sudo apt-get install -y python-flask-restful` |
| Flask-HTTPAuth  | `pip install Flask-HTTPAuth`                  |
| PyNaCl          | `pip install pynacl`                          |
| JSON Schema     | `pip install jsonschema`                      |
| EOReader        | `pip install eoreader`                        |
| ILWISPy    | `pip install ilwis`                       |
| Qt5            | `sudo apt install -y qtbase5-dev`              |
| GDAL           | `sudo apt-get install gdal-bin`                |
| NetCDF         | `sudo apt -y install netcdf-bin`               |

- the file app.py starts the server on port 5000
- The file config.json describes locations that the server needs to find and write information/data. Modify this to suit your own needs.

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
The following class implement the API

*table 3: Flask Implementation*
| Name                        | Endpoint                                  |
|----------------------------|-------------------------------------------|
| OpenEOIPCollections        | /collections                              |
| OpenEOIPCollection         | /collections/<string:name>                |
| OpenEOIPCapabilities       | /                                         |
| OpenEOIPProcessDiscovery   | /processes                                |
| OpenEOIPResult             | /result                                   |
| OpenEOIPFileFormats        | /file_formats                             |
| OpenEOIPServices           | /services                                 |
| OpenEOIPServiceTypes       | /service_types                            |
| OpenEOIPJobs               | /jobs                                     |
| OpenEOMetadata4JobById     | /jobs/<string:job_id>                     |
| OpenEOJobResults           | /jobs/<string:job_id>/results             |
| OpenEOIJobByIdEstimate     | /jobs/<string:job_id>/estimate            |
| OpenEOProcessGraphs        | /process_graphs                           |
| OpenEOProcessGraph         | /process_graphs/<string:name>             |
| OpenEOIPLogs               | /jobs/<string:job_id>/logs                |
| OpenEOIPValidate           | /validation                               |
| OpenEOUdfRuntimes          | /udf_runtimes                             |
| OpenEODownloadFile         | /files/<string:filepath>                  |
| OpenEODataDownload         | /download/<token>                         |
| Authenitication            | /credentials/basic                        |
| OpenEOUploadFile           | /files/<path>                             |

e.g.
The class **processPostJobId** (/jobs). Implements a 'post' method to handle the POST request method and 'get' method to handle to GET request method.

### Backend structures
The framework communicate with the rest of the backend ( usually through an api) through the following sources.

*table 4: Key data sources*
| Structure                        | Description                                  |
|----------------------------|-------------------------------------------|
| raster_database | A database that contains all raster datasets (objects of type RasterData) |
| processManager | A class that registers all 'jobs' that are entered into the system |
| openeoip_config | general locations and settings needed to run the server |
| OpenEOProcess | A class that encapsulates a 'job' that has entered the systm |


![masterflow](https://github.com/user-attachments/assets/ecc0d491-9ed3-4be9-8501-2a5cbcbac840)

The server (Flask thread) gets a HTTP request and creates a thread in which an object is instanced mapped to the request. 
## Processing Backend



