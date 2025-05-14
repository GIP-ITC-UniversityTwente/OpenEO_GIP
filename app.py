import sys
import os
import pathlib
import logging

pp = pathlib.Path(__file__).parent.resolve()
pp = str(pp)
sys.path.append(pp)

import common
import openeologging

def initLogger():
    logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
    logger = logging.getLogger('openeo')
    logger.setLevel(logging.INFO)
    logpath = os.path.join(os.path.dirname(__file__), 'log')
    if not os.path.exists(logpath):
        os.mkdir(logpath)
    fileHandler = logging.FileHandler("{0}/{1}.log".format(logpath, 'openeoserver' ))
    fileHandler.setFormatter(logFormatter)
    logger.addHandler(fileHandler)

    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logFormatter)
    logger.addHandler(consoleHandler)   

initLogger()
openeologging.logMessage(logging.INFO, '----------------------------------------------')
openeologging.logMessage(logging.INFO, 'server started, process id:' + str(os.getpid()))

sys.path.append(pp + '/workflow')
sys.path.append(pp + '/constants')
sys.path.append(pp + '/operations')
sys.path.append(pp + '/operations/ilwispy')


from flask import Flask, jsonify, make_response, request
from flask_restful import Api
from flask_cors import CORS
from werkzeug.middleware.proxy_fix import ProxyFix
from flask_httpauth import HTTPBasicAuth
from flask import Flask, render_template
from globals import globalsSingleton
from openeocollections import OpenEOIPCollections
from openeocapabilities import OpenEOIPCapabilities, OpenEOIPServices, OpenEOIPServiceTypes,replace_links_in_capabilities
from openeocollection import OpenEOIPCollection
from openeoprocessdiscovery import OpenEOIPProcessDiscovery
from openeoresult import OpenEOIPResult
from openeofileformats import OpenEOIPFileFormats
from openeojobs import OpenEOIPJobs, OpenEOMetadata4JobById,OpenEOJobResults, OpenEOIJobByIdEstimate
from openeoprocessgraphs import OpenEOProcessGraphs
from openeoproccessgraph import OpenEOProcessGraph
from openeologs import OpenEOIPLogs
from openeovalidate import OpenEOIPValidate
from openeoudfruntimes import OpenEOUdfRuntimes
from openeofiles import OpenEODownloadFile
from datadownload import OpenEODataDownload
from authentication import Authenitication
from openeouploadfile import OpenEOUploadFile


from processmanager import globalProcessManager
from threading import Thread
from wellknown import WellKnown
import ilwis

#init part



app = Flask(__name__)
#app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_host=1, x_proto=1)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True

api = Api(app)

auth = HTTPBasicAuth()

@app.route('/')
def index():
    CAPABILITIES = replace_links_in_capabilities()
    return make_response(jsonify(CAPABILITIES), 200)

@app.route('/.well-known/openeo')
def well_known():
        return WellKnown.get(api)

@app.route('/.well-known/openid-configuration')
def openid_configuration():
        return WellKnown.get(api)

@app.route('/get_file/<token>')
def get_file(token):
    CAPABILITIES = replace_links_in_capabilities()
    return make_response(jsonify(CAPABILITIES), 200)

api.add_resource( OpenEOIPCollections,'/collections')
api.add_resource( OpenEOIPCollection,'/collections/<string:name>')
api.add_resource( OpenEOIPCapabilities,'/')
api.add_resource( OpenEOIPProcessDiscovery,'/processes')
api.add_resource( OpenEOIPResult, '/result')
api.add_resource( OpenEOIPFileFormats, '/file_formats')
api.add_resource( OpenEOIPServices, '/services')
api.add_resource( OpenEOIPServiceTypes, '/service_types')
api.add_resource( OpenEOIPJobs, '/jobs') 
api.add_resource( OpenEOMetadata4JobById, '/jobs/<string:job_id>') 
api.add_resource( OpenEOJobResults, '/jobs/<string:job_id>/results') 
api.add_resource( OpenEOIJobByIdEstimate, '/jobs/<string:job_id>/estimate') 
api.add_resource( OpenEOProcessGraphs, '/process_graphs')
api.add_resource( OpenEOProcessGraph,'/process_graphs/<string:name>')
api.add_resource( OpenEOIPLogs,'/jobs/<string:job_id>/logs')
api.add_resource( OpenEOIPValidate,'/validation')
api.add_resource( OpenEOUdfRuntimes,'/udf_runtimes')
api.add_resource( OpenEODownloadFile,'/files/<string:filepath>')
api.add_resource( OpenEODataDownload,'/download/<token>')
api.add_resource( Authenitication,'/credentials/basic')
api.add_resource( OpenEOUploadFile,'/files/<path>')

CORS(app)

def startProcesses():
    globalProcessManager.runServer()

t1 = Thread(target=startProcesses)
t1.start()

f = common.openeoip_config['data_locations']['cached_data']
ilwis.setContextProperty('cachelocation', f['location'])
f = common.openeoip_config['data_locations']['system_files']
ilwis.setContextProperty('initlogger', f['location'])


openeologging.logMessage(logging.INFO, 'ilwis cache location ' + ilwis.contextProperty('cachelocation'))
openeologging.logMessage(logging.INFO, 'ilwis system catalog ' +  ilwis.contextProperty('systemcatalog'))
openeologging.logMessage(logging.INFO, 'ilwis root ' +  ilwis.contextProperty('ilwisfolder'))

openeologging.logMessage(logging.INFO, 'server started, initialization finished')

if __name__ == '__main__':
    app.run()
    globalProcessManager.stop()