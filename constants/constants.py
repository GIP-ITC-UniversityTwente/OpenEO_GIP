from enum import Enum

DTUNKNOWN = 0
DTINTEGER = 1
DTFLOAT = 2
DTSTRING = 4
DTRASTER = 8
DTERROR = 16
DTRASTER = 32
DTTABLE = 64
DTFEATURES = 128
DTLIST = 256
DTARRAY = 512
DTBOOL = 1024
DTDICT = 2048
DTNUMBER = DTINTEGER | DTFLOAT
DTRASTERLIST = DTLIST | DTRASTER

UNDEFNUMBER = 10**20 - 1
RSUNDEF = '-1e308'
RUNDEFFL = -1e308
RUNDEFI32 = 2**31 - 3
RUNDEFI16 = 2**15 - 3

PDUNKNOWN = 0
PDUSERDEFINED = 1
PDPREDEFINED = 2
PDPROCESSGRAPH = 4

STATUSQUEUED = "queued"
STATUSCREATED = "created"
STATUSRUNNING = "running"
STATUSSTOPPED = "canceled"
STATUSFINISHED = "finished"
STATUSJOBDONE = "jobdone"
STATUSUNKNOWN = "unknown"
STATUSERROR = 'error'
CUSTOMERROR = "custom error"

ERRORPARAMETER = 'ParameterError'
ERROROPERATION = 'OperationError'

TESTFILENAME1 = 'SYNTHETIC_DATA1'
TESTFILENAME2 = 'SYNTHETIC_DATA2'
TESTFILENAME_MASKMAP = 'SYNTHETIC_DATA3_MASK'
TESTFILENAME_MASKMAP_SHIFTED = 'SYNTHETIC_DATA4_MASK_SHIFTED'
TESTFILENAME_NO_LAYERS = 'SYNTHETIC_DATA5_NOLAYERS'
TESTFILENAME_MORE_LAYERS = "SYNTHETIC_DATA_MORE_LAYERS"
TESTFILENAME_MANY_LAYERS = "SYNTHETIC_DATA_MANY_LAYERS"

DIMSPECTRALBANDS = "bands"
DIMTEMPORALLAYER = "t"
DIMXRASTER = "x"
DIMYRASTER = "y"
DIMENSIONSLABEL = 'dimensions'
DATASOURCE = 'source'
DIMORDER = 'implementation'

TEMPORALEXTENT = "extent"
SPATIALEXTENT = "extent"
BANDINDEX = 'index'
LAYERINDEX = 'index'
INDEXID = 'index'
RASTERDATA = 'data'
NAMEID = 'name'







