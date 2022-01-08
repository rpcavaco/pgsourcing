import logging

########################################################################
# Instalation-relatec configs
########################################################################

# Config de logging
LOG_CFG = {
  # "format" : "%(asctime)s %(levelname)-8s %(name)s %(message)s",
  "format" : "%(asctime)s %(levelname)-8s %(message)s",
  "filename": "log/cli.log",
  "level": logging.DEBUG
}	


########################################################################
# Version-related configs -- DO NOT EDIT
########################################################################

LANG = "en"
OPS = ["chksrc", "chkdest", "dropref", "updref", "updscript", "upddirect", "chkcode", "updcode", "filldata"]
OPS_CHECK = ["chksrc", "chkdest"]
OPS_INPUT = ["updref", "updscript", "upddirect", "updcode"]
OPS_OUTPUT = ["updscript"]
OPS_CODE = ["chkcode", "updcode"]
PROJECTDIR = "projetos"
PROC_SRC_BODY_FNAME = "body"

STORAGE_VERSION = 4

OPS_HELP = {
	"en": {
		"chksrc": "compare source (development) database to reference local repository (instantiate it in case this is not already done)",
		"chkdest": "compare reference local repository to destination (production) database",
		"chkcode": "compare procedure source code files to reference local repository",
		"dropref": "drop reference data, becase it became invalid, e.g., existing storage format version was superseded by a new one, imeplemented in the software",
		"updref": "update reference local repository from source (development) database",
		"updscript": "generate SQL and DDL script to update destination (production) database, includes procedural code",
		"upddirect": "directly update destination (production) database (should 'updscript' first to check all changes prior to update), includes procedural code",
		"updcode": "update procedural code in source (development) database, from local repository",
		"filldata": "fill parameter table values in destination (production) database"
	}
}

OPS_PRECEDENCE = {
	"updref": "chksrc",
	"updscript": "chkdest",
	"upddirect": "chkdest"	
}

SETUP_ZIP = "pgsourcing_setup.zip"

BASE_FILTERS_RE = {
	"schema": [],
	"transformschema": {},
	"tables": {},
	"views": {},
	"matviews": {},
	"procedures": {},
	"parameterstables": {}	
}

BASE_CONNCFG = {
	"src": {
		"conn": {
		"host": "XX.XX.XX.XX", 
		"database": "the_dbname", 
		"user": "the_user", 
		"password": "base64_converted_passwd" 
		  },
		"filters": BASE_FILTERS_RE,
		"srccodedir": ""
	},
	"dest": {
		"conn": {
		"host": "XX.XX.XX.XX", 
		"database": "the_dbname", 
		"user": "the_user", 
		"password": "base64_converted_passwd" 
		  },
		"filters": BASE_FILTERS_RE
	}	
}

CFG_GROUPS = [
	"owners", "roles", "schemata", 
	"sequences", "tables", 
	"views", "matviews", "procedures"
]

CFG_DEST_GROUPS = [
	"roles", "schemata", 
	"sequences", "tables", 
	"views", "matviews", "procedures"
]

CFG_LISTGROUPS = ["owners"]

CFG_SHALLOW_GROUPS = ["schemata", "roles"]
SHALLOW_DEPTH = 2

FLOAT_TYPES = ["numeric", "float"]
INT_TYPES = ["integer", "smallint"]

COL_ITEMS_CHG_AVOIDING_SUBSTITUTION = ["default", "nullable"]

# Upper level ops: chaves para as quais uma alteracao num detalhe obriga a 
#  destruir e recriar o objeto completo, determinando uma operacao de 
#  ou "update" (com "newvalue" igual 'a totalidade do objeto,
#  num nivel na arvore acima daquele onde a diferenca foi
#  detetada.
#
# Valor indica:
#
#  a) a profundidade da alteracao abaixo da chave indicada (inteiro);
#  b) tuplo contendo o valor para a) e uma flag True para indicar que a
#		alteracao e' para aplcar ao 'parent'
#
UPPERLEVELOPS = { "pkey": 1, "cols": 1, "check": 1, "index": 1, "unique": 1, "trigger": 1, "procedures": 2, "roles": 1, "schdetails": (0, True), "seqdetails": (0, True), "vdetails": (0, True), "mvdetails": (0, True) }


