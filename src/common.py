import logging

########################################################################
# Config de instalacao (a alterar numa instalacao, se necessario)
########################################################################

# Versao antiga de Postgresql (Pre-11) ?
OLDER_PG = True

# Config de logging
LOG_CLI_CFG = {
  # "format" : "%(asctime)s %(levelname)-8s %(name)s %(message)s",
  "format" : "%(asctime)s %(levelname)-8s %(message)s",
  "filename": "log/cli.log",
  "level": logging.DEBUG
}	



########################################################################
# Config de versao -- NAO EDITAR
########################################################################



LANG = "pt"
OPS = ["chksrc", "chkdest", "updref", "upddest"]
OPS_CONNECTED = OPS
OPS_INPUT = ["updref", "upddest"]
PROJECTDIR = "projetos"
PROC_SRC_BODY_FNAME = "body"

OPS_HELP = {
	"pt": {
		"chksrc": "comparar b.d. fonte com o repositorio de referencia",
		"chkdest": "comparar o repositorio de referencia com uma b.d. destino",
		"updref": "atualizar o repositorio de referencia desde a b.d. fonte",
		"upddest": "atualizar b.d. destino"
	}
}

SETUP_ZIP = "pgsourcing_setup.zip"

BASE_FILTERS_RE = {
	"schema": [],
	"tables": [],
	"procedures": []
}

BASE_CONNCFG = {
	"src": {
		"conn": {
		"host": "XX.XX.XX.XX", 
		"dbname": "the_dbname", 
		"user": "the_user", 
		"password": "base64_converted_passwd" 
		  },
		"filters": BASE_FILTERS_RE
	}
}




