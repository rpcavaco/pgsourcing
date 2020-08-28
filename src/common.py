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
OPS_CHECK = ["chksrc", "chkdest"]
OPS_CONNECTED = ["chksrc", "chkdest"]
OPS_INPUT = ["updref", "upddest"]
OPS_OUTPUT = ["upddest"]
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

CFG_GROUPS = [
	"owners", "roles", "schemas", 
	"sequences", "tables", "procedures"
]

CFG_DEST_GROUPS = [
	"roles", "schemas", 
	"sequences", "tables", "procedures"
]

CFG_LISTGROUPS = ["owners"]

CFG_SHALLOW_GROUPS = ["schemas"]
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
# Valor indica a profundidade da alteracao abaixo da chave indicada
#
UPPERLEVELOPS = { "pkey": 1, "check": 1, "index": 1, "unique": 1, "procedures": 2 }


