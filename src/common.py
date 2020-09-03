import logging

########################################################################
# Config de instalacao (a alterar numa instalacao, se necessario)
########################################################################

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
OPS = ["chksrc", "chkdest", "updref", "upddest", "upddir"]
OPS_CHECK = ["chksrc", "chkdest"]
OPS_CONNECTED = ["chksrc", "chkdest", "upddir"]
OPS_INPUT = ["updref", "upddest", "upddir"]
OPS_OUTPUT = ["upddest"]
PROJECTDIR = "projetos"
PROC_SRC_BODY_FNAME = "body"

STORAGE_VERSION = 1

OPS_HELP = {
	"pt": {
		"chksrc": "comparar b.d. fonte com o repositorio de referencia",
		"chkdest": "comparar o repositorio de referencia com uma b.d. destino",
		"updref": "atualizar o repositorio de referencia desde a b.d. fonte",
		"upddest": "gerar script para b.d. destino",
		"upddir": "atualizar diretamente b.d. destino"
	}
}

SETUP_ZIP = "pgsourcing_setup.zip"

BASE_FILTERS_RE = {
	"schema": [],
	"tables": [],
	"views": [],
	"matviews": [],
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
	"sequences", "tables", 
	"views", "matviews", "procedures"
]

CFG_DEST_GROUPS = [
	"roles", "schemas", 
	"sequences", "tables", 
	"views", "matviews", "procedures"
]

CFG_LISTGROUPS = ["owners"]

CFG_SHALLOW_GROUPS = ["schemas", "roles"]
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


