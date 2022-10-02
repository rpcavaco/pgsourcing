import logging
import re
import hashlib
import logging

########################################################################
# Instalation-related configs
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
OPS = ["chksrc", "chkdest", "dropref", "updref", "upddestscript", "upddestdirect", "chksrccode", "updcodeinsrc", "fillparamdata"]
OPS_CHECK = ["chksrc", "chkdest", "chksrccode"]
OPS_DBCHECK = ["chksrc", "chkdest"]
OPS_INPUT = ["updref", "updrefcode", "upddestscript", "upddestdirect", "updcodeinsrc"]
OPS_OUTPUT = ["upddestscript"]
OPS_CODE = ["chksrccode", "updcodeinsrc"]
PROJECTDIR = "projetos"
PROC_SRC_BODY_FNAME = "body"

STORAGE_VERSION = 8

OPS_HELP = {
	"en": {
		"chksrc": "compare source (development) database to reference local repository (instantiate it in case this is not already done), clears and refills 'code' folder",
		"chkdest": "compare reference local repository to destination (production) database",
		"chksrccode": "compare source code files to reference local repository",
		"dropref": "drop reference data, becase it became invalid, e.g., existing storage format version was superseded by a new one, imeplemented in the software",
		# "getcode": "check source (development) database to just refill 'code' folder",
		"updref": "update reference local repository from source (development) database",
		"upddestscript": "generate SQL and DDL script to update destination (production) database, includes procedural code",
		"upddestdirect": "directly update destination (production) database (should 'upddestscript' first to check all changes prior to update), includes procedural code",
		"updcodeinsrc": "update code in source (development) database, from source code files",
		"fillparamdata": "fill parameter table values in destination (production) database"
	}
}

OPS_PRECEDENCE = {
	"updref": "chksrc",
	"upddestscript": "chkdest",
	"upddestdirect": "chkdest",
	"updcodeinsrc": "chksrccode"
}

SETUP_ZIP = "pgsourcing_setup.zip"

BASE_FILTERS_RE = {
	"schema": [],
	"transformschema": {},
	"udttypes": {},
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
	"sequences", "udttypes", "tables", 
	"views", "matviews", "procedures"
]

CFG_DEST_GROUPS = [
	"roles", "schemata", 
	"sequences", "udttypes", "tables", 
	"views", "matviews", "procedures"
]

CFG_LISTGROUPS = ["owners"]

CFG_SHALLOW_GROUPS = ["schemata", "roles"]
SHALLOW_DEPTH = 2

CHECK_DICTCHANGEOPS_GRPDEPTH_GENCHECK = {
	"tables": 3, 
	"sequences": 3, 
	"views": 3, 
	"matviews": 3
}

CHECK_DICTCHANGEOPS_GRP_OTHER = {
	"udttypes": {
		"depth": 3,
		"sqlqry": "UDTTYPE_CHECK"
	}
}

FLOAT_TYPES = ["numeric", "float"]
INT_TYPES = ["integer", "smallint"]

# COL_ITEMS_CHG_AVOIDING_SUBSTITUTION = ["defaultval", "nullable"]

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

def reverse_shortpgdtype(p_typestr):

	l_p_typestr = p_typestr.lower()
	if l_p_typestr == "bool":
		typstr = "boolean"
	elif l_p_typestr == "varchar":
		typstr = "character varying"
	elif l_p_typestr == "float8":
		typstr = "double precision"
	elif l_p_typestr == "varbit":
		typstr = "bit varying"
	elif l_p_typestr == "timestamp":
		typstr = "timestamp without timezone"
	elif l_p_typestr == "timestampz":
		typstr = "timestamp with timezone"
	elif l_p_typestr == "time":
		typstr = "time without timezone"
	elif l_p_typestr == "timetz":
		typstr = "time with timezone"
	elif l_p_typestr == "int8":
		typstr = "bigint"
	elif l_p_typestr == "serial8":
		typstr = "bigserial"
	elif l_p_typestr == "char":
		typstr = "character"
	elif l_p_typestr == "int4":
		typstr = "integer"
	elif l_p_typestr == "float4":
		typstr = "real"
	elif l_p_typestr == "int2":
		typstr = "smallint"
	elif l_p_typestr == "serial2":
		typstr = "smallserial"
	else:
		typstr = l_p_typestr

	return typstr

def _condensed_pgdtype(p_typestr):

	l_p_typestr = p_typestr.lower()
	if l_p_typestr == "boolean":
		typstr = "bool"
	elif l_p_typestr == "character varying":
		typstr = "varchar"
	elif l_p_typestr == "double precision":
		typstr = "float8"
	elif l_p_typestr == "bit varying":
		typstr = "varbit"
	elif l_p_typestr == "timestamp without timezone":
		typstr = "timestamp"
	elif l_p_typestr == "timestamp with timezone":
		typstr = "timestamptz"
	elif l_p_typestr == "time without timezone":
		typstr = "time"
	elif l_p_typestr == "time with timezone":
		typstr = "timetz"
	elif l_p_typestr == "bigint":
		typstr = "int8"
	elif l_p_typestr == "bigserial":
		typstr = "serial8"
	elif l_p_typestr == "character":
		typstr = "char"
	elif l_p_typestr == "integer":
		typstr = "int4"
	elif l_p_typestr == "real":
		typstr = "float4"
	elif l_p_typestr == "smallint":
		typstr = "int2"
	elif l_p_typestr == "smallserial":
		typstr = "serial2"
	else:
		typstr = l_p_typestr

	if len(typstr) > 4:
		hashv = hashlib.sha1(typstr.encode("UTF-8")).hexdigest()
		ret = typstr[:2] + hashv[:2]
	else:
		ret = typstr

#	print(">> _condensed_pgdtype:", typstr, ret)

	return ret

def gen_proc_fname(p_pname, p_rettype, p_argtypes_list):
	
	if len(p_argtypes_list) > 0:
		template = "%s#%s$%s" 
		catlist = [_condensed_pgdtype(cat) for cat in p_argtypes_list]
		ret = template % (p_pname, 
		_condensed_pgdtype(p_rettype), "-".join(catlist))
	else:
		template = "%s#%s" 
		ret = template % (p_pname, 
		_condensed_pgdtype(p_rettype))

	return ret	
		
def gen_proc_fname_argsstr(p_pname, p_rettype, p_args):
	
	if p_args:
		argtypeslist = []
		for spl in re.split(",[ ]+", p_args):
			splits = spl.split(" ")
			if splits[1] == "character":
				if len(splits) == 2:
					typstr = splits[1]
				elif splits[2] == "varying":
					typstr = splits[1] + " " + splits[2]
			elif splits[1] == "double" and splits[2] == "precision":
				typstr = splits[1] + " " + splits[2]
			elif splits[1] == "bit" and splits[2] == "varying":
				typstr = splits[1] + " " + splits[2]
			elif splits[1] == "timestamp" and splits[2] == "without":
				typstr = splits[1] + " " + splits[2] +  " " + splits[3]
			elif splits[1] == "timestamp" and splits[2] == "with":
				typstr = splits[1] + " " + splits[2] +  " " + splits[3]
			elif splits[1] == "time" and splits[2] == "without":
				typstr = splits[1] + " " + splits[2] +  " " + splits[3]
			elif splits[1] == "time" and splits[2] == "with":
				typstr = splits[1] + " " + splits[2] +  " " + splits[3]
			else:
				typstr = splits[1]
			argtypeslist.append(typstr)
		#argtypeslist = [spl.split(" ")[1] for spl in re.split(",[ ]+", p_args)]
		ret = gen_proc_fname(p_pname, p_rettype, argtypeslist)
	else:
		ret = gen_proc_fname(p_pname, p_rettype, [])

	return ret

def gen_proc_fname_row(p_row):
	
	return gen_proc_fname_argsstr(p_row["procedure_name"], 
			p_row["return_type"], p_row["args"])
	
def reverse_proc_fname(p_fname, o_dict):

	logger = logging.getLogger('pgsourcing')

	try:
		fullname, rest = p_fname.split("#")
	except Exception as e:
		logger.exception(f"invalid procedure sorce file name '{p_fname}'")
		raise
		
	schema, procname = fullname.split(".")
	
	if "$" in rest:
		# has arguments
		rettype, argstring = rest.split("$")
	else:
		rettype, argstring = (rest, None)
		
	o_dict["procschema"] = schema
	o_dict["procname"] = procname
	o_dict["rettype"] = rettype
	o_dict["argstring"] = argstring

# Storage version history
# 
# 4 > 5 (11/01/2022) - Changed source file naming convention, such names are procedure  unique in reference repositoru

# 6 > 7 (23/04/2022) - Procedure source file naming was changed - function signatures in filename were normalized to short datatype aliases like varchar; also naming was different when read form database from when creating a new file
# 7 > 8 (23/04/2022) - Procedure item 'fargs' containing full procedure args including defaults