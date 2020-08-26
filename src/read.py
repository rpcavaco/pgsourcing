
import re
import logging
import codecs

from os.path import exists, join as path_join


from src.sql import SQL
from src.common import OLDER_PG, PROC_SRC_BODY_FNAME, CFG_GROUPS, CFG_LISTGROUPS, OPS_CHECK, FLOAT_TYPES, INT_TYPES

WARN_KEYS = {
	"PROC_SU_OWNED": "Procedimentos cujo owner e' 'postgres'",
	"TABLE_SU_OWNED": "Tabelas cujo owner e' 'postgres'"
}

def gen_where_from_re_list(p_filter_fieldname, p_re_list, dojoin=False, intersect=False):	
	
	if intersect:
		prevjoiner = "and %s"
	else:
		prevjoiner = "or %s"
		
	lst = ["%s ~* '%s'" % (p_filter_fieldname,ire) for ire in p_re_list]
	seq = " or ".join(lst)
	
	if dojoin and len(lst) > 1:
		seq = "(%s)" % seq
		
	if dojoin:
		ret = prevjoiner % seq
	else:
		ret = "where %s" % seq
	
	return ret

def gen_where_from_list(p_filter_fieldname, p_re_list, dojoin=False, intersect=False):	

	if intersect:
		prevjoiner = "and %s"
	else:
		prevjoiner = "or %s"
	
	lst = ["%s = '%s'" % (p_filter_fieldname,ire) for ire in p_re_list]	
	seq = " or ".join(lst)

	if dojoin and len(lst) > 1:
		seq = "(%s)" % seq

	if dojoin:
		ret = prevjoiner % seq
	else:
		ret = "where %s" % seq
		
	#print("from_list", ret)
	
	return ret
	
def gen_tables_where_from_re_list(p_schemafilter_fieldname, p_tablefilter_fieldname, p_curr_schema, p_filters_cfg, dojoin=False, intersect=False):

	wherecl = None

	if "tables" in p_filters_cfg and len(p_filters_cfg["tables"].keys()) > 0 and p_curr_schema in p_filters_cfg["tables"].keys():
		wherecl = gen_where_from_re_list(p_tablefilter_fieldname, p_filters_cfg["tables"][p_curr_schema], dojoin=dojoin, intersect=intersect)
			
	return wherecl

def gen_procs_where_from_re_list(p_schemafilter_fieldname, p_procfilter_fieldname, p_curr_schema, p_filters_cfg, dojoin=False, intersect=False):

	wherecl = None

	if "procedures" in p_filters_cfg and len(p_filters_cfg["procedures"].keys()) > 0 and p_curr_schema in p_filters_cfg["procedures"].keys():
		wherecl = gen_where_from_re_list(p_procfilter_fieldname, p_filters_cfg["procedures"][p_curr_schema], dojoin=dojoin, intersect=intersect)
			
	return wherecl

def schema_dependency(p_found_schema, p_schema_name, p_typekey):
	
	if p_found_schema != p_schema_name:

		if not "warnings" in out_dict.keys():
			out_dict["warnings"] = {}
			
		warnings = out_dict["warnings"]

		if not "schemadependencies" in warnings.keys():
			warnings["schemadependencies"] = {}
		if not p_schema_name in warnings["schemadependencies"].keys():
			warnings["schemadependencies"][p_schema_name] = {}
			
		schdepobj = warnings["schemadependencies"][p_schema_name]
			
		if not p_found_schema in schdepobj.keys():
			schdepobj[p_found_schema] = { p_typekey: [seqname] }
		else:
			if not seqname in schdepobj[p_found_schema][p_typekey]:
				schdepobj[p_found_schema][p_typekey].append(seqname)

def schemas(p_cursor, p_filters_cfg, p_include_public, out_dict):
	
	if "schema" in p_filters_cfg and len(p_filters_cfg["schema"]) > 0:
		sql = "%s %s" % (SQL["SCHEMAS"], gen_where_from_list("schema_name", p_filters_cfg["schema"]))
	else:
		sql = SQL["SCHEMAS"]
	p_cursor.execute(sql)
	
	if not "content" in out_dict.keys():
		out_dict["content"] = {}	

	the_dict = out_dict["content"]["schemas"] = {}
	owners = set()
	for row in p_cursor:
		schema_name = row["schema_name"]
		schema_owner = row["schema_owner"]
		if schema_name.startswith("pg_") or schema_name == "information_schema":
			continue
		if schema_name == "public" and not p_include_public:
			continue	
		owners.add(schema_owner)	
		the_dict[schema_name] = {
			"auth": schema_owner
		}	
		
	out_dict["content"]["owners"] = list(owners)
	
def ownership(p_cursor, p_filters_cfg, out_dict):

	if not "content" in out_dict.keys():
		out_dict["content"] = {}	
	
	# Procedures
	if "owners" in out_dict["content"].keys():
		owners = set(out_dict["content"]["owners"])
	else:
		owners = set()
	
	for sch in out_dict["content"]["schemas"]:
		
		p_cursor.execute(SQL["PROC_SU_OWNED"], [sch])
		#print(p_cursor.mogrify(SQL["PROC_SU_OWNED"], [sch]))
		for row in p_cursor:
			if not "warnings" in out_dict.keys():
				out_dict["warnings"] = {}
			if not WARN_KEYS["PROC_SU_OWNED"] in out_dict["warnings"].keys():
				out_dict["warnings"][WARN_KEYS["PROC_SU_OWNED"]] = []
			out_dict["warnings"][WARN_KEYS["PROC_SU_OWNED"]].append("%s.%s" % (sch, row["proname"]))
		
		p_cursor.execute(SQL["PROCOWNERS"], [sch])
		for row in p_cursor:
			ownr = row["ownr"]
			if ownr != "postgres":
				owners.add(row["ownr"])

	# Tables
	for sch in out_dict["content"]["schemas"]:
		
		wherecl = gen_tables_where_from_re_list("schemaname", "tablename", sch, p_filters_cfg, dojoin=True, intersect=True)

		if not wherecl is None:
			sql = "%s %s" % (SQL["TABLE_SU_OWNED"], wherecl)
		else:
			sql = SQL["TABLE_SU_OWNED"]
			
		p_cursor.execute(sql, [sch])	
		
		for row in p_cursor:
			if not "warnings" in out_dict.keys():
				out_dict["warnings"] = {}
			if not WARN_KEYS["TABLE_SU_OWNED"] in out_dict["warnings"].keys():
				out_dict["warnings"][WARN_KEYS["TABLE_SU_OWNED"]] = []
			out_dict["warnings"][WARN_KEYS["TABLE_SU_OWNED"]].append("%s.%s" % (sch, row["tablename"]))

		if "tables" in p_filters_cfg and len(p_filters_cfg["tables"]) > 0:
			sql = "%s %s" % (SQL["TABLEOWNERS"], gen_where_from_re_list("tablename", p_filters_cfg["tables"], dojoin=True))
		else:
			sql = SQL["TABLEOWNERS"]
		p_cursor.execute(sql, [sch])	
		
		for row in p_cursor:
			ownr = row["ownr"]
			if ownr != "postgres":
				owners.add(row["ownr"])
			
	out_dict["content"]["owners"] = list(owners)
			
def roles(p_cursor, out_dict):

	if not "content" in out_dict.keys():
		out_dict["content"] = {}	
			
	p_cursor.execute(SQL["ROLES"])
	
	rl_dict = out_dict["content"]["roles"] = {}
	#print(p_cursor.description)
	for row in p_cursor:
		
		rolename = row["rolname"]
		if rolename.startswith("pg_") or \
			rolename == "postgres":
				continue
				
		if "owners" in out_dict["content"].keys() and not rolename in out_dict["content"]["owners"]:
			continue
				
		if row["rolvaliduntil"] is None:
			validuntil = "None"
		else:
			validuntil = row["rolvaliduntil"].isoformat()
			
		rl_dict[rolename] = {
			"inherit": str(row["rolinherit"]),
			"canlogin": str(row["rolcanlogin"]),
			"validuntil": validuntil
		}

def tables(p_cursor, p_filters_cfg, out_dict):

	assert "content" in out_dict.keys(), "'content' em falta no dic. de saida"
	assert "schemas" in out_dict["content"].keys(), "'content.schemas' em falta no dic. de saida"

	for sch in out_dict["content"]["schemas"]:

		if sch not in p_filters_cfg["tables"].keys():
			continue 
			
		wherecl = gen_tables_where_from_re_list("schemaname", "tablename", sch, p_filters_cfg, dojoin=True, intersect=True)
		if wherecl is None:
			wherecl = "and schemaname = '%s'" % sch
		else:
			wherecl = "and schemaname = '%s' %s" % (sch, wherecl)

		sql = "%s %s" % (SQL["TABLES"], wherecl)

		if not "tables" in out_dict["content"].keys():
			out_dict["content"]["tables"] = {}
		
		the_dict = out_dict["content"]["tables"]
		#print(p_cursor.description)
		
		p_cursor.execute(sql)
		#print(p_cursor.mogrify(sql))

		for row in p_cursor:

			# if row["schemaname"] == "information_schema":
				# continue
			
			# if row["schemaname"] == "public" and not p_include_public:
				# continue
				
			## remover tabelas internas ArcGIS
			m = re.match("i[\d]+", row["tablename"])
			if not m is None:
				continue

			if not row["schemaname"] in the_dict.keys():
				sch_dict = the_dict[row["schemaname"]] = {}	
			else:
				sch_dict = the_dict[row["schemaname"]]	
				
			sch_dict[row["tablename"]] = {
				"owner": row["tableowner"]
			}
			
			if not row["tablespace"] is None:
				sch_dict[row["tablename"]]["tablespace"] = row["tablespace"]
				
			
	## TODO - contar registos, ler dados das parameterstables

def columns(p_cursor, out_dict, p_include_colorder):
	
	assert "content" in out_dict.keys(), "'content' em falta no dic. de saida"
	assert "tables" in out_dict["content"].keys(), "'content.tables' em falta no dic. de saida"

	pattern = re.compile(r"nextval\('  (?P<schema>[^\.]+) \. (?P<seqname>[0-9A-Za-z_]+)  ", re.VERBOSE)			

	tables_root = out_dict["content"]["tables"]
	optionals = [
					("column_default", "default"), 
					("character_maximum_length", "char_max_len"), 
					("numeric_precision", "num_precision"),
					("numeric_precision_radix", "num_prec_radix"),
					("numeric_scale", "num_scale"),
					#("datetime_precision", "dttime_precision")
				]
	
	for schema_name in tables_root.keys():
		
		unreadable_tables = []
		
		for table_name in tables_root[schema_name].keys():

			p_cursor.execute(SQL["COLUMNS"], (schema_name, table_name))
			
			if p_cursor.rowcount < 1:
				unreadable_tables.append((schema_name, table_name))

			for row in p_cursor:
				
				if not "cols" in tables_root[schema_name][table_name]:
					cols_dict = tables_root[schema_name][table_name]["cols"] = {}
				else:
					cols_dict = tables_root[schema_name][table_name]["cols"]
					
				if row["data_type"] == "USER-DEFINED":
					dt = row["udt_name"]
				else:
					dt = row["data_type"]

				col_dict = cols_dict[row["column_name"]] = {}
				if p_include_colorder:
					col_dict["ordpos"] = row["ordinal_position"]
				col_dict["type"] = dt
				col_dict["nullable"] = row["is_nullable"]
				
				for opt_colname, opt_key in optionals:
					
					if not row[opt_colname] is None:
						
						# detetar dependencias entre schemas, avaliando se
						# sao invocadas sequencias de outros schemas
						#
						if opt_key == "default":
							
							if col_dict["type"] in FLOAT_TYPES:								
								try:
									parsed_val =  float(row[opt_colname])
								except ValueError:
									parsed_val =  row[opt_colname]
							elif col_dict["type"] in INT_TYPES:								
								try:
									parsed_val =  int(row[opt_colname])
								except ValueError:
									parsed_val =  row[opt_colname]
							else:
								parsed_val =  row[opt_colname]
								
							col_dict[opt_key] = parsed_val
							
							if isinstance(parsed_val, basestring):

								match = pattern.match(parsed_val)
								if not match is None:
									
									found_schema = match.group("schema")
									seqname = match.group("seqname")

									# coletar nomes de sequencia em uso
									
									if not schema_name in out_dict["content"]["sequences"].keys():
										seqs = out_dict["content"]["sequences"][schema_name] = {}
									else:
										seqs = out_dict["content"]["sequences"][schema_name]
										
									if not seqname in seqs.keys():
										seqs[seqname] = {}
										
									schema_dependency(found_schema, schema_name, "sequences")	
								
						else:

							col_dict[opt_key] = row[opt_colname]	
						

		#print(schema_name, unreadable_tables)

def constraints(p_cursor, out_dict):
	
	assert "content" in out_dict.keys(), "'content' em falta no dic. de saida"
	assert "tables" in out_dict["content"].keys(), "'content.tables' em falta no dic. de saida"

	pattern = re.compile(r"foreign \s+ key \s+ \([^\)\s]+\) \s+ references \s+ (?P<schema>[^\.]+) \. (?P<tname>[^\(]+)", re.VERBOSE | re.IGNORECASE)			
		
	tables_root = out_dict["content"]["tables"]

	for schema_name in tables_root.keys():
		
		for table_name in tables_root[schema_name].keys():

			p_cursor.execute(SQL["PKEYS"], (schema_name, table_name))

			for row in p_cursor:
				
				dk = "pkey"
				if not dk in tables_root[schema_name][table_name]:
					constrs_dict = tables_root[schema_name][table_name][dk] = {}
				else:
					constrs_dict = tables_root[schema_name][table_name][dk]

				constr_list = constrs_dict[row["constraint_name"]] = {
				
					"index_tablespace": row["idxtblspc"],
					"columns": row["column_names"]
				
				}

			p_cursor.execute(SQL["CHECKS"], (schema_name, table_name))

			for row in p_cursor:
				
				dk = "check"
				if not dk in tables_root[schema_name][table_name]:
					constrs_dict = tables_root[schema_name][table_name][dk] = {}
				else:
					constrs_dict = tables_root[schema_name][table_name][dk]

				constrs_dict[row["conname"]] = { "chkdesc": row["cdef"]	}	

			p_cursor.execute(SQL["UNIQUE"], (schema_name, table_name))

			for row in p_cursor:
				
				dk = "unique"
				if not dk in tables_root[schema_name][table_name]:
					constrs_dict = tables_root[schema_name][table_name][dk] = {}
				else:
					constrs_dict = tables_root[schema_name][table_name][dk]

				constr_list = constrs_dict[row["constraint_name"]] = {
				
					"index_tablespace": row["idxtblspc"],
					"columns": row["column_names"]
				
				}

			p_cursor.execute(SQL["FKEYS"], (schema_name, table_name))
			
			for row in p_cursor:

				dk = "fkey"
				if not dk in tables_root[schema_name][table_name]:
					constrs_dict = tables_root[schema_name][table_name][dk] = {}
				else:
					constrs_dict = tables_root[schema_name][table_name][dk]

				constrs_dict[row["conname"]] = row["cdef"]

				# detetar dependencias entre schemas, avaliando se
				# sao invocadas tabelas de outros schemas nas foreign keys
				#

				match = pattern.match(row["cdef"])
				if not match is None:
					
					found_schema = match.group("schema")
					tname = match.group("tname")
					
					schema_dependency(found_schema, schema_name, "fkeys")

def indexes(p_cursor, out_dict):
	
	# ATENCAO - indexes deve ser corrido depois de constraints, para poder filtrar
	# os indices associados a primary keys e que sao implicitos,
	# nao precisando de ser listados.
	
	assert "content" in out_dict.keys(), "'content' em falta no dic. de saida"
	assert "tables" in out_dict["content"].keys(), "'content.tables' em falta no dic. de saida"

	# pattern = re.compile(r"foreign \s+ key \s+ \([^\)\s]+\) \s+ references \s+ (?P<schema>[^\.]+) \. (?P<tname>[^\(]+)", re.VERBOSE | re.IGNORECASE)			
		
	tables_root = out_dict["content"]["tables"]
	
	for schema_name in tables_root.keys():
		
		for table_name in tables_root[schema_name].keys():

			if "pkey" in tables_root[schema_name][table_name].keys():
				pkeys = tables_root[schema_name][table_name]["pkey"].keys()
			else:
				pkeys = []

			if "unique" in tables_root[schema_name][table_name].keys():
				unique = tables_root[schema_name][table_name]["unique"].keys()
			else:
				unique = []

			p_cursor.execute(SQL["INDEXES"], (schema_name, table_name))
			for row in p_cursor:
				
				if not row["indexname"] in pkeys and not row["indexname"] in unique:	
												
					dk = "index"
					if not dk in tables_root[schema_name][table_name]:
						constrs_dict = tables_root[schema_name][table_name][dk] = {}
					else:
						constrs_dict = tables_root[schema_name][table_name][dk]
						
					idxdef = row["indexdef"] 
					idxt1 = "ON %s" % table_name
					idxt2 = "ON %s.%s" % (schema_name, table_name)
					if idxt1 in idxdef:
						idxdef = idxdef.replace(idxt1, idxt2)
						
					constrs_dict[row["indexname"]] = { "idxdesc": idxdef }

def sequences(p_cursor, out_dict):
	
	assert "content" in out_dict.keys(), "'content' em falta no dic. de saida"

	if not "sequences" in out_dict["content"].keys():
		return
		
	seq_root = out_dict["content"]["sequences"]
	items = [
			"data_type", "numeric_precision", "numeric_precision_radix", "numeric_scale",
			"start_value", "minimum_value", "maximum_value", "increment", "cycle_option"
			]

	for schema_name in seq_root.keys():
		
		for seq_name in seq_root[schema_name].keys():

			p_cursor.execute(SQL["SEQUENCES"], (schema_name, seq_name))

			the_dict = seq_root[schema_name][seq_name]
			row = p_cursor.fetchone()
			
			if row is None:
				#raise RuntimeError, p_cursor.mogrify(SQL["SEQUENCES"], (schema_name, seq_name))
				the_dict["error"] = "unreadable"
			else:
				for item in items:
					# print(item, row[item])
					if not row[item] is None:
						the_dict[item] = row[item]	
						
				p_cursor.execute(SQL["SEQ_CURRVAL"], (schema_name, seq_name))				
				row = p_cursor.fetchone()
				
				the_dict["current"] = row["current_value"]	

def triggers(p_cursor, out_trigger_functions, out_dict):
	
	assert "content" in out_dict.keys(), "'content' em falta no dic. de saida"
	assert "tables" in out_dict["content"].keys(), "'content.tables' em falta no dic. de saida"
	
	tables_root = out_dict["content"]["tables"]
	items = [
				"disabled",
				"trigger_activation",
				"trigger_event",
				"trigger_level",
				"function_schema", 
				"function_name"
			]
		
	#p_cursor.execute(SQL["TRIGGERS"], (schema_name, t_name))
	p_cursor.execute(SQL["TRIGGERS"])
	
	#print(p_cursor.mogrify(SQL["TRIGGERS"], (schema_name, t_name)))
	#print (p_cursor.description)
	
	dk = "trigger"
						
	for row in p_cursor:
		
		t_schema = row["table_schema"]
		t_name = row["table_name"]

		if not t_schema in tables_root.keys() or not t_name in tables_root[t_schema].keys():
			continue
		
		if not dk in tables_root[t_schema][t_name].keys():
			tables_root[t_schema][t_name][dk] = {}
			
		the_dict = tables_root[t_schema][t_name][dk]
		
		if not row["trigger_name"] in the_dict.keys():
			the_dict[row["trigger_name"]] = {}
		
		details = the_dict[row["trigger_name"]]
		
		out_trigger_functions.add((row["function_schema"], row["function_name"]))
		
		schema_dependency(t_schema, row["function_schema"], "triggerfuncs")

		for item in items:
			if not row[item] is None:
				if item == "disabled":
					details[item] = str(row[item]).lower()
				else:
					details[item] = row[item]	

def gen_proc_hdr(p_sch, p_row):
	
	volatdict = {
		"v": "VOLATILE",
		"i": "IMMUTABLE",
		"s": "STABLE"
	}
	
	template = """CREATE OR REPLACE FUNCTION %s.%s(%s)
	RETURNS %s
	LANGUAGE '%s'
	%s 
AS $BODY$\n""" 

	return template % (p_sch, p_row["procedure_name"], p_row["args"],
		p_row["return_type"], p_row["language_type"], volatdict[p_row["provolatile"]])

def gen_proc_ftr(p_sch, p_row):

	template = """$BODY$;\n
ALTER FUNCTION %s.%s(%s)
OWNER TO %s;"""

	return template % (p_sch, p_row["procedure_name"], p_row["args"],
		p_row["procedure_owner"])

def gen_proc_file(p_genprocsdir, p_schema, p_proc_row, winendings=True):
	if winendings:
		with codecs.open(path_join(p_genprocsdir, "%s.%s.sql" % (p_schema, p_proc_row["procedure_name"])), "wb", "utf-8") as fl:
			#print("......", str(type(row[PROC_SRC_BODY_FNAME])))
			fl.write(gen_proc_hdr(p_schema, p_proc_row).replace("\n","\r\n"))
			fl.write(p_proc_row[PROC_SRC_BODY_FNAME].decode('utf-8').replace("\n","\r\n"))									
			fl.write(gen_proc_ftr(p_schema, p_proc_row).replace("\n","\r\n"))
	else:
		with codecs.open(path_join(p_genprocsdir, "%s.%s.sql" % (p_schema, p_proc_row["procedure_name"])), "w", "utf-8") as fl:
			#print("......", str(type(row[PROC_SRC_BODY_FNAME])))
			fl.write(gen_proc_hdr(p_schema, p_proc_row))
			fl.write(p_proc_row[PROC_SRC_BODY_FNAME].decode('utf-8'))									
			fl.write(gen_proc_ftr(p_schema, p_proc_row))						
						

def procs(p_cursor, p_filters_cfg, in_trigger_functions, out_dict, genprocsdir=None):
	
	assert "content" in out_dict.keys(), "'content' em falta no dic. de saida"
	assert "schemas" in out_dict["content"].keys(), "'content.schemas' em falta no dic. de saida"
	
	trig_dict = {}
	for tr_schema, tr_funcname in in_trigger_functions:
		if not tr_schema in trig_dict.keys():
			trig_dict[tr_schema] = []
		trig_dict[tr_schema].append(tr_funcname)

	items = ["args", "return_type", "procedure_owner", 
			 "language_type", PROC_SRC_BODY_FNAME, "provolatile"]

	if OLDER_PG:
		dict_key = "PROCS_OLD"
	else:
		dict_key = "PROCS_NEW"

	for sch in out_dict["content"]["schemas"]:
		
		if sch not in p_filters_cfg["procedures"].keys():
			continue 
			
		tr_funcnames = trig_dict[sch]	
			
		wherecl = gen_procs_where_from_re_list("procedure_schema", "procedure_name", sch, p_filters_cfg, dojoin=True, intersect=True)
		if wherecl is None:
			wherecl = "and procedure_schema = '%s'" % sch
		else:
			wherecl = "and procedure_schema = '%s' %s" % (sch, wherecl)
			
		sql = "%s %s" % (SQL[dict_key], wherecl)

		if not "procedures" in out_dict["content"].keys():
			out_dict["content"]["procedures"] = {}
		
		the_dict = out_dict["content"]["procedures"]
		
		p_cursor.execute(sql)		
		for row in p_cursor:
			
			# if row["schema"] == "public" and not p_include_public:
				# continue
			
			if not row["procedure_schema"] in the_dict.keys():
				schdict = the_dict[row["procedure_schema"]] = {}
			else:
				schdict = the_dict[row["procedure_schema"]]
				
			pdict = schdict[row["procedure_name"]] = {}
			for item in items:
				if not row[item] is None:
					pdict[item] = row[item]

			if not genprocsdir is None:
				gen_proc_file(genprocsdir, sch, row)
					
		wherecl = "and procedure_schema = %s and procedure_name = ANY(%s)"

		p_cursor.execute("%s %s" % (SQL[dict_key], wherecl), (sch, tr_funcnames))	
		# print(p_cursor.mogrify("%s %s" % (SQL[dict_key], wherecl), (sch, tr_funcnames)))
			
		for row in p_cursor:
			
			# if row["schema"] == "public" and not p_include_public:
				# continue
			
			if not row["procedure_schema"] in the_dict.keys():
				schdict = the_dict[row["procedure_schema"]] = {}
			else:
				schdict = the_dict[row["procedure_schema"]]
				
			pdict = schdict[row["procedure_name"]] = {}
			for item in items:
				if not row[item] is None:
					pdict[item] = row[item]

			if not genprocsdir is None:
				gen_proc_file(genprocsdir, sch, row)
		
def srcreader(p_conn, p_filters_cfg, out_dict, outprocs_dir=None, include_public=False, include_colorder=False):
	
	logger = logging.getLogger('pgsourcing')
	with p_conn as cnobj:

		if cnobj.dict_cursor_factory is None:
			raise RuntimeError("srcreader precisa de cursor dictionary, este driver nao parece ter um")

		cn = cnobj.getConn()
		trigger_functions = set()
		
		with cn.cursor(cursor_factory=cnobj.dict_cursor_factory) as cr:
			
			logger.info("reading roles and schemas ..")
			
			schemas(cr, p_filters_cfg, include_public, out_dict)			
			ownership(cr, p_filters_cfg, out_dict)		
			roles(cr, out_dict)
			
			logger.info("reading tables ..")			
			tables(cr, p_filters_cfg, out_dict)

			if not "sequences" in out_dict["content"].keys():
				out_dict["content"]["sequences"] = {}
			
			logger.info("reading cols ..")
			columns(cr, out_dict, include_colorder)		
			
			logger.info("reading triggers ..")
			triggers(cr, trigger_functions, out_dict)	
			
			logger.info("reading sequences ..")
			sequences(cr, out_dict)
			
			logger.info("reading constraints ..")
			constraints(cr, out_dict)
			
			logger.info("reading indexes ..")
			indexes(cr, out_dict)
					
			logger.info("reading procedures ..")
			procs(cr, p_filters_cfg, trigger_functions, out_dict, genprocsdir=outprocs_dir)
			
			logger.info("reading finished.")

	# if "content" in out_dict.keys() and "replace" in p_filters_cfg.keys():
		
		# chdict = out_dict["content"]	
		# for from_schema, to_schema in p_filters_cfg["replace"]:
			
			# for grp in chdict.keys():
				
				# if not grp in CFG_GROUPS:
					# continue

				# if grp in CFG_LISTGROUPS:
					# continue
										
				# for sch in chdict[grp].keys():

					# if sch == from_schema:					
						# chdict[grp][to_schema] = chdict[grp].pop(sch)

# def destreader(p_conn, p_filters_cfg, out_dict, include_public=False, include_colorder=False):
	
	# logger = logging.getLogger('pgsourcing')
	# with p_conn as cnobj:

		# if cnobj.dict_cursor_factory is None:
			# raise RuntimeError("destreader precisa de cursor dictionary, este driver nao parece ter um")

		# cn = cnobj.getConn()
		# trigger_functions = set()
		
		# with cn.cursor(cursor_factory=cnobj.dict_cursor_factory) as cr:
			
			# logger.info("reading roles and schemas ..")
			
			# schemas(cr, p_filters_cfg, include_public, out_dict)			
			# ownership(cr, p_filters_cfg, out_dict)		
			# roles(cr, out_dict)
			
			# logger.info("reading tables ..")			
			# tables(cr, p_filters_cfg, out_dict)

			# if not "sequences" in out_dict["content"].keys():
				# out_dict["content"]["sequences"] = {}
			
			# logger.info("reading cols ..")
			# columns(cr, out_dict, include_colorder)		
			
			# logger.info("reading triggers ..")
			# triggers(cr, trigger_functions, out_dict)	
			
			# logger.info("reading sequences ..")
			# sequences(cr, out_dict)
			
			# logger.info("reading constraints ..")
			# constraints(cr, out_dict)
			
			# logger.info("reading indexes ..")
			# indexes(cr, out_dict)
					
			# logger.info("reading procedures ..")
			# procs(cr, p_filters_cfg, trigger_functions, out_dict)
			
			# logger.info("reading finished.")

	# if "content" in out_dict.keys() and "replace" in p_filters_cfg.keys():
		
		# chdict = out_dict["content"]	
		# for from_schema, to_schema in p_filters_cfg["replace"]:
			
			# for grp in chdict.keys():
				
				# if not grp in CFG_GROUPS:
					# continue
					
				# if grp in CFG_LISTGROUPS:
					# continue
					
				# for sch in chdict[grp].keys():

					# if sch == from_schema:					
						# chdict[grp][to_schema] = chdict[grp].pop(sch)

			

# Teste ad-hoc

#if __name__ == "__main__":
#	print(gen_where_from_re_list("campo", ["sra_.*", "sre[a-z]"]))
	
