# -*- coding: utf-8 -*-

#=======================================================================
# Licença MIT (MIT)
#
# Copyright © 2020 Rui Pedro Cavaco Barrosa
#
# Por este meio, é dada permissão, livre de encargos, a qualquer pessoa
# de obter uma cópia deste software e da documentação associada
# (o "Software"), de negociar o Software sem restrições, incluindo os
# di#reitos de usar, copiar, modificar, fundir, publicar, distribuir,
# sublicenciar e/ou vender cópias do Software, sem restrições, e de per-
# mitir a quem o Software seja fornecido que o faça também, sob a se-
# guinte condição: a notificação de copyright e a notificação de permis-
# sões concedidas, dadas acima, deverão ser incluídas em todas as có-
# pias ou partes substanciais do Software.
#
# O SOFTWARE É FORNECIDO "AS IS", TAL COMO SE ENCONTRA, SEM GARANTIAS DE
# QUALQUER TIPO, SEJAM EXPLÍCITAS OU IMPLÍCITAS, INCLUINDO, MAS NÃO SE
# LIMITANDO A, GARANTIAS DE COMERCIALIZAÇÃO, DE ADEQUAÇÃO PARA UM
# PROPÓSITO ESPECÍFICO E DE NÃO TRANSGRESSÃO DA LEI. EM CASO ALGUM SERÃO
# ADMISSÍVEIS REIVINDICAÇÕES POR PERDAS E DANOS IMPUTÁVEIS AOS AUTORES
# OU DETENTORES DO COPYRIGHT, DECORRENTES DA UTILIZAÇÃO, LEGAL OU ILÍCI-
# TA, DO SOFTWARE OU DE QUALQUER FORMA LIGADOS AO SOFTWARE OU A DERIVA-
# ÇÕES DO MESMO.
#=======================================================================

import re
import logging
import codecs
import csv

from os.path import exists, join as path_join
from psycopg2.errors import UndefinedFunction, InsufficientPrivilege
from shutil import rmtree

from src.sql import SQL
from src.common import PROC_SRC_BODY_FNAME, CFG_GROUPS, CFG_LISTGROUPS, OPS_DBCHECK, FLOAT_TYPES, INT_TYPES, gen_proc_fname_row, reverse_shortpgdtype
from src.fileandpath import clear_dir

from src.write import create_function

WARN_KEYS = {
	"PROC_SU_OWNED": "Procedimentos cujo owner e' 'postgres'",
	"TABLE_SU_OWNED": "Tabelas cujo owner e' 'postgres'"
}

SQL_GET_TS = """select ts.spcname
from pg_database d
join pg_tablespace ts
on d.dattablespace = ts.oid
where d.datname = %s"""

def gen_where_from_re_list(p_filter_fieldname, p_re_list, dojoin=False, intersect=False):	
	
	if intersect:
		prevjoiner = "and %s"
	else:
		prevjoiner = "or %s"
		
	lst = ["%s ~* '%s'" % (p_filter_fieldname,ire) for ire in p_re_list]
	seq = " or ".join(lst)
	
	if dojoin and len(lst) > 1:
		seq = "(%s)" % seq

	if len(seq) < 1:
		ret = None
	else:		
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
	
def gen_obj_where_from_re_list(p_objfilter_fieldname, p_curr_schema, p_filters_cfg, dojoin=False, intersect=False, key=None):

	wherecl = None

	if key in p_filters_cfg and len(p_filters_cfg[key].keys()) > 0 and p_curr_schema in p_filters_cfg[key].keys():
		wherecl = gen_where_from_re_list(p_objfilter_fieldname, p_filters_cfg[key][p_curr_schema], dojoin=dojoin, intersect=intersect)
			
	return wherecl

def gen_views_where_from_re_list(p_schemafilter_fieldname, p_viewfilter_fieldname, p_curr_schema, p_filters_cfg, dojoin=False, intersect=False):

	wherecl = None

	if "views" in p_filters_cfg and len(p_filters_cfg["views"].keys()) > 0 and p_curr_schema in p_filters_cfg["views"].keys():
		wherecl = gen_where_from_re_list(p_viewfilter_fieldname, p_filters_cfg["views"][p_curr_schema], dojoin=dojoin, intersect=intersect)
			
	return wherecl

def gen_matviews_where_from_re_list(p_schemafilter_fieldname, p_viewfilter_fieldname, p_curr_schema, p_filters_cfg, dojoin=False, intersect=False):

	wherecl = None

	if "matviews" in p_filters_cfg and len(p_filters_cfg["matviews"].keys()) > 0 and p_curr_schema in p_filters_cfg["matviews"].keys():
		wherecl = gen_where_from_re_list(p_viewfilter_fieldname, p_filters_cfg["matviews"][p_curr_schema], dojoin=dojoin, intersect=intersect)
			
	return wherecl


def gen_procs_where_from_re_list(p_schemafilter_fieldname, p_procfilter_fieldname, p_curr_schema, p_filters_cfg, dojoin=False, intersect=False):

	wherecl = None

	if "procedures" in p_filters_cfg and len(p_filters_cfg["procedures"].keys()) > 0 and p_curr_schema in p_filters_cfg["procedures"].keys():
		wherecl = gen_where_from_re_list(p_procfilter_fieldname, p_filters_cfg["procedures"][p_curr_schema], dojoin=dojoin, intersect=intersect)
			
	return wherecl

def schema_dependency(out_dict, p_found_schema, p_schema_name, p_typekey):
	
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
			schdepobj[p_found_schema] = { p_typekey: [p_schema_name] }
		else:
			if not p_schema_name in schdepobj[p_found_schema][p_typekey]:
				schdepobj[p_found_schema][p_typekey].append(p_schema_name)

def srvanddb_metadata(p_conn, out_dict):
		
	logger = logging.getLogger('pgsourcing')
		
	cn = p_conn.getConn()
	
	if not "pg_metadata" in out_dict.keys():
		out_dict["pg_metadata"] = {}	
	
	out_dict["pg_metadata"]["database"] = p_conn.getDb()
	
	do_run = True
	step = 0
	count = 0
	ret_majorversion = None
	
	while do_run and count <= 6:
		
		count += 1
		try:
			
			with cn.cursor() as cr:		

				step_mark = 1
				if step == step_mark:
					out_dict["pg_metadata"]["pg_version"] = "unknown"
				elif step < step_mark:
					step = step_mark
					cr.execute("select version()")
					row = cr.fetchone()	
					out_dict["pg_metadata"]["pg_version"] = row[0]

					m = re.match("PostgreSQL[\s]+(\d+\.\d+)", row[0])
					if m:
						vval = float(m.group(1))
						if vval >= 10:
							majorver = int(vval) 
						else:
							majorver = vval
						ret_majorversion = majorver
						out_dict["pg_metadata"]["pg_major_version"] = majorver

				step_mark = 2
				if step == step_mark:
					out_dict["pg_metadata"]["postgis"] = { "found": "false" }
				elif step < step_mark:
					step = step_mark		
					cr.execute("select Postgis_full_version()")
					row = cr.fetchone()	
					out_dict["pg_metadata"]["postgis"] = { "found": "true", "version": row[0] }

				step_mark = 3
				if step == step_mark:
					out_dict["pg_metadata"]["pgrouting"] = { "found": "false" }
				elif step < step_mark:
					step = step_mark				
					cr.execute("select pgr_version()")
					row = cr.fetchone()	
					out_dict["pg_metadata"]["pgrouting"] = { "found": "true", "version": row[0] }

				step_mark = 4
				if step == step_mark:
					out_dict["pg_metadata"]["tablespace"] = "NULL"
				elif step < step_mark:
					step = step_mark				
					cr.execute(SQL_GET_TS, (out_dict["pg_metadata"]["database"],))
					row = cr.fetchone()	
					out_dict["pg_metadata"]["tablespace"] = row[0]
									
				do_run = False

		except UndefinedFunction as e:
			cn.rollback()
				
		except Exception as e:			 
			logger.exception("")
			cn.rollback()
			
	return ret_majorversion
			
def get_grants(p_the_dict, p_cursor, forschemata=False):

	def _getit(p_pthe_dict, p_pcursor, p_sch, p_obj = None):
		if not p_obj is None:
			p_pcursor.execute(SQL["GRANTS"], (p_sch, p_obj))
		else:
			p_pcursor.execute(SQL["SCHGRANTS"], (p_sch,))
		for row in p_pcursor:
			if row["allprivs"]:
				privs = "ALL"
			else:
				privs = row["privileges"]					
			if not p_obj is None:
				if not "grants" in p_pthe_dict[p_sch][p_obj].keys():
					p_pthe_dict[p_sch][p_obj]["grants"] = {}					
				p_pthe_dict[p_sch][p_obj]["grants"][row["grantee"]] = privs
			else:
				if not "grants" in p_pthe_dict[p_sch].keys():
					p_pthe_dict[p_sch]["grants"] = {}					
				p_pthe_dict[p_sch]["grants"][row["grantee"]] = privs
					
	for sch in p_the_dict.keys():		
		if forschemata:
			_getit(p_the_dict, p_cursor, sch)			
		else:					
			for obj in p_the_dict[sch].keys():				
				_getit(p_the_dict, p_cursor, sch, p_obj = obj)
			

	
def schemata(p_cursor, p_filters_cfg, p_db_direction, out_dict):
	
	if "schema" in p_filters_cfg and len(p_filters_cfg["schema"]) > 0:
		sql = "%s %s" % (SQL["SCHEMAS"], gen_where_from_list("schema_name", p_filters_cfg["schema"]))
	else:
		sql = SQL["SCHEMAS"]
		
	p_cursor.execute(sql)
	
	if not "content" in out_dict.keys():
		out_dict["content"] = {}	

	owners = set()
	the_dict = {}
	sch_found = False
	for row in p_cursor:
		
		if not "schemata" in out_dict["content"].keys():
			out_dict["content"]["schemata"] = {}
		the_dict = out_dict["content"]["schemata"]
			
		schema_name = row["schema_name"]
		schema_owner = row["schema_owner"]
		if schema_name.startswith("pg_") or schema_name == "information_schema":
			continue
		# if schema_name == "public" and not p_include_public:
		# 	continue

		sch_found = True

		owners.add(schema_owner)	
		the_dict[schema_name] = {
			"schdetails": { "auth": schema_owner }
		}	

	if not sch_found and p_db_direction == "source": 
		raise RuntimeError(f"no schemata -- blocked access, missing schemata in source database or wrong schemata filters in config")
		
	get_grants(the_dict, p_cursor, forschemata=True)
		
	out_dict["content"]["owners"] = list(owners)

def ownership(p_cursor, p_filters_cfg, out_dict):

	if not "content" in out_dict.keys():
		out_dict["content"] = {}	
	
	# Procedures
	if "owners" in out_dict["content"].keys():
		owners = set(out_dict["content"]["owners"])
	else:
		owners = set()

	if not "schemata" in out_dict["content"].keys():
		return
		# raise RuntimeError("no schemata in source: blocked access, missing schemata in source database or wrong schemata filters in config")
	
	for sch in out_dict["content"]["schemata"]:
		
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
	for sch in out_dict["content"]["schemata"]:
		
		wherecl = gen_obj_where_from_re_list("tablename", sch, p_filters_cfg, dojoin=True, intersect=True, key="tables")

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
			
def roles(p_cursor, p_filters_cfg, out_dict):

	if not "content" in out_dict.keys():
		out_dict["content"] = {}	
			
	p_cursor.execute(SQL["ROLES"])
	
	ownerfilter = set()
	if "owners" in out_dict["content"].keys() and len(out_dict["content"]["owners"]) > 0:
		ownerfilter.update(out_dict["content"]["owners"])

	if "roles" in p_filters_cfg.keys() and len(p_filters_cfg["roles"]) > 0:
		ownerfilter.update(p_filters_cfg["roles"])
	
	for row in p_cursor:

		if not "roles" in out_dict["content"].keys():
			out_dict["content"]["roles"] = {}
		
		rl_dict = out_dict["content"]["roles"]
		
		rolename = row["rolname"]
		if rolename.startswith("pg_") or \
			rolename == "postgres":
				continue
				
		if rolename not in ownerfilter:
				continue 
				
		# if row["rolvaliduntil"] is None:
		# 	validuntil = "None"
		# else:
		# 	validuntil = row["rolvaliduntil"].isoformat()
			
		rl_dict[rolename] = {
			"inherit": str(row["rolinherit"]),
			"canlogin": str(row["rolcanlogin"])
			# "validuntil": validuntil
		}

def udttypes(p_cursor, p_filters_cfg, out_dict):

	# logger = logging.getLogger('pgsourcing')

	assert "content" in out_dict.keys(), "missing content in output dict"
	# assert "schemata" in out_dict["content"].keys(), "'content.schemata' em falta no dic. de saida"
	if "schemata" not in out_dict["content"].keys():
		return

	for sch in out_dict["content"]["schemata"]:

		if "udttypes" in p_filters_cfg.keys():
			if len(p_filters_cfg["udttypes"].keys()) > 0  and sch not in p_filters_cfg["udttypes"].keys():
				continue 
			
		wherecl = gen_obj_where_from_re_list("t.typname", sch, p_filters_cfg, dojoin=True, intersect=True, key="udttypes")
		if wherecl is None:
			wherecl = "and n.nspname = '%s'" % sch
		else:
			wherecl = "and n.nspname = '%s' %s" % (sch, wherecl)

		sql = "%s %s" % (SQL["UDTENUMS"], wherecl)
		#print(p_cursor.description)
		
		p_cursor.execute(sql)
		the_dict = None

		for row in p_cursor:

			if not "udttypes" in out_dict["content"].keys():
				out_dict["content"]["udttypes"] = {}
			
			the_dict = out_dict["content"]["udttypes"]

			# if row["schemaname"] == "public" and not p_include_public:
				# continue
				
			if not row["schema"] in the_dict.keys():
				sch_dict = the_dict[row["schema"]] = {}	
			else:
				sch_dict = the_dict[row["schema"]]	
				
			sch_dict[row["typname"]] = {
				"udttype": "enum",
				"labels": row["labels"],
				"owner": row["typowner"]
			}
			
		# TODO - verificar se necessário
		# if not the_dict is None:
		# 	get_grants(the_dict, p_cursor)


def tables(p_conn, p_cursor, p_filters_cfg, out_dict, opt_rowcount_path=None, usetbs=False):

	logger = logging.getLogger('pgsourcing')

	assert "content" in out_dict.keys(), "missing content in output dict"
	# assert "schemata" in out_dict["content"].keys(), "'content.schemata' em falta no dic. de saida"
	if "schemata" not in out_dict["content"].keys():
		return

	rowcounts = []

	for sch in out_dict["content"]["schemata"]:

		if "tables" in p_filters_cfg.keys():
			if len(p_filters_cfg["tables"].keys()) > 0  and sch not in p_filters_cfg["tables"].keys():
				continue 
			
		wherecl = gen_obj_where_from_re_list("tablename", sch, p_filters_cfg, dojoin=True, intersect=True, key="tables")

		if wherecl is None:
			wherecl = "and schemaname = '%s'" % sch
		else:
			wherecl = "and schemaname = '%s' %s" % (sch, wherecl)

		sql = "%s %s" % (SQL["TABLES"], wherecl)
		#print(p_cursor.description)
		
		p_cursor.execute(sql)
		#print(p_cursor.mogrify(sql))
		the_dict = None

		for row in p_cursor:

			if not "tables" in out_dict["content"].keys():
				out_dict["content"]["tables"] = {}
			
			the_dict = out_dict["content"]["tables"]

			# if row["schemaname"] == "information_schema":
				# continue
			
			# if row["schemaname"] == "public" and not p_include_public:
				# continue
				
			## remover tabelas internas ArcGIS
			m = re.match("[adi][\d]+", row["tablename"])
			if not m is None:
				continue

			if not row["schemaname"] in the_dict.keys():
				sch_dict = the_dict[row["schemaname"]] = {}	
			else:
				sch_dict = the_dict[row["schemaname"]]	
				
			sch_dict[row["tablename"]] = {
				"owner": row["tableowner"]
			}
			
			if not row["tablespace"] is None and usetbs:
				sch_dict[row["tablename"]]["tablespace"] = row["tablespace"]

			if not opt_rowcount_path is None:
				rowcounts.append([sch, row["tablename"], 0])

		if not the_dict is None:
			get_grants(the_dict, p_cursor)

	if len(rowcounts) > 0:
		try:
			with open(opt_rowcount_path, 'w', newline='') as csvfl:
				csvwriter = csv.writer(csvfl, delimiter=';',
								quotechar='"', quoting=csv.QUOTE_MINIMAL)
				csvwriter.writerow(('Schema', 'Table', 'Row count'))
				for rc_row in rowcounts:
					p_cursor.execute(SQL["ROW_COUNT"].format(rc_row[0], rc_row[1]))
					frow = p_cursor.fetchone()
					rc_row[2] = frow[0]
					csvwriter.writerow(rc_row)
		except PermissionError:
			logger.error("- CSV file probably opened in Excel, must be closed first,")
			logger.exception("------- tables --------")
		except InsufficientPrivilege:
			p_conn.rollback()
			logger.error("- table {}.{}, insuficient privileges for reading".format(rc_row[0], rc_row[1]))

def views(p_cursor, p_filters_cfg, out_dict):

	# Previously tested on "tables"
	assert "content" in out_dict.keys(), "missing content in output dict"
	# assert "schemata" in out_dict["content"].keys(), "'content.schemata' em falta no dic. de saida"

	if not "schemata" in out_dict["content"].keys():
		return

	for sch in out_dict["content"]["schemata"]:

		if "views" in p_filters_cfg.keys():
			if len(p_filters_cfg["views"].keys()) > 0 and sch not in p_filters_cfg["views"].keys():
				continue 
			
		wherecl = gen_views_where_from_re_list("schemaname", "viewname", sch, p_filters_cfg)
		if wherecl is None:
			wherecl = "where schemaname = '%s'" % sch
		else:
			wherecl = "%s and schemaname = '%s'" % (wherecl, sch)
			
		sql = "%s %s" % (SQL["VIEWS"], wherecl)

		p_cursor.execute(sql)
		#print(p_cursor.mogrify(sql))
		the_dict =  None

		for row in p_cursor:

			if not "views" in out_dict["content"].keys():
				out_dict["content"]["views"] = {}
			
			the_dict = out_dict["content"]["views"]

			# if row["schemaname"] == "information_schema":
				# continue
			
			# if row["schemaname"] == "public" and not p_include_public:
				# continue
				
			# ## remover tabelas internas ArcGIS
			# m = re.match("i[\d]+", row["tablename"])
			# if not m is None:
				# continue

			if not row["schemaname"] in the_dict.keys():
				sch_dict = the_dict[row["schemaname"]] = {}	
			else:
				sch_dict = the_dict[row["schemaname"]]	
				
			sch_dict[row["viewname"]] = {
				"vdetails": {
					"vowner": row["viewowner"],
					"vdef": row["definition"]
				}
			}
			
		if not the_dict is None:
			get_grants(the_dict, p_cursor)

def matviews(p_cursor, p_filters_cfg, p_deftablespace, out_dict, usetbs=False):

	# Previously tested on "tables"
	assert "content" in out_dict.keys(), "missing content in output dict"
	# assert "schemata" in out_dict["content"].keys(), "'content.schemata' em falta no dic. de saida"

	if not "schemata" in out_dict["content"].keys():
		return

	for sch in out_dict["content"]["schemata"]:

		if "matviews" in p_filters_cfg.keys():
			if len(p_filters_cfg["matviews"].keys()) > 0 and sch not in p_filters_cfg["matviews"].keys():
				continue 
			
		wherecl = gen_matviews_where_from_re_list("schemaname", "matviewname", sch, p_filters_cfg)
		if wherecl is None:
			wherecl = "where schemaname = '%s'" % sch
		else:
			wherecl = "%s and schemaname = '%s'" % (wherecl, sch)

		sql = "%s %s" % (SQL["MATVIEWS"], wherecl)		
		p_cursor.execute(sql)
		#print(p_cursor.mogrify(sql))
		the_dict = None

		for row in p_cursor:
	
			if not "matviews" in out_dict["content"].keys():
				out_dict["content"]["matviews"] = {}
			
			the_dict = out_dict["content"]["matviews"]
			
			# if row["schemaname"] == "information_schema":
				# continue
			
			# if row["schemaname"] == "public" and not p_include_public:
				# continue
				
			# ## remover tabelas internas ArcGIS
			# m = re.match("i[\d]+", row["tablename"])
			# if not m is None:
				# continue

			if not row["schemaname"] in the_dict.keys():
				sch_dict = the_dict[row["schemaname"]] = {}	
			else:
				sch_dict = the_dict[row["schemaname"]]	
				
			# if row["ispopulated"]:
				# pop = "True"
			# else:
				# pop = "False"
			
			if row["tablespace"] is None:
				tblspc = p_deftablespace
			else:
				tblspc = row["tablespace"]
				
			if usetbs:
				sch_dict[row["matviewname"]] = {
					"mvdetails": {
						"vowner": row["viewowner"],
						"vdef": row["definition"],
						"vtablespace": tblspc
					}
				}
			else:
				sch_dict[row["matviewname"]] = {
					"mvdetails": {
						"vowner": row["viewowner"],
						"vdef": row["definition"]
					}
				}
		
		if not the_dict is None:	
			get_grants(the_dict, p_cursor)
			
def columns(p_cursor, o_unreadable_tables_dict, out_dict):
	
	assert "content" in out_dict.keys(), "missing content in output dict"
	
	if not "tables" in out_dict["content"].keys():
		return
		
	# assert "tables" in out_dict["content"].keys(), "'content.tables' em falta no dic. de saida"

	pattern = re.compile(r"nextval\('  (?P<schema>[^\.]+) \. (?P<seqname>[0-9A-Za-z_]+)  ", re.VERBOSE)			
	pattern2 = re.compile(r"nextval\('  (?P<seqname>[0-9A-Za-z_]+)  ", re.VERBOSE)			

	tables_root = out_dict["content"]["tables"]
	optionals = [
					("column_default", "defaultval"), 
					("character_maximum_length", "char_max_len"), 
					("numeric_precision", "num_precision"),
					("numeric_precision_radix", "num_prec_radix"),
					("numeric_scale", "num_scale")
					#("datetime_precision", "dttime_precision")
				]
	
	for schema_name in tables_root.keys():
		
		#unreadable_tables = []
		
		for table_name in tables_root[schema_name].keys():
			
			p_cursor.execute(SQL["COLUMNS"], (schema_name, table_name))
			
			if p_cursor.rowcount < 1:
				if schema_name not in o_unreadable_tables_dict.keys():
					o_unreadable_tables_dict[schema_name] = []
				o_unreadable_tables_dict[schema_name].append(table_name)
				
				if not "error" in tables_root[schema_name][table_name].keys():				
					tables_root[schema_name][table_name]["error"] = []
				tables_root[schema_name][table_name]["error"].append("cols unreadable, review permissions")	
				continue

			for row in p_cursor:
				
				if not "cols" in tables_root[schema_name][table_name]:
					cols_dict = tables_root[schema_name][table_name]["cols"] = {}
				else:
					cols_dict = tables_root[schema_name][table_name]["cols"]
					
				if row["data_type"] == "USER-DEFINED":
					if row["udt_schema"] != "public":
						dt = row["udt_schema"] + "." + row["udt_name"]
					else:
						dt = row["udt_name"]
				elif row["data_type"] == "ARRAY" and row["udt_name"][0] == "_":
					dt = row["udt_name"][1:] + "[]"
				else:
					dt = row["data_type"]

				# print("** row keys", list(row.keys()))
				# print("** row column_default", row["column_default"])

				col_dict = cols_dict[row["column_name"]] = {}
				#if p_include_colorder:
				col_dict["ordpos"] = row["ordinal_position"]
				col_dict["type"] = dt
				col_dict["nullable"] = row["is_nullable"]
				if not row["column_default"] is None:
					col_dict["defaultval"] = row["column_default"]
				else:
					col_dict["defaultval"] ='NULL'
				
				for opt_colname, opt_key in optionals:
					
					if not row[opt_colname] is None:
						
						# detetar dependencias entre schemata, avaliando se
						# sao invocadas sequencias de outros schemata
						#
						if opt_key == "defaultval":
							
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
								
							flagv = isinstance(parsed_val, str)							
							if flagv:
								
								match = pattern.match(parsed_val)
								if not match is None:
									
									found_schema = match.group("schema")
									seqname = match.group("seqname")
									
									# coletar nomes de sequencia em uso		
									if not "sequences" in out_dict["content"].keys():
										out_dict["content"]["sequences"] = {}
																
									if not found_schema in out_dict["content"]["sequences"].keys():
										out_dict["content"]["sequences"][found_schema] = {}
									seqs = out_dict["content"]["sequences"][found_schema]	
									do_continue_onseq = True									
									if not seqname in seqs.keys():
										seqs[seqname] = {}	
									else:
										if "diffoper" in seqs[seqname].keys() and seqs[seqname]["diffoper"] == "update":
											del seqs[seqname]
											do_continue_onseq = False
									if do_continue_onseq:
										if not "serialcols_dependencies" in seqs[seqname].keys(): 	
											seqs[seqname]["serialcols_dependencies"] = {}	
										seqs[seqname]["serialcols_dependencies"][table_name] = row["column_name"]	
									if found_schema != schema_name:
										schema_dependency(out_dict, found_schema, schema_name, "sequences")
									col_dict[opt_key] = parsed_val	
									
								else:

									match = pattern2.match(parsed_val)
									if not match is None:
										seqname = match.group("seqname")
										# coletar nomes de sequencia em uso , usar o prorprio schema da tabela										
										if not "sequences" in out_dict["content"].keys():
											out_dict["content"]["sequences"] = {}

										if not schema_name in out_dict["content"]["sequences"].keys():
											out_dict["content"]["sequences"][schema_name] = {}
											
										seqs = out_dict["content"]["sequences"][schema_name]											
										if not seqname in seqs.keys():
											seqs[seqname] = {}			
										col_dict[opt_key] = parsed_val.replace(seqname, "%s.%s" % (schema_name, seqname))
									
									else:										
										col_dict[opt_key] = parsed_val
								
						else:

							col_dict[opt_key] = row[opt_colname]	
						
		# print("unreadable_tables:", schema_name, unreadable_tables)

def constraints(p_cursor, p_deftablespace, out_dict, usetbs=False):
	
	assert "content" in out_dict.keys(), "missing content in output dict"
	if not "tables" in out_dict["content"].keys():
		return
	#assert "tables" in out_dict["content"].keys(), "'content.tables' em falta no dic. de saida"

	# pattern = re.compile(r"foreign \s+ key \s+ \([^\)\s]+\) \s+ references \s+ (?P<schema>[^\.]+) \. (?P<tname>[^\(]+)", re.VERBOSE | re.IGNORECASE)			
		
	tables_root = out_dict["content"]["tables"]

	for schema_name in tables_root.keys():
		
		for table_name in tables_root[schema_name].keys():

			if "error" in tables_root[schema_name][table_name].keys():
				continue

			p_cursor.execute(SQL["PKEYS"], (schema_name, table_name))

			for row in p_cursor:
				
				dk = "pkey"
				if not dk in tables_root[schema_name][table_name]:
					constrs_dict = tables_root[schema_name][table_name][dk] = {}
				else:
					constrs_dict = tables_root[schema_name][table_name][dk]
					
				if row["idxtblspc"] is None:
					tblspc = p_deftablespace
				else:
					tblspc = row["idxtblspc"]

				constrs_dict[row["constraint_name"]] = {				
					"columns": row["column_names"]			
				}

				if usetbs:
					constrs_dict[row["constraint_name"]]["index_tablespace"] = tblspc

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

				constrs_dict[row["constraint_name"]] = {
				
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

				constrs_dict[row["conname"]] = {				
					"cdef": row["cdef"],
					"schema_ref": row["schema_ref"],
					"table_ref": row["table_ref"],
					"matchtype": row["matchtype"],
					"updtype": row["updtype"],
					"deltype": row["deltype"]				
				}
				# detetar dependencias entre schemata, avaliando se
				# sao invocadas tabelas de outros schemata nas foreign keys
				#

				schema_dependency(out_dict, row["schema_ref"], schema_name, "fkeys")

def indexes(p_cursor, p_deftablespace, out_dict, usetbs=False):
	
	# ATENCAO - indexes deve ser corrido depois de constraints, para poder filtrar
	# os indices associados a primary keys e que sao implicitos,
	# nao precisando de ser listados.
	
	assert "content" in out_dict.keys(), "missing content in output dict"
	if not "tables" in out_dict["content"].keys():
		return
	#assert "tables" in out_dict["content"].keys(), "'content.tables' em falta no dic. de saida"

	# pattern = re.compile(r"foreign \s+ key \s+ \([^\)\s]+\) \s+ references \s+ (?P<schema>[^\.]+) \. (?P<tname>[^\(]+)", re.VERBOSE | re.IGNORECASE)			
		
	tables_root = out_dict["content"]["tables"]
	
	for schema_name in tables_root.keys():
		
		for table_name in tables_root[schema_name].keys():

			if "error" in tables_root[schema_name][table_name].keys():
				continue

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
						
					if row["tablespace"] is None:
						tblspc = p_deftablespace
					else:
						tblspc = row["tablespace"]

					if usetbs:
						constrs_dict[row["indexname"]] = { 
							"idxdesc": idxdef,
							"tablespace": tblspc
						}
					else:
						constrs_dict[row["indexname"]] = { 
							"idxdesc": idxdef
						}

def sequences(p_conn, p_majorversion, out_dict):
	
	assert "content" in out_dict.keys(), "missing content in output dict"

	if not "sequences" in out_dict["content"].keys():
		return
		
	seq_root = out_dict["content"]["sequences"]
	items = [
			"start_value", "minimum_value", "maximum_value", "increment", "cycle_option"
			]

	logger = logging.getLogger('pgsourcing')
		
	cn = p_conn.getConn()
	
	try:
		
		with cn.cursor(cursor_factory=p_conn.dict_cursor_factory) as cr:		

			for schema_name in seq_root.keys():
				
				for seq_name in seq_root[schema_name].keys():

					cr.execute(SQL["SEQUENCES"], (schema_name, seq_name))

					row =  cr.fetchone()
					
					if row is None:
						raise RuntimeError(f"cannot read sequence definitions on schema {schema_name}, most probably the aplicational role being used in this project is lacking the required read permissions")
						# the_dict["empty"] = "true"
					else:
						the_dict = seq_root[schema_name][seq_name]["seqdetails"] = { }
						for item in items:
							# print(item, row[item])
							if not row[item] is None:
								the_dict[item] = row[item]	
								
						cr.execute(SQL["SEQ_CURRVAL"], (schema_name, seq_name))				
						row = cr.fetchone()						
						the_dict["current"] = row["current_value"]	
						
						if p_majorversion < 10:
							cr.execute(SQL["SEQ_CACHEVALUE_PRE10"] % (schema_name, seq_name))				
						else: 
							cr.execute(SQL["SEQ_CACHEVALUE_FROM10"] % (schema_name, seq_name))				
						row = cr.fetchone()						
						the_dict["cache_value"] = row[0]	
						
						cr.execute(SQL["SEQ_OWNER"], (schema_name, seq_name))				
						row = cr.fetchone()						
						the_dict["owner"] = row[0]	

					get_grants(seq_root, cr)
						

	except Exception as e:			 
		logger.exception("")
		cn.rollback()

def triggers(p_cursor, out_trigger_functions, out_dict):
	
	assert "content" in out_dict.keys(), "missing content in output dict"
	if not "tables" in out_dict["content"].keys():
		return
		
	#assert "tables" in out_dict["content"].keys(), "'content.tables' em falta no dic. de saida"
	
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
			
		if "error" in tables_root[t_schema][t_name].keys():
			continue
		
		if not dk in tables_root[t_schema][t_name].keys():
			tables_root[t_schema][t_name][dk] = {}
			
		the_dict = tables_root[t_schema][t_name][dk]
		
		if not row["trigger_name"] in the_dict.keys():
			the_dict[row["trigger_name"]] = {}
		
		details = the_dict[row["trigger_name"]]
		
		out_trigger_functions.add((row["function_schema"], row["function_name"]))
		
		schema_dependency(out_dict, t_schema, row["function_schema"], "triggerfuncs")

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

# 1024: dict_keys(['procedure_name', 'args', 'return_type', 'procedure_owner', 'language_type', 'body' (PROC_SRC_BODY_FNAME), 'provolatile'])


	# if p_row["return_type"] == "record" and "return_table" in p_row.keys():
	# 	return_type = f"TABLE({ p_row['return_table'] })"
	# else:
	# 	return_type = p_row["return_type"]

	print("1136:", p_row.keys())

	return_type = None
	if p_row["return_type"] == "record":
		if not p_row["return_table"] is None:
			return_type = f"TABLE({p_row['return_table']})"
	elif p_row["return_type"] == "_text":
		return_type = "text[]"

	if return_type is None:
		return_type = p_row["return_type"]

	return template % (p_sch, p_row["procedure_name"], p_row["fargs"],
		return_type, p_row["language_type"], volatdict[p_row["provolatile"]])

def gen_proc_ftr(p_sch, p_row):

	template = """$BODY$;\n
ALTER FUNCTION %s.%s(%s) OWNER TO %s;"""

	return template % (p_sch, p_row["procedure_name"], p_row["args"],
		p_row["procedure_owner"])

def gen_proc_file(p_genprocsdir, p_schema, p_proc_row, winendings=False):
	
	fname = gen_proc_fname_row(p_proc_row)
	complfname = "%s.%s.sql" % (p_schema, fname) 

	flines = []
	# print(">>", p_proc_row.keys())
	# dict_keys(['procedure_name', 'args', 'fargs', 'return_type', 'procedure_owner', 'language_type', 'body', 'provolatile'])

	usable_proc = p_proc_row['procedure_name']
	create_function(p_schema, usable_proc, p_proc_row, flines, replace=True)	
	# print("...", flines[3:])					
	
	# try:
	# 	procsrc = p_proc_row[PROC_SRC_BODY_FNAME].decode('utf-8')
	# except AttributeError:
	# 	procsrc = p_proc_row[PROC_SRC_BODY_FNAME]

	content = "\n".join(flines)

	with codecs.open(path_join(p_genprocsdir, complfname), "wb", "utf-8") as fl:
			#print("......", str(type(row[PROC_SRC_BODY_FNAME])))
		if winendings:
			fl.write(content.replace("\n","\r\n"))
		else:
			fl.write(content)
	
	# if winendings:
	# 	with codecs.open(path_join(p_genprocsdir, complfname), "wb", "utf-8") as fl:
	# 		#print("......", str(type(row[PROC_SRC_BODY_FNAME])))
	# 		fl.write(gen_proc_hdr(p_schema, p_proc_row).replace("\n","\r\n"))
	# 		fl.write(procsrc.replace("\n","\r\n"))									
	# 		fl.write(gen_proc_ftr(p_schema, p_proc_row).replace("\n","\r\n"))
	# else:
	# 	with codecs.open(path_join(p_genprocsdir, complfname), "w", "utf-8") as fl:
	# 		#print("......", str(type(row[PROC_SRC_BODY_FNAME])))
	# 		fl.write(gen_proc_hdr(p_schema, p_proc_row))
	# 		fl.write(procsrc)									
	# 		fl.write(gen_proc_ftr(p_schema, p_proc_row))						
						
def procs(p_cursor, p_filters_cfg, in_trigger_functions, p_majorversion, out_dict, genprocsdir=None):
	
	assert "content" in out_dict.keys(), "missing content in output dict"
	# assert "schemata" in out_dict["content"].keys(), "'content.schemata' em falta no dic. de saida"

	if not "schemata" in out_dict["content"].keys():
		return

	patt = "[\s]?\,[\s]?"

	trig_dict = {}
	for tr_schema, tr_funcname in in_trigger_functions:
		if not tr_schema in trig_dict.keys():
			trig_dict[tr_schema] = []
		trig_dict[tr_schema].append(tr_funcname)

	items = ["procedure_name", "args", "fargs", "return_type", "procedure_owner", 
			 "language_type", PROC_SRC_BODY_FNAME, "provolatile"]

	if p_majorversion < 11:
		dict_key = "PROCS_PRE11"
	else:
		dict_key = "PROCS_FROM11"

	if not genprocsdir is None:
		clear_dir(genprocsdir, ".sql")

	tr_funcnames = []
	the_dict = {}
	for sch in out_dict["content"]["schemata"]:
		
		if "procedures" in p_filters_cfg.keys():			
			if len(p_filters_cfg["procedures"].keys()) > 0 and sch not in p_filters_cfg["procedures"].keys():
				continue 
				
		if sch in trig_dict.keys():
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

		# print("sql:", sql)
		
		p_cursor.execute(sql)		
		for row in p_cursor:
			
			# if row["schema"] == "public" and not p_include_public:
				# continue
			
			if not row["procedure_schema"] in the_dict.keys():
				schdict = the_dict[row["procedure_schema"]] = {}
			else:
				schdict = the_dict[row["procedure_schema"]]
				
			procfname = gen_proc_fname_row(row)
			pdict = schdict[procfname] = {}
			for item in items:
				if not row[item] is None:
					if item == "return_type":
						pdict[item] = reverse_shortpgdtype(row[item]) 
					elif item == "args":
						pdict[item] = ", ".join([" ".join(frstsplit.split()[1:]) for frstsplit in re.split(patt, row[item]) if len(frstsplit)>0])
						# print("1268 pdict[item]:", pdict[item])
					else:
						pdict[item] = row[item]

	for schname in the_dict.keys():

		schdict = the_dict[schname]
		for procfname in schdict.keys():

			pdict = schdict[procfname]

			if "return_type" in pdict.keys() and pdict["return_type"] == "record":
				p_cursor.execute(SQL["PROCS_RETTYPE_TABLE"], (schname, pdict["procedure_name"]))		
				row = p_cursor.fetchone()
				pdict["return_table"] = row[0] 

			p_cursor.execute(SQL["PROCS_XACL"], (schname, pdict["procedure_name"]))
			for row in p_cursor:
				if not "exec_acl" in pdict.keys():
					pdict["exec_acl"] = []
				pdict["exec_acl"].append(row["nome"])

			if not genprocsdir is None:
				gen_proc_file(genprocsdir, schname, pdict)


	# trigger funcs
	if len(tr_funcnames) > 0:
		
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
				
			procfname = gen_proc_fname_row(row)
			pdict = schdict[procfname] = {}
			for item in items:
				if not row[item] is None:
					pdict[item] = row[item]

			if not genprocsdir is None:
				gen_proc_file(genprocsdir, sch, row)

def paramtables(p_cursor, p_filters_cfg, p_gendumpsdir):
		
	if not "parameterstables" in p_filters_cfg.keys() or p_gendumpsdir is None:	
		return
		
	assert exists(p_gendumpsdir)
				
	for sch in p_filters_cfg["parameterstables"].keys():
		
		# for tname_patt in  p_filters_cfg["parameterstables"][sch]:

		wherecl = gen_obj_where_from_re_list("tablename", sch, p_filters_cfg, dojoin=True, intersect=True, key="parameterstables")
		if wherecl is None:
			wherecl = "and schemaname = '%s'" % sch
		else:
			wherecl = "and schemaname = '%s' %s" % (sch, wherecl)

		sql = "%s %s" % (SQL["TABLES"], wherecl)
		p_cursor.execute(sql)
		#print(sql)

		work_list = []
		for row in p_cursor:

			ftname = "%s.%s" % (row[0], row[1])
			fname = "%s.copy" % (ftname)
			fullp = path_join(p_gendumpsdir, fname)

			work_list.append((fullp, row[0], row[1]))
			
		for fullp, ftschema, ftname in work_list:	
			with open(fullp, "wb") as fp:
				p_cursor.execute(f'SET search_path TO {ftschema}')
				p_cursor.copy_to(fp, ftname)
						
def dbreader(p_conn, p_filters_cfg, out_dict, outtables_dir, 
		outprocs_dir=None, is_upstreamdb=None, opt_rowcount_path=None, 
		code_only=False, usetbs=False):

	logger = logging.getLogger('pgsourcing')
	with p_conn as cnobj:

		if cnobj.dict_cursor_factory is None:
			raise RuntimeError("dbreader requires a cursor dictionary, this driver doesn't seem to have one")

		trigger_functions = set()

		if is_upstreamdb is None:
			db_direction = ""
		elif is_upstreamdb:
			db_direction = "source"
		else:
			db_direction = "destination"

		logger.info(f"reading server and {db_direction}db metadata ...")
		
		majorversion = srvanddb_metadata(cnobj, out_dict)
			
		cn = cnobj.getConn()
		with cn.cursor(cursor_factory=cnobj.dict_cursor_factory) as cr:
			
			logger.info("reading roles and schemata ..")
			
			schemata(cr, p_filters_cfg, db_direction, out_dict)		

			if not code_only:

				ownership(cr, p_filters_cfg, out_dict)		
				roles(cr, p_filters_cfg, out_dict)

				logger.info("reading tables ..")			
				tables(cn, cr, p_filters_cfg, out_dict, 
					opt_rowcount_path=opt_rowcount_path, usetbs=usetbs)

				logger.info("reading user datatypes (enums) ..")
				udttypes(cr, p_filters_cfg, out_dict)

				unreadable_tables = {}
				
				logger.info("reading cols ..")
				columns(cr, unreadable_tables, out_dict)		
				
				#print("ur tables:", unreadable_tables)
				
				logger.info("reading triggers ..")
				triggers(cr, trigger_functions, out_dict)	
			
		if not code_only:
			logger.info("reading sequences ..")
			sequences(cnobj, majorversion, out_dict)
		
		with cn.cursor(cursor_factory=cnobj.dict_cursor_factory) as cr:
			
			if not code_only:

				logger.info("reading constraints ..")
				constraints(cr, out_dict["pg_metadata"]["tablespace"], out_dict, usetbs=usetbs)
				
				logger.info("reading indexes ..")
				indexes(cr, out_dict["pg_metadata"]["tablespace"], out_dict, usetbs=usetbs)

				logger.info("reading views ..")			
				views(cr, p_filters_cfg, out_dict)

				logger.info("reading mat.views ..")			
				matviews(cr, p_filters_cfg, 
				out_dict["pg_metadata"]["tablespace"], out_dict, usetbs=usetbs)
					
			logger.info("reading procedures ..")
			procs(cr, p_filters_cfg, trigger_functions, majorversion, out_dict, genprocsdir=outprocs_dir)
		
		if not code_only:

			with cn.cursor() as cr:

				logger.info("reading parameter table data ..")
				paramtables(cr, p_filters_cfg, outtables_dir)
						
		logger.info("reading finished.")
			

# Teste ad-hoc

#if __name__ == "__main__":
#	print(gen_where_from_re_list("campo", ["sra_.*", "sre[a-z]"]))
	
