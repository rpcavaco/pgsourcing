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


from src.fileandpath import load_currentref
from src.common import CFG_GROUPS, CFG_LISTGROUPS, COL_ITEMS_CHG_AVOIDING_SUBSTITUTION
from copy import deepcopy

# import pprint as pp

SQL_CREATE_PK = """%s ADD CONSTRAINT %s PRIMARY KEY (%s)
    USING INDEX TABLESPACE %s"""
    
SQL_CREATE_UNIQUE = """%s ADD CONSTRAINT %s UNIQUE (%s)
    USING INDEX TABLESPACE %s"""

SQL_CREATE_CONSTR = "%s ADD CONSTRAINT %s %s"

SQL_CREATE_FK = """%s ADD CONSTRAINT %s %s 
	MATCH %s
    ON UPDATE %s
    ON DELETE %s"""

SQL_DROP_CONSTR = "%s DROP CONSTRAINT %s"

def reverse_proc_fname(p_mangled_fname):

	if "." in p_mangled_fname:
		_sch, rest = p_mangled_fname.split(".")
	else:
		rest = p_mangled_fname
	
	if "#" in rest:
		# has arguments
		procname, rest2 = rest.split("#")
	else:
		procname, rest2 = rest.split("$")
		
	return procname

def changegrp_list(p_chg_group_list, p_currdiff_block_list, p_updates_ids_list):

	changed = False
	
	for op in p_currdiff_block_list:
		if len(p_updates_ids_list) < 1 or op["operorder"] in p_updates_ids_list:
			if op["diffoper"] == "addtolist":
				if op["newvalue"] not in p_chg_group_list:
					p_chg_group_list.append(op["newvalue"])
					changed = True
			elif op["diffoper"] == "removefrom":
				if op["oldvalue"] in p_chg_group_list:
					p_chg_group_list.remove(op["oldvalue"])
				changed = True
				
	return changed

def changegrp(p_chg_group, p_currdiff_block, p_updates_ids_list, p_keys_byref, p_limkeys_list):	
	
	changed = False
	
	if len(p_keys_byref) > 15:
		raise RuntimeError("changegrp excessive recursion, key seq: %s" % str(p_keys_byref))

	# try:
	# 	print("\n||>>", p_keys_byref, p_chg_group.keys(), p_currdiff_block.keys())
	# except AttributeError:
	# 	print('Err:', p_keys_byref, p_chg_group, p_currdiff_block)
	# 	raise

	if isinstance(p_currdiff_block, dict):

		for k in p_currdiff_block.keys():	

			if k == p_keys_byref[-1]:
				raise RuntimeError("changegrp recursive error, identical seq keys: %s + %s" % (str(p_keys_byref), k))

			if not k in p_chg_group.keys():
				p_chg_group[k] = {}

			# objects that can occur within other objects
			if k in ["index"]:

				changed = changed | changegrp(p_chg_group[k], p_currdiff_block[k], p_updates_ids_list, p_keys_byref+[k], p_limkeys_list) 

			else:

				diff_item = p_currdiff_block[k]

				if isinstance(diff_item, list):
					assert isinstance(p_chg_group, list), "Incoerencia: list diff para alterar dicionario: '%s'" % str(diff_item)
					changed = changed | changegrp_list(p_chg_group, diff_item, p_updates_ids_list)
				else:
					if "diffoper" in diff_item.keys():
						if len(p_updates_ids_list) < 1 or diff_item["operorder"] in p_updates_ids_list:
							if len(p_limkeys_list) < 1 or k in p_limkeys_list:
								#print(diff_item["operorder"], p_updates_ids_list)
								if diff_item["diffoper"] == "insert":
									#print(p_keys_byref, k, "oper:", diff_item["diffoper"], "new:", diff_item["newvalue"])	
									p_chg_group[k] = diff_item["newvalue"]
									changed = True
									#print("-->", p_chg_group)	
								elif diff_item["diffoper"] == "update":
									p_chg_group[k] = diff_item["newvalue"]
									changed = True
								elif diff_item["diffoper"] == "delete":
									p_chg_group.pop(k, None)
									changed = True						
								elif diff_item["diffoper"] == "rename":
									p_chg_group[diff_item["newvalue"]] = p_chg_group.pop(k)
									changed = True						
					else:
						changed = changed | changegrp(p_chg_group[k], diff_item, p_updates_ids_list, p_keys_byref+[k], p_limkeys_list) 
					
	elif isinstance(p_currdiff_block, list):
		
		changed = changed | changegrp_list(p_chg_group, p_currdiff_block, p_updates_ids_list) 
			
	return changed

	
def updateref(p_proj, p_difdict, updates_ids_list, limkeys_list):
	
	# print("updates_ids_list:", updates_ids_list)
	# print("limkeys_list:", limkeys_list)
	
	changed = False
	root_ref_json = load_currentref(p_proj)
	ref_json = root_ref_json["content"]
	
	diff_content = p_difdict["content"]

	reading_keys = CFG_GROUPS
	
	for grp in reading_keys:
		
		if grp in diff_content.keys():
		
			currdiff_block = diff_content[grp]
			
			if grp in ref_json.keys():
				
				if grp in CFG_LISTGROUPS:
					
					changed = changed | changegrp_list(ref_json[grp], currdiff_block, updates_ids_list)
								
				else:
					
					changed = changed | changegrp(ref_json[grp], currdiff_block, updates_ids_list, [grp], limkeys_list) 
					
	if changed:
		return root_ref_json
	else:
		return None

def col_create(p_tname, p_colname, p_col):
	
	colitems = [p_col["ordpos"], p_colname, p_col["type"]]
	
	## Larguras dos campos
	
	if p_col["type"].startswith("char") and "char_max_len" in p_col.keys():
		colitems[-1] = "%s(%s)" % (colitems[-1], p_col["char_max_len"])
		
	if p_col["type"] in ("numeric", "float"):
		if "num_precision" in p_col.keys():
			assert p_col["num_prec_radix"] == 2, "updatedb: non binary radix in numeric precision, table %s p_col %s" % (p_tname, p_colname)
			if "num_scale" in p_col.keys():
				colitems[-1] = "%s(%s,%s)" % (colitems[-1], p_col["num_precision"], p_col["num_scale"])
			else:
				colitems[-1] = "%s(%s)" % (colitems[-1], p_col["num_precision"])
		
	if p_col["nullable"] == "NO":
		colitems.append("NOT NULL")
	else:
		colitems.append("")
		
	#if "defaultval" in p_col.keys():
	if p_col["defaultval"] != "NULL":
		colitems.append("DEFAULT %s" % p_col["defaultval"])
	else:
		colitems.append("")
	
	return colitems

def table_operation(p_sch, p_tname, p_diff_item, p_delmode, p_out_sql_src):	

	tmplt = "ALTER TABLE %s.%s"

	if p_diff_item["diffoper"] == "delete":
		
		if p_delmode is None or p_delmode == "NODEL":
			tmplt = "-- DROP TABLE %s.%s"
		elif p_delmode == "DEL":
			tmplt = "DROP TABLE %s.%s"
		elif p_delmode == "CASCADE":
			tmplt = "DROP TABLE %s.%s CASCADE"
		
		p_out_sql_src.append(tmplt % (p_sch,p_tname))


	elif p_diff_item["diffoper"] == "rename":

		p_out_sql_src.append("ALTER TABLE {0}.{1} RENAME TO {0}.{2}".format(p_sch, p_tname, p_diff_item["newvalue"]))


	elif p_diff_item["diffoper"] == "insert":
		
		assert "cols" in p_diff_item["newvalue"]
			# tmplist = ["CREATE TABLE %s.%s" % (p_sch,p_tname)]
		# else:
		tmplist = ["CREATE TABLE %s.%s" % (p_sch,p_tname), "("]
		
		colitems_list = []
		
		for colname in p_diff_item["newvalue"]["cols"].keys():
			col = p_diff_item["newvalue"]["cols"][colname]			
			colitems = col_create(p_tname, colname, col)					
			colitems_list.append(colitems)
				
		colitems_list.sort(key=lambda ci: ci[0])
		
		for cii, cil_item in enumerate(colitems_list):
			cont0 = re.sub("\s\s+", " ", "%s %s %s %s" % tuple(cil_item[1:]))
			cont = "\t" + cont0.strip()
			if cii < (len(colitems_list) - 1):
				tmplist.append(cont + ",")
			else:
				tmplist.append(cont)
				tmplist.append(")")
			
		p_out_sql_src.append(tmplist)

		if "pkey" in p_diff_item["newvalue"].keys():										
			for pkname in p_diff_item["newvalue"]["pkey"].keys():	
				di = p_diff_item["newvalue"]["pkey"][pkname]
				p_out_sql_src.append(SQL_CREATE_PK % (tmplt % (p_sch, p_tname), pkname, ",".join(di["columns"]), di["index_tablespace"]))	

		if "check" in p_diff_item["newvalue"].keys():	
			for cname in p_diff_item["newvalue"]["check"].keys():	
				di = p_diff_item["newvalue"]["check"][cname]
				p_out_sql_src.append(SQL_CREATE_CONSTR % (tmplt % (p_sch, p_tname), cname, di["chkdesc"]))								

		if "fkey" in p_diff_item["newvalue"].keys():										
			for cname in p_diff_item["newvalue"]["fkey"].keys():	
				di = p_diff_item["newvalue"]["fkey"][cname]
				p_out_sql_src.append(SQL_CREATE_FK % (tmplt % (p_sch, p_tname), cname, di["cdef"], di["matchtype"], di["updtype"], di["deltype"]))								

		if "index" in p_diff_item["newvalue"].keys():	
			for cname in p_diff_item["newvalue"]["index"].keys():	
				di = p_diff_item["newvalue"]["index"][cname]
				p_out_sql_src.append("%s TABLESPACE %s" % (di["idxdesc"], di["tablespace"]))						

		if "unique" in p_diff_item["newvalue"].keys():							
			for cname in p_diff_item["newvalue"]["unique"].keys():	
				di = p_diff_item["newvalue"]["unique"][cname]
				p_out_sql_src.append(SQL_CREATE_UNIQUE % (tmplt % (p_sch, p_tname), cname, ",".join(di["columns"]), di["index_tablespace"]))									
		if "trigger" in p_diff_item["newvalue"].keys():	
			for trname in p_diff_item["newvalue"]["trigger"].keys():	
				di = p_diff_item["newvalue"]["trigger"][trname]
				flines = []
				create_trigger(trname, p_sch, p_tname, di, flines)						
				p_out_sql_src.append("".join(flines))

		if "owner" in p_diff_item["newvalue"]:
			p_out_sql_src.append("ALTER TABLE %s.%s OWNER to %s" % (p_sch, p_tname, p_diff_item["newvalue"]["owner"]))

def col_operation(docomment, p_sch, p_tname, p_colname, p_diff_item, p_delmode, p_updates_ids_list, p_out_sql_src, p_out_hdr_flag):
	
	if p_delmode is None or p_delmode == "NODEL":
		tmpltd = "-- ALTER TABLE %s.%s"
	else:
		tmpltd = "ALTER TABLE %s.%s"
		
	tmplt = "ALTER TABLE %s.%s"
	tmplt_new = "ALTER TABLE {}.{}"

	if p_delmode == "CASCADE":
		tmplt2 = "%s DROP COLUMN %s CASCADE"
	else:
		tmplt2 = "%s DROP COLUMN %s"

	assert 	"diffoper" in p_diff_item.keys(), ", ".join(p_diff_item.keys())

	# if "diffoper" in p_diff_item.keys():

	if len(p_updates_ids_list) < 1 or p_diff_item["operorder"] in p_updates_ids_list:
					
		print_tablehdr(docomment, p_sch, p_tname, p_out_sql_src, p_out_hdr_flag)
		if docomment:
			p_out_sql_src.append("-- Op #%d" % p_diff_item["operorder"])
	
		if p_diff_item["diffoper"] == "delete":
			
			p_out_sql_src.append(tmplt2 % (tmpltd % (p_sch, p_tname), p_colname))
			
		elif p_diff_item["diffoper"] == "insert":
			
			colcreatitems = col_create(p_tname, p_colname, p_diff_item["newvalue"])
			cont0 = re.sub("\s\s+", " ",   "\t%s %s %s %s" % tuple(colcreatitems[1:]))
			p_out_sql_src.append("%s ADD COLUMN %s" % (tmplt % (p_sch, p_tname), cont0.strip()))

		elif p_diff_item["diffoper"] == "update":

			prev_changed = False
			if "nullable" in p_diff_item["changedkeys"].split(", "):
				if p_diff_item["newvalue"]["nullable"] == "YES":
					set_drop = "DROP"
				else:
					set_drop = "SET"
				p_out_sql_src.append("{} ALTER COLUMN {} {} NOT NULL".format(tmplt % (p_sch, p_tname), p_colname, set_drop))
				prev_changed = True

			if "type" in p_diff_item["changedkeys"].split(", "):
				p_out_sql_src.append("{} ALTER COLUMN {} TYPE {}".format(tmplt % (p_sch, p_tname), p_colname, p_diff_item["newvalue"]["type"]))
				prev_changed = True

			if "defaultval" in p_diff_item["changedkeys"].split(", "):
				if "defaultval" in p_diff_item["newvalue"].keys() and p_diff_item["newvalue"]["defaultval"] != "NULL":
					p_out_sql_src.append("{} ALTER COLUMN {} SET DEFAULT {}".format(tmplt % (p_sch, p_tname), p_colname, p_diff_item["newvalue"]["defaultval"]))
				else:
					p_out_sql_src.append("{} ALTER COLUMN {} FROP DEFAULT".format(tmplt % (p_sch, p_tname), p_colname))
				prev_changed = True

			if not prev_changed:

				p_out_sql_src.append(tmplt2 % (tmpltd % (p_sch, p_tname), p_colname))
				
				colcreatitems = col_create(p_tname, p_colname, p_diff_item["newvalue"])
				cont0 = re.sub("\s\s+", " ",   "\t%s %s %s %s" % tuple(colcreatitems[1:]))
				p_out_sql_src.append("%s ADD COLUMN %s" % (tmplt % (p_sch, p_tname), cont0.strip()))

		elif p_diff_item["diffoper"] == "rename":

			p_out_sql_src.append("{} RENAME COLUMN {} TO {}".format(tmplt_new.format(p_sch, p_tname), p_colname, p_diff_item["newvalue"]))

	# else:

	# 	print("!! 362 !!:", list(p_diff_item.keys()))

	# 	# TODO: verificar utilidade deste bloco que parece inalcancável
		
	# 	p_substitution = False
	# 	for k in p_diff_item.keys():
	# 		if k not in COL_ITEMS_CHG_AVOIDING_SUBSTITUTION:
	# 			p_substitution = True
	# 			break

	# 	if not p_substitution:
			
	# 		for k in p_diff_item.keys():

	# 			print("!! 372 !!:", k)
				
	# 			if not "operorder" in p_diff_item[k].keys():
	# 				continue
				
	# 			if len(p_updates_ids_list) > 0 and not p_diff_item[k]["operorder"] in p_updates_ids_list:	
	# 				continue
					
	# 			print_tablehdr(docomment, p_sch, p_tname, p_out_sql_src, p_out_hdr_flag)	
	# 			if docomment:			
	# 				p_out_sql_src.append("-- Op #%d" % p_diff_item[k]["operorder"])	

	# 			# if k == "defaultval":				
	# 			# 	if p_diff_item[k]["diffoper"] == "delete":					
	# 			# 		p_out_sql_src.append("ALTER TABLE %s.%s ALTER COLUMN %s DROP DEFAULT" % (p_sch, p_tname, p_colname))
	# 			# 	elif p_diff_item[k]["diffoper"] in ("insert", "update"):	
	# 			# 		newval = p_diff_item[k]["newvalue"]
	# 			# 		if isinstance(newval, str):
	# 			# 			tmpl = "ALTER TABLE %s.%s ALTER COLUMN %s SET DEFAULT '%s'"
	# 			# 		else:
	# 			# 			tmpl = "ALTER TABLE %s.%s ALTER COLUMN %s SET DEFAULT %s"
	# 			# 		p_out_sql_src.append(tmpl % (p_sch, p_tname, p_colname, newval ))				
	# 			if k == "defaultval":				
	# 				if p_diff_item[k]["diffoper"] == "update":	
	# 					newval = p_diff_item[k]["newvalue"]
	# 					if newval != "NULL":
	# 						if isinstance(newval, str):
	# 							tmpl = "ALTER TABLE %s.%s ALTER COLUMN %s SET DEFAULT '%s'"
	# 						else:
	# 							tmpl = "ALTER TABLE %s.%s ALTER COLUMN %s SET DEFAULT %s"
	# 						p_out_sql_src.append(tmpl % (p_sch, p_tname, p_colname, newval ))
	# 					else:
	# 						tmpl = "ALTER TABLE %s.%s ALTER COLUMN %s DROP DEFAULT"
	# 						p_out_sql_src.append(tmpl % (p_sch, p_tname, p_colname ))
	# 			elif k == "nullable":
	# 				if p_diff_item[k]["diffoper"] == "update":
	# 					if p_diff_item[k]["newvalue"] == "NO":
	# 						p_out_sql_src.append("ALTER TABLE %s.%s ALTER COLUMN %s SET NOT NULL" % (p_sch, p_tname, p_colname))
	# 					elif p_diff_item[k]["newvalue"] == "YES":
	# 						p_out_sql_src.append("ALTER TABLE %s.%s ALTER COLUMN %s DROP NOT NULL" % (p_sch, p_tname, p_colname))
							
	# 	else:
			
	# 		print_tablehdr(docomment, p_sch, p_tname, p_out_sql_src, p_out_hdr_flag)
			
	# 		# remover ...
	# 		p_out_sql_src.append(tmpltd % (p_sch, p_tname))
			
	# 		# ... e recriar
	# 		colcreatitems = col_create(p_tname, p_colname, p_diff_item["newvalue"])
	# 		cont0 = re.sub("\s\s+", " ",   "%s %s %s %s" % tuple(colcreatitems[1:]))
	# 		p_out_sql_src.append("%s ADD COLUMN %s" % (tmplt % (p_sch, p_tname), cont0.strip()))

def update_search_path(p_function_body, p_schematrans):

	global_changed = False
	current_body = None

	for trans_dict in p_schematrans:
		# changed = False 
		if current_body is None:
			current_body = p_function_body
		ret = re.sub(f"(search_path[%\('\sa-zA-Z_À-Ýà-ý0-9_\$,]+){trans_dict['src']}", f"\\1{trans_dict['dest']}", current_body)
		if ret != current_body:
			# changed = True
			global_changed = True
			current_body = ret

	return global_changed, ret


def create_function_items(p_schema, p_name, p_args, p_rettype, p_langtype, p_owner, p_volatility, 
	p_body, o_sql_linebuffer, return_table_defstr=None, replace=True):

	if replace:
		cr = "CREATE OR REPLACE FUNCTION %s.%s"
	else:
		cr = "CREATE FUNCTION %s.%s"

	retstr = None
	if p_rettype == "record":
		if not return_table_defstr is None:
			retstr = f"TABLE({return_table_defstr})"
	elif p_rettype == "_text":
		retstr = "text[]"

	if retstr is None:
		retstr = p_rettype
		
	o_sql_linebuffer.append(cr % (p_schema, p_name))	
	o_sql_linebuffer.append("(%s)" % p_args)
	o_sql_linebuffer.append("\n")
	o_sql_linebuffer.append("\tRETURNS %s\n" % retstr)
	o_sql_linebuffer.append("\tLANGUAGE '%s'\n" % p_langtype)
	
	if p_volatility == "s":
		vol = "STABLE"
	elif p_volatility == "v":
		vol = "VOLATILE"
	elif p_volatility == "i":
		vol = "IMMUTABLE"
	else:
		vol = f"??? vol ({p_volatility}) ???"

	o_sql_linebuffer.append("\t%s\n" % vol)
	o_sql_linebuffer.append("AS $BODY$\n")
	o_sql_linebuffer.append(p_body.strip())
	o_sql_linebuffer.append("\n$BODY$;\n\n")
	
	o_sql_linebuffer.append("ALTER FUNCTION %s.%s(%s) OWNER to %s" % (p_schema, p_name, p_args, p_owner))
					
def create_function(p_schema, p_name, p_new_value, o_sql_linebuffer, schematrans=None, replace=True):

	if "return_table" in p_new_value.keys():
		return_table_defstr = p_new_value["return_table"]
	else:
		return_table_defstr = None

	if not schematrans is None:
		_bodychanged, fbody = update_search_path(p_new_value["body"], schematrans)
	else:
		fbody = p_new_value["body"]
	
	create_function_items(p_schema, p_name, p_new_value["args"],
		p_new_value["return_type"], p_new_value["language_type"], 
		p_new_value["procedure_owner"], p_new_value["provolatile"], 
		fbody, o_sql_linebuffer, 
		return_table_defstr=return_table_defstr, replace=replace)

def create_role(p_rolename, p_new_value, o_sql_linebuffer):
	
	cr = "CREATE ROLE %s WITH\n"
		
	o_sql_linebuffer.append(cr % p_rolename)	
	if p_new_value["canlogin"] == "True":
		o_sql_linebuffer.append("\tLOGIN\n")
	else:
		o_sql_linebuffer.append("\tNOLOGIN\n")
	if p_new_value["inherit"] == "True":
		o_sql_linebuffer.append("\tINHERIT\n")
	else:
		o_sql_linebuffer.append("\tNOINHERIT\n")
	if p_new_value["validuntil"] == "None":
		o_sql_linebuffer.append("\tVALID UNTIL 'infinity'")
	else:
		o_sql_linebuffer.append("\tVALID UNTIL '%s'" % p_new_value["validuntil"])
	
def create_sequence(p_schema, p_name, p_new_value, o_sql_linebuffer):

	if len(list(p_new_value.keys())) < 1:
		raise RuntimeError(f"sequence {p_name}, empty definition")
	
	try:

		seqdetails = p_new_value["seqdetails"]
		if "serialcols_dependencies" in p_new_value.keys():
			has_tablecol_dependency = True
		else:
			has_tablecol_dependency = False

		cr = "CREATE SEQUENCE %s.%s\n"
			
		o_sql_linebuffer.append(cr % (p_schema, p_name))	
		o_sql_linebuffer.append("\tINCREMENT %s\n" % seqdetails["increment"])
		o_sql_linebuffer.append("\tSTART %s\n" % seqdetails["start_value"])
		o_sql_linebuffer.append("\tMINVALUE %s\n" % seqdetails["minimum_value"])
		o_sql_linebuffer.append("\tMAXVALUE %s\n" % seqdetails["maximum_value"])
		if seqdetails["cycle_option"] != "NO":
			o_sql_linebuffer.append("\tCYCLE\n")
		o_sql_linebuffer.append("\tCACHE %s;\n" % seqdetails["cache_value"])

		if not has_tablecol_dependency and "current" in seqdetails.keys():
			o_sql_linebuffer.append("\nSELECT setval('%s.%s', %s, true);\n" % (p_schema, p_name, seqdetails["current"]))
			
		o_sql_linebuffer.append("\nALTER SEQUENCE %s.%s OWNER to %s" % (p_schema, p_name, seqdetails["owner"]))

	except:
		logger = logging.getLogger('pgsourcing')
		logger.error(f"create_sequence, p_new_value parameter value: {str(p_new_value)}")
		raise

def create_trigger(p_trname, p_schema, p_tname, p_new_value, o_sql_linebuffer):

	cr = "CREATE TRIGGER %s\n"
	
	o_sql_linebuffer.append(cr % p_trname)
	o_sql_linebuffer.append("\t%s %s\n" % (p_new_value["trigger_activation"], p_new_value["trigger_event"]))
	o_sql_linebuffer.append("\tON %s.%s\n" % (p_schema, p_tname))
	o_sql_linebuffer.append("\tFOR EACH %s\n" % p_new_value["trigger_level"])
	o_sql_linebuffer.append("\tEXECUTE PROCEDURE %s.%s();\n" % (p_new_value["function_schema"], p_new_value["function_name"]))
	
	
def print_tablehdr(p_docomment, p_sch, p_name, p_out_sql_src, o_flag_byref):
	if p_docomment and not o_flag_byref[0]:
		p_out_sql_src.append("\n-- " + "".join(['#'] * 77) + "\n" + "-- Table %s.%s\n" % (p_sch, p_name) + "-- " + "".join(['#'] * 77))
		o_flag_byref[0] = True

def print_viewhdr(p_docomment, p_sch, p_name, p_out_sql_src, o_flag_byref):
	if p_docomment and not o_flag_byref[0]:
		p_out_sql_src.append("\n-- " + "".join(['#'] * 77) + "\n" + "-- View %s.%s\n" % (p_sch, p_name) + "-- " + "".join(['#'] * 77))
		o_flag_byref[0] = True

def print_matviewhdr(p_docomment, p_sch, p_name, p_out_sql_src, o_flag_byref):
	if p_docomment and not o_flag_byref[0]:
		p_out_sql_src.append("\n-- " + "".join(['#'] * 77) + "\n" + "-- Materialized view %s.%s\n" % (p_sch, p_name) + "-- " + "".join(['#'] * 77))
		o_flag_byref[0] = True

def updatedb(p_difdict, p_updates_ids_list, p_limkeys_list, delmode=None, docomment=True):
	
	diff_content = p_difdict["content"]	
	if "transformschema" in p_difdict.keys():
		transformschema = p_difdict["transformschema"]	
	else:
		transformschema = None

	out_sql_src = []
	
	logger = logging.getLogger('pgsourcing')
	
	# delmode: NODEL DEL CASCADE 
	if delmode is None or delmode == "NODEL":
		tmpltd = "-- ALTER TABLE %s.%s"
		tmplpd = "-- DROP FUNCTION %s.%s"
		tmplsd = "-- DROP SEQUENCE %s.%s"
		tmplvd = "-- DROP VIEW %s.%s"
		tmplmvd = "-- DROP MATERIALIZED VIEW %s.%s"
	else:
		tmpltd = "ALTER TABLE %s.%s"
		tmplpd = "DROP FUNCTION %s.%s"
		tmplsd = "DROP SEQUENCE %s.%s"
		tmplvd = "DROP VIEW %s.%s"
		tmplmvd = "DROP MATERIALIZED VIEW %s.%s"
		
	tmplt = "ALTER TABLE %s.%s"

	grpkey = "roles"
	if (len(p_limkeys_list) < 1 or grpkey in p_limkeys_list) and grpkey in diff_content.keys():
		
		header_printed = False
	
		currdiff_block = diff_content[grpkey]			
		for role in sorted(currdiff_block.keys()):
			
			diff_item = currdiff_block[role]
			assert "diffoper" in diff_item.keys()
			assert "insert" in diff_item["diffoper"].keys() or "update" in diff_item["diffoper"].keys() or "delete" in diff_item["diffoper"].keys(), f"not 'inserting', not 'updating' and not 'deleting', diffoper:{diff_item['diffoper']}"

			operorder = diff_item['newvalue']['operorder']
			
			if len(p_updates_ids_list) < 1 or operorder in p_updates_ids_list:	

				if docomment and not header_printed:
					out_sql_src.append("\n-- " + "".join(['#'] * 77) + "\n" + "-- Roles\n" + "-- " + "".join(['#'] * 77))
					header_printed = True
					
				out_sql_src.append("-- Op #%d" % operorder)

				if delmode == "NODEL":
					xtmpl = "-- DROP ROLE %s"
				elif delmode == "DEL":
					xtmpl = "DROP ROLE %s"
				elif delmode == "CASCADE":
					xtmpl = "DROP ROLE %s CASCADE"
				else:
					xtmpl = "??????? %s"

				if diff_item["diffoper"] == "insert":
					flines = []
					create_role(role, diff_item["newvalue"], flines)						
					out_sql_src.append("".join(flines))
				elif diff_item["diffoper"] == "update":
					out_sql_src.append(xtmpl % role)
					flines = []
					create_role(role, diff_item["newvalue"], flines)						
					out_sql_src.append("".join(flines))
				elif diff_item["diffoper"] == "delete":
					out_sql_src.append(xtmpl % role)

	grpkey = "schemata"	
	dropped_schemata = []
	
	if (len(p_limkeys_list) < 1 or grpkey in p_limkeys_list) and grpkey in diff_content.keys():
		
		header_printed = False
	
		currdiff_block = diff_content[grpkey]			
		for sch in sorted(currdiff_block.keys()):
			
			diff_item = currdiff_block[sch]
			if "schdetails" in diff_item.keys():

				di_base = diff_item["schdetails"]
				operorder = di_base['operorder']						
				if len(p_updates_ids_list) < 1 or operorder in p_updates_ids_list:	

					if docomment and not header_printed:
						out_sql_src.append("\n-- " + "".join(['#'] * 77) + "\n" + "-- Schemas\n" + "-- " + "".join(['#'] * 77))
						header_printed = True
					if docomment:
						out_sql_src.append("-- Op #%d" % operorder)
				
					if di_base["diffoper"] in ("delete", "update"):
						if delmode == "NODEL":
							xtmpl = "-- DROP SCHEMA %s"
						elif delmode == "DEL":
							xtmpl = "DROP SCHEMA %s"
						elif delmode == "CASCADE":
							xtmpl = "DROP SCHEMA %s CASCADE"
						else:
							xtmpl = "??????? %s"
						out_sql_src.append(xtmpl % sch)
						dropped_schemata.append(sch)

					if di_base["diffoper"] in ("insert", "update"):
						out_sql_src.append("CREATE SCHEMA %s AUTHORIZATION %s" % (sch, di_base["newvalue"]["auth"]))

						# if "grants" in diff_item["newvalue"].keys():
						# 	for user_name in diff_item["newvalue"]["grants"].keys():
						# 		privs = diff_item["newvalue"]["grants"][user_name]
						# 		out_sql_src.append("GRANT %s ON SCHEMA %s TO %s" % (privs, sch, user_name))
						
			if "grants" in diff_item.keys():
				di_base = diff_item["grants"]
				for user_name in di_base.keys():
					if 'newvalue' in di_base[user_name].keys():
						pre_privs = di_base[user_name]['newvalue']	
					else:
						pre_privs = None
					operorder = di_base[user_name]['operorder']	
					if len(p_updates_ids_list) < 1 or operorder in p_updates_ids_list:
						if docomment and not header_printed:
							out_sql_src.append("\n-- " + "".join(['#'] * 77) + "\n" + "-- Schemas\n" + "-- " + "".join(['#'] * 77))
							header_printed = True
						if docomment:
							try:
								out_sql_src.append("-- Op #%d" % operorder)
							except:
								logger.error("pre_privs:", pre_privs)
								logger.error("di_base:", di_base)
								raise
						if di_base[user_name]['diffoper'] in ("update", "delete"):
							if di_base[user_name]['diffoper'] == "delete":
								assert pre_privs is None, pre_privs
								privs = "ALL"
							else:
								privs = pre_privs	
							if delmode == "NODEL":
								xtmpl = "-- REVOKE %s ON %s FROM %s"
							else:
								xtmpl = "REVOKE %s ON %s FROM %s"
							out_sql_src.append(xtmpl % (privs, sch, user_name))
						if di_base[user_name]['diffoper'] in ("update", "insert"):
							out_sql_src.append("GRANT %s ON SCHEMA %s TO %s" % (pre_privs, sch, user_name))

	grpkey = "procedures"	
	if (len(p_limkeys_list) < 1 or grpkey in p_limkeys_list) and grpkey in diff_content.keys():

		schema_trans = None
		if not transformschema is None:
			if "procedures" in transformschema["types"]:
				schema_trans = transformschema["trans"]

		currdiff_block = diff_content[grpkey]			
		for sch in sorted(currdiff_block.keys()):

			if sch in dropped_schemata:
				logger.warning("CONFLICT: schema to drop '%s' is in use in procedures." % sch)
			
			for procname in sorted(currdiff_block[sch].keys()):

				usable_proc = reverse_proc_fname(procname)

				proc_blk = currdiff_block[sch][procname]				
				if "diffoper" in proc_blk.keys():
					
					if len(p_updates_ids_list) < 1 or proc_blk["operorder"] in p_updates_ids_list:

						if docomment:
							out_sql_src.append("\n-- " + "".join(['#'] * 77) + "\n" + "-- Function %s.%s\n" % (sch, usable_proc) + "-- " + "".join(['#'] * 77))
							out_sql_src.append("-- Op #%d" % proc_blk["operorder"])
					
						if proc_blk["diffoper"] == "insert":
							
							flines = []
							create_function(sch, usable_proc, proc_blk["newvalue"], flines, schematrans=schema_trans, replace=True)						
							out_sql_src.append("".join(flines))
							
						elif proc_blk["diffoper"] == "delete":
							
							if delmode == "CASCADE":
								out_sql_src.append((tmplpd + " CASCADE") % (sch, usable_proc))
							else:
								out_sql_src.append(tmplpd % (sch, usable_proc))
					
						elif proc_blk["diffoper"] == "update":
							
							# se os parametros de entrada ou de saida forem diferentes ....
							do_replace = True
							if "return_type" in proc_blk["changedkeys"].split(", ") or "args" in proc_blk["changedkeys"].split(", "):	
								do_replace = False						
								out_sql_src.append(tmplpd % (sch, usable_proc))
							# e "create function"
							
							flines = []
							create_function(sch, usable_proc, proc_blk["newvalue"], flines, schematrans=schema_trans, replace=do_replace)						
							out_sql_src.append("".join(flines))
							
						else:
							
							raise RuntimeError("function %s.%s, wrong diffoper: %s" % (sch, usable_proc, proc_blk["diffoper"]))
					
	grpkey = "sequences"	
	if (len(p_limkeys_list) < 1 or grpkey in p_limkeys_list) and grpkey in diff_content.keys():
	
		header_printed = False

		currdiff_block = diff_content[grpkey]			
		for sch in sorted(currdiff_block.keys()):
			
			if sch in dropped_schemata:
				logger.warning("CONFLICT: schema to drop '%s' is in use in sequences." % sch)
			
			for sname in sorted(currdiff_block[sch].keys()):

				diff_item = currdiff_block[sch][sname]	
				if "diffoper" in diff_item.keys():				
					if len(p_updates_ids_list) < 1 or diff_item["operorder"] in p_updates_ids_list:	

						if docomment and not header_printed:
							out_sql_src.append("\n-- " + "".join(['#'] * 77) + "\n" + "-- Sequence %s.%s\n" % (sch, sname) + "-- " + "".join(['#'] * 77))
							header_printed = True
						if docomment:
							out_sql_src.append("-- Op #%d" % diff_item["operorder"])
							
						assert "seqdetails" in diff_item["newvalue"].keys()

						if diff_item["diffoper"] in ("update", "delete"):
							out_sql_src.append(tmplsd % (sch, sname))
					
						if diff_item["diffoper"] in ("update", "insert"):
							
							flines = []
							create_sequence(sch, sname, diff_item["newvalue"], flines)						
							out_sql_src.append("".join(flines))

							if "grants" in diff_item["newvalue"].keys():
								for user_name in diff_item["newvalue"]["grants"].keys():
									privs = diff_item["newvalue"]["grants"][user_name]
									out_sql_src.append("GRANT %s ON TABLE %s.%s TO %s" % (privs, sch, sname, user_name))
							
				if "grants" in diff_item.keys():
					for user_name in diff_item["grants"].keys():
						di = diff_item["grants"][user_name]					
						if len(p_updates_ids_list) < 1 or di["operorder"] in p_updates_ids_list:
							if docomment and not header_printed:
								out_sql_src.append("\n-- " + "".join(['#'] * 77) + "\n" + "-- Sequence %s.%s\n" % (sch, sname) + "-- " + "".join(['#'] * 77))
								header_printed = True
							if docomment:
								out_sql_src.append("-- Op #%d" % di["operorder"])
							privs = di["newvalue"]	
							if di["diffoper"] == "delete":
								if delmode == "NODEL":
									xtmpl = "-- REVOKE %s ON %s.%s FROM %s"
								else:
									xtmpl = "REVOKE %s ON %s.%s FROM %s"
								out_sql_src.append(xtmpl % (privs, sch, sname, user_name))
							if di["diffoper"] in ("update", "insert"):
								out_sql_src.append("GRANT %s ON TABLE %s.%s TO %s" % (privs, sch, sname, user_name))
	
	grpkey = "tables"	
	if (len(p_limkeys_list) < 1 or grpkey in p_limkeys_list) and grpkey in diff_content.keys():

		header_printed = [False]
			
		currdiff_block = diff_content[grpkey]			
		for sch in sorted(currdiff_block.keys()):	

			if sch in dropped_schemata:
				logger.warning("CONFLICT: schema to drop '%s' is in use in tables." % sch)
								
			for tname in sorted(currdiff_block[sch].keys()):
				
				header_printed[0] = False
				
				diff_item = currdiff_block[sch][tname]	

				# pp.pprint(diff_item)

				if "diffoper" in diff_item.keys():						
				
					if len(p_updates_ids_list) < 1 or diff_item["operorder"] in p_updates_ids_list:	
						print_tablehdr(docomment, sch, tname, out_sql_src, header_printed)	
						if docomment:				
							out_sql_src.append("-- Op #%d" % diff_item["operorder"])
						table_operation(sch, tname, diff_item, delmode, out_sql_src)
					
				else:

					if "owner" in diff_item.keys():	
						if diff_item["owner"]["diffoper"] == "update":
							if len(p_updates_ids_list) < 1 or diff_item["owner"]["operorder"] in p_updates_ids_list:
								print_tablehdr(docomment, sch, tname, out_sql_src, header_printed)	
								if docomment:
									out_sql_src.append("-- Op #%d" % diff_item["owner"]["operorder"])
								out_sql_src.append("ALTER TABLE %s.%s OWNER to %s" % (sch, tname, diff_item["owner"]["newvalue"]))						
					
					if "cols" in diff_item.keys():							
						for colname in diff_item["cols"].keys():						
							col_operation(docomment, sch, tname, colname, diff_item["cols"][colname], delmode, p_updates_ids_list, out_sql_src, header_printed)

					if "pkey" in diff_item.keys():	
												
						if delmode == "CASCADE":
							xtmpl = SQL_DROP_CONSTR + " CASCADE"
						else:
							xtmpl = SQL_DROP_CONSTR	

						for pkname in diff_item["pkey"].keys():	
							di = diff_item["pkey"][pkname]
							if "diffoper" in di.keys():
								if len(p_updates_ids_list) < 1 or di["operorder"] in p_updates_ids_list:
									print_tablehdr(docomment, sch, tname, out_sql_src, header_printed)		
									if docomment:																						
										out_sql_src.append("-- Op #%d" % di["operorder"])
									if di["diffoper"] in ("insert", "update"):
										if di["diffoper"] == "update":
											out_sql_src.append(xtmpl % (tmpltd % (sch, tname), pkname))
										nv = di["newvalue"]
										out_sql_src.append(SQL_CREATE_PK % (tmplt % (sch, tname), pkname, ",".join(nv["columns"]), nv["index_tablespace"]))	
									elif di["diffoper"] == "delete":
										out_sql_src.append(xtmpl % (tmpltd % (sch, tname), pkname))								

					if "check" in diff_item.keys():	
												
						if delmode == "CASCADE":
							xtmpl = SQL_DROP_CONSTR + " CASCADE"
						else:
							xtmpl = SQL_DROP_CONSTR	

						for cname in diff_item["check"].keys():	
							di = diff_item["check"][cname]
							if "diffoper" in di.keys():
								if len(p_updates_ids_list) < 1 or di["operorder"] in p_updates_ids_list:
									print_tablehdr(docomment, sch, tname, out_sql_src, header_printed)
									if docomment:
										out_sql_src.append("-- Op #%d" % di["operorder"])
									if di["diffoper"] in ("insert", "update"):
										if di["diffoper"] == "update":
											out_sql_src.append(xtmpl % (tmpltd % (sch, tname), cname))
										nv = di["newvalue"]
										out_sql_src.append(SQL_CREATE_CONSTR % (tmplt % (sch, tname), cname, nv["chkdesc"]))								
									elif di["diffoper"] == "delete":
										out_sql_src.append(xtmpl % (tmpltd % (sch, tname), cname))								

					if "fkey" in diff_item.keys():	
												
						if delmode == "CASCADE":
							xtmpl = SQL_DROP_CONSTR + " CASCADE"
						else:
							xtmpl = SQL_DROP_CONSTR	

						for cname in diff_item["fkey"].keys():	
							di = diff_item["fkey"][cname]
							if "diffoper" in di.keys():
								if len(p_updates_ids_list) < 1 or di["operorder"] in p_updates_ids_list:
									print_tablehdr(docomment, sch, tname, out_sql_src, header_printed)
									if docomment:
										out_sql_src.append("-- Op #%d" % di["operorder"])
									if di["diffoper"] in ("insert", "update"):
										if di["diffoper"] == "update":
											out_sql_src.append(xtmpl % (tmpltd % (sch, tname), cname))
										nv = di["newvalue"]
										out_sql_src.append(SQL_CREATE_FK % (tmplt % (sch, tname), cname, nv["cdef"], nv["matchtype"], nv["updtype"], nv["deltype"]))								
									elif di["diffoper"] == "delete":
										out_sql_src.append(xtmpl % (tmpltd % (sch, tname), cname))								

					if "index" in diff_item.keys():	
												
						if delmode is None or delmode == "NODEL":
							xtmpl = "-- DROP INDEX %s.%s"
						elif delmode == "CASCADE":
							xtmpl = "DROP INDEX %s.%s CASCADE"
						else:
							xtmpl = "DROP INDEX %s.%s"

						for cname in diff_item["index"].keys():	
							di = diff_item["index"][cname]
							if "diffoper" in di.keys():
								if len(p_updates_ids_list) < 1 or di["operorder"] in p_updates_ids_list:
									print_tablehdr(docomment, sch, tname, out_sql_src, header_printed)
									if docomment:
										out_sql_src.append("-- Op #%d" % di["operorder"])
									if di["diffoper"] in ("delete", "update"):
										out_sql_src.append(xtmpl % (sch, cname))
									if di["diffoper"] in ("insert", "update"):
										nv = di["newvalue"]
										out_sql_src.append("%s TABLESPACE %s" % (nv["idxdesc"], nv["tablespace"]))						

					if "unique" in diff_item.keys():							

						if delmode == "CASCADE":
							xtmpl = SQL_DROP_CONSTR + " CASCADE"
						else:
							xtmpl = SQL_DROP_CONSTR	

						for pkname in diff_item["unique"].keys():	
							di = diff_item["unique"][pkname]
							if "diffoper" in di.keys():
								if len(p_updates_ids_list) < 1 or di["operorder"] in p_updates_ids_list:
									print_tablehdr(docomment, sch, tname, out_sql_src, header_printed)
									if docomment:
										out_sql_src.append("-- Op #%d" % di["operorder"])
									if di["diffoper"] in ("insert", "update"):
										if di["diffoper"] == "update":
											out_sql_src.append(xtmpl % (tmpltd % (sch, tname), pkname))
										nv = di["newvalue"]
										out_sql_src.append(SQL_CREATE_UNIQUE % (tmplt % (sch, tname), pkname, ",".join(nv["columns"]), nv["index_tablespace"]))									
									elif di["diffoper"] == "delete":
										out_sql_src.append(xtmpl % (tmpltd % (sch, tname), pkname))

					if "trigger" in diff_item.keys():	

						if delmode is None or delmode == "NODEL":
							xtmpl = "-- DROP TRIGGER %s ON %s.%s"
						elif delmode == "CASCADE":
							xtmpl = "DROP TRIGGER %s ON %s.%s CASCADE"
						else:
							xtmpl = "DROP TRIGGER %s ON %s.%s"
						
						for trname in diff_item["trigger"].keys():	
							di = diff_item["trigger"][trname]
							if "diffoper" in di.keys():
								if len(p_updates_ids_list) < 1 or di["operorder"] in p_updates_ids_list:
									print_tablehdr(docomment, sch, tname, out_sql_src, header_printed)
									if docomment:
										out_sql_src.append("-- Op #%d" % di["operorder"])
									if di["diffoper"] in ("insert", "update"):
										if di["diffoper"] == "update":
											out_sql_src.append(xtmpl % (trname, sch, tname))
										nv = di["newvalue"]
										flines = []
										create_trigger(trname, sch, tname, nv, flines)						
										out_sql_src.append("".join(flines))
									elif di["diffoper"] == "delete":
										out_sql_src.append(xtmpl % (trname, sch, tname))

					# Table priv grants
					if "grants" in diff_item.keys():

						this_diff = diff_item["grants"]
						if "newvalue" in this_diff.keys():

							assert this_diff["diffoper"] in ("update", "insert"), this_diff["diffoper"]

							for user_name in this_diff["newvalue"].keys():
								pre_privs = this_diff["newvalue"][user_name]					
								if len(p_updates_ids_list) < 1 or this_diff["operorder"] in p_updates_ids_list:
									print_tablehdr(docomment, sch, tname, out_sql_src, header_printed)
									if docomment:
										out_sql_src.append("-- Op #%d" % this_diff["operorder"])							
									if this_diff["diffoper"] in ("update", "delete"):
										if "newvalue" in di.keys():
											privs = pre_privs
										else:
											privs = "ALL"	
										if delmode == "NODEL":
											xtmpl = "-- REVOKE %s ON %s.%s FROM %s"
										else:
											xtmpl = "REVOKE %s ON %s.%s FROM %s"
										out_sql_src.append(xtmpl % (privs, sch, tname, user_name))
									if this_diff["diffoper"] in ("update", "insert"):
										out_sql_src.append("GRANT %s ON TABLE %s.%s TO %s" % (pre_privs, sch, tname, user_name))

						else:

							assert this_diff["diffoper"] == "delete", this_diff["diffoper"]
							privs = "ALL"
							if delmode == "NODEL":
								xtmpl = "-- REVOKE %s ON %s.%s FROM %s"
							else:
								xtmpl = "REVOKE %s ON %s.%s FROM %s"
							out_sql_src.append(xtmpl % (privs, sch, tname, user_name))

	grpkey = "views"
	if (len(p_limkeys_list) < 1 or grpkey in p_limkeys_list) and grpkey in diff_content.keys():

		header_printed = [False]
			
		currdiff_block = diff_content[grpkey]			
		for sch in sorted(currdiff_block.keys()):	

			if sch in dropped_schemata:
				logger.warning("CONFLICT: schema to drop '%s' is in use in views." % sch)
								
			for vname in sorted(currdiff_block[sch].keys()):
				
				header_printed[0] = False				
				diff_item = currdiff_block[sch][vname]	
				
				if "newvalue" in diff_item.keys():

					assert "vdetails" in diff_item["newvalue"].keys()
					
					di = diff_item["newvalue"]["vdetails"]				
					if len(p_updates_ids_list) < 1 or diff_item["operorder"] in p_updates_ids_list:						
						print_matviewhdr(docomment, sch, vname, out_sql_src, header_printed)	
						if docomment:				
							out_sql_src.append("-- Op #%d" % diff_item["operorder"])
						if diff_item["diffoper"] in ("update", "delete"):
							out_sql_src.append(tmplvd % (sch,vname))
						if diff_item["diffoper"] in ("update", "insert"):
							tmplv = "CREATE VIEW %s.%s AS %s" 
							out_sql_src.append(tmplv % (sch,vname, di["vdef"]))
								
							if "vowner" in di.keys():
								out_sql_src.append("ALTER TABLE %s.%s OWNER to %s" % (sch, vname, di["vowner"]))
							
							if "grants" in diff_item["newvalue"].keys():
								for user_name in diff_item["newvalue"]["grants"].keys():
									privs = diff_item["newvalue"]["grants"][user_name]
									out_sql_src.append("GRANT %s ON TABLE %s.%s TO %s" % (privs, sch, vname, user_name))
							
				if "grants" in diff_item.keys():
					for user_name in diff_item["grants"].keys():
						di = diff_item["grants"][user_name]					
						if len(p_updates_ids_list) < 1 or di["operorder"] in p_updates_ids_list:
							print_matviewhdr(docomment, sch, vname, out_sql_src, header_printed)
							if docomment:
								out_sql_src.append("-- Op #%d" % di["operorder"])
							privs = di["newvalue"]	
							if di["diffoper"] in ("update", "delete"):
								if delmode == "NODEL":
									xtmpl = "-- REVOKE %s ON %s.%s FROM %s"
								else:
									xtmpl = "REVOKE %s ON %s.%s FROM %s"
								out_sql_src.append(xtmpl % (privs, sch, vname, user_name))
							if di["diffoper"] in ("update", "insert"):
								out_sql_src.append("GRANT %s ON TABLE %s.%s TO %s" % (privs, sch, vname, user_name))

	grpkey = "matviews"
	if (len(p_limkeys_list) < 1 or grpkey in p_limkeys_list) and grpkey in diff_content.keys():

		header_printed = [False]
			
		currdiff_block = diff_content[grpkey]			
		for sch in sorted(currdiff_block.keys()):	

			if sch in dropped_schemata:
				logger.warning("CONFLICT: schema to drop '%s' is in use in matviews." % sch)
								
			for vname in sorted(currdiff_block[sch].keys()):
				
				header_printed[0] = False				
				diff_item = currdiff_block[sch][vname]	
				
				if "newvalue" in diff_item.keys():

					assert "mvdetails" in diff_item["newvalue"].keys()
					
					di = diff_item["newvalue"]["mvdetails"]				
					if len(p_updates_ids_list) < 1 or diff_item["operorder"] in p_updates_ids_list:						
						print_matviewhdr(docomment, sch, vname, out_sql_src, header_printed)	
						if docomment:				
							out_sql_src.append("-- Op #%d" % diff_item["operorder"])
						if diff_item["diffoper"] in ("update", "delete"):
							out_sql_src.append(tmplmvd % (sch,vname))
						if diff_item["diffoper"] in ("update", "insert"):
							tmplv = "CREATE MATERIALIZED VIEW %s.%s TABLESPACE %s AS %s" 
							out_sql_src.append(tmplv % (sch,vname, di["vtablespace"], di["vdef"]))
								
							if "vowner" in di.keys():
								out_sql_src.append("ALTER TABLE %s.%s OWNER to %s" % (sch, vname, di["vowner"]))
							
							if "grants" in diff_item["newvalue"].keys():
								for user_name in diff_item["newvalue"]["grants"].keys():
									privs = diff_item["newvalue"]["grants"][user_name]
									out_sql_src.append("GRANT %s ON TABLE %s.%s TO %s" % (privs, sch, vname, user_name))
							
				if "grants" in diff_item.keys():
					for user_name in diff_item["grants"].keys():
						di = diff_item["grants"][user_name]					
						if len(p_updates_ids_list) < 1 or di["operorder"] in p_updates_ids_list:
							print_matviewhdr(docomment, sch, vname, out_sql_src, header_printed)
							if docomment:
								out_sql_src.append("-- Op #%d" % di["operorder"])
							privs = di["newvalue"]	
							if di["diffoper"] in ("update", "delete"):
								if delmode == "NODEL":
									xtmpl = "-- REVOKE %s ON %s.%s FROM %s"
								else:
									xtmpl = "REVOKE %s ON %s.%s FROM %s"
								out_sql_src.append(xtmpl % (privs, sch, vname, user_name))
							if di["diffoper"] in ("update", "insert"):
								out_sql_src.append("GRANT %s ON TABLE %s.%s TO %s" % (privs, sch, vname, user_name))

						
	return out_sql_src
						
				
				
			
	
