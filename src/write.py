
import re

from src.fileandpath import load_currentref
from src.common import CFG_GROUPS, CFG_LISTGROUPS, COL_ITEMS_CHG_AVOIDING_SUBSTITUTION

SQL_CREATE_PK = """%s ADD CONSTRAINT %s PRIMARY KEY (%s)
    USING INDEX TABLESPACE %s"""
    
SQL_CREATE_UNIQUE = """%s ADD CONSTRAINT %s UNIQUE (%s)
    USING INDEX TABLESPACE %s"""

SQL_CREATE_CONSTR = "%s ADD CONSTRAINT %s %s"

SQL_DROP_CONSTR = "%s DROP CONSTRAINT %s"


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
		raise RuntimeError, "changegrp: excessiva recursao, seq de chaves: %s" % str(p_keys_byref)

	if isinstance(p_currdiff_block, dict):
		
		for k in p_currdiff_block.keys():	
			
			if k == p_keys_byref[-1]:
				raise RuntimeError, "changegrp: erro recursao, seq de chaves identicas: %s + %s" % (str(p_keys_byref), k)
			diff_item = p_currdiff_block[k]
			if isinstance(diff_item, list):
				assert isinstance(p_chg_group, list), "Incoerencia: list diff para alterar dicionario: '%s'" % str(diff_item)
				changed = changed | changegrp_list(p_chg_group, diff_item, p_updates_ids_list)
			else:
				if "diffoper" in diff_item.keys():
					if len(p_updates_ids_list) < 1 or diff_item["operorder"] in p_updates_ids_list:
						#print(k)
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
		
	if "default" in p_col.keys():
		colitems.append("DEFAULT %s" % p_col["default"])
	else:
		colitems.append("")
	
	return colitems

def table_operation(p_sch, p_tname, p_diff_item, p_delmode, p_out_sql_src):	

	if p_diff_item["diffoper"] == "delete":
		
		if p_delmode is None or p_delmode == "NODEL":
			tmplt = "-- DROP TABLE %s.%s"
		elif p_delmode == "DEL":
			tmplt = "DROP TABLE %s.%s"
		elif p_delmode == "CASCADE":
			tmplt = "DROP TABLE %s.%s CASCADE"
		
		p_out_sql_src.append(tmplt % (p_sch,p_tname))

	elif p_diff_item["diffoper"] == "insert":
		
		if not "cols" in p_diff_item["newvalue"]:
			tmplist = ["CREATE TABLE %s.%s" % (p_sch,p_tname)]
		else:
			tmplist = ["CREATE TABLE %s.%s" % (p_sch,p_tname), "("]
		
		if "cols" in p_diff_item["newvalue"]:
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
		
		if "owner" in p_diff_item["newvalue"]:
			p_out_sql_src.append("ALTER TABLE %s.%s OWNER to %s" % (p_sch, p_tname, p_diff_item["newvalue"]["owner"]))

def col_operation(docomment, p_sch, p_tname, p_colname, p_diff_item, p_delmode, p_updates_ids_list, p_out_sql_src, p_out_hdr_flag):
	
	if p_delmode is None or p_delmode == "NODEL":
		tmpltd = "-- ALTER TABLE %s.%s"
	else:
		tmpltd = "ALTER TABLE %s.%s"
		
	tmplt = "ALTER TABLE %s.%s"

	if p_delmode == "CASCADE":
		tmplt2 = "%s DROP COLUMN %s CASCADE"
	else:
		tmplt2 = "%s DROP COLUMN %s"
	
	if "diffoper" in p_diff_item.keys():

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

				p_out_sql_src.append(tmplt2 % (tmpltd % (p_sch, p_tname), p_colname))
				
				colcreatitems = col_create(p_tname, p_colname, p_diff_item["newvalue"])
				cont0 = re.sub("\s\s+", " ",   "\t%s %s %s %s" % tuple(colcreatitems[1:]))
				p_out_sql_src.append("%s ADD COLUMN %s" % (tmplt % (p_sch, p_tname), cont0.strip()))
				
			## TODO - implementar o reconhecimento que uma coluna foi renomeada em SRC
			# if p_diff_item["diffoper"] == "update":			
				# p_out_sql_src.append("%s RENAME COLUMN %s TO %s" % (tmpltd % (p_sch, p_tname), p_colname, p_diff_item["newvalue"]))

	else:
		
		p_substitution = False
		for k in p_diff_item.keys():
			if k not in COL_ITEMS_CHG_AVOIDING_SUBSTITUTION:
				p_substitution = True
				break

		if not p_substitution:
			
			for k in p_diff_item.keys():
				
				if not "operorder" in p_diff_item[k].keys():
					continue
				
				if len(p_updates_ids_list) > 0 and not p_diff_item[k]["operorder"] in p_updates_ids_list:	
					continue
					
				print_tablehdr(docomment, p_sch, p_tname, p_out_sql_src, p_out_hdr_flag)	
				if docomment:			
					p_out_sql_src.append("-- Op #%d" % p_diff_item[k]["operorder"])	
				
				if k == "default":				
					if p_diff_item[k]["diffoper"] == "delete":					
						p_out_sql_src.append("ALTER TABLE %s.%s ALTER COLUMN %s DROP DEFAULT" % (p_sch, p_tname, p_colname))
					elif p_diff_item[k]["diffoper"] in ("insert", "update"):	
						newval = p_diff_item[k]["newvalue"]
						if isinstance(newval, str):
							tmpl = "ALTER TABLE %s.%s ALTER COLUMN %s SET DEFAULT '%s'"
						else:
							tmpl = "ALTER TABLE %s.%s ALTER COLUMN %s SET DEFAULT %s"
						p_out_sql_src.append(tmpl % (p_sch, p_tname, p_colname, newval ))
				elif k == "nullable":
					if p_diff_item[k]["diffoper"] == "update":
						if p_diff_item[k]["newvalue"] == "NO":
							p_out_sql_src.append("ALTER TABLE %s.%s ALTER COLUMN %s SET NOT NULL" % (p_sch, p_tname, p_colname))
						elif p_diff_item[k]["newvalue"] == "YES":
							p_out_sql_src.append("ALTER TABLE %s.%s ALTER COLUMN %s DROP NOT NULL" % (p_sch, p_tname, p_colname))
							
		else:
			
			print_tablehdr(docomment, p_sch, p_tname, p_out_sql_src, p_out_hdr_flag)
			
			# remover ...
			p_out_sql_src.append(tmpltd % (p_sch, p_tname))
			
			# ... e recriar
			colcreatitems = col_create(p_tname, p_colname, p_diff_item["newvalue"])
			cont0 = re.sub("\s\s+", " ",   "%s %s %s %s" % tuple(colcreatitems[1:]))
			p_out_sql_src.append("%s ADD COLUMN %s" % (tmplt % (p_sch, p_tname), cont0.strip()))
					

def create_function(p_schema, p_name, p_new_value, o_sql_linebuffer, replace=False):
	
	if replace:
		cr = "CREATE OR REPLACE FUNCTION %s.%s"
	else:
		cr = "CREATE FUNCTION %s.%s"
		
	o_sql_linebuffer.append(cr % (p_schema, p_name))	
	o_sql_linebuffer.append("(%s)" % p_new_value["args"])
	o_sql_linebuffer.append("\n")
	o_sql_linebuffer.append("RETURNS %s\n" % p_new_value["return_type"])
	o_sql_linebuffer.append("LANGUAGE %s\n" % p_new_value["language_type"])
	
	if p_new_value["provolatile"] == "s":
		vol = "STABLE"
	elif p_new_value["provolatile"] == "v":
		vol = "VOLATILE"
	elif p_new_value["provolatile"] == "i":
		vol = "IMMUTABLE"
	else:
		vol = "???????"

	o_sql_linebuffer.append("%s\n" % vol)
	o_sql_linebuffer.append("AS $BODY$\n")
	o_sql_linebuffer.append(p_new_value["body"].strip())
	o_sql_linebuffer.append("\n$BODY$;\n\n")
	
	o_sql_linebuffer.append("ALTER FUNCTION %s.%s OWNER to %s" % (p_schema, p_name, p_new_value["procedure_owner"]))

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

	cr = "CREATE SEQUENCE %s.%s\n"
		
	o_sql_linebuffer.append(cr % (p_schema, p_name))	
	o_sql_linebuffer.append("\tINCREMENT %s\n" % p_new_value["increment"])
	o_sql_linebuffer.append("\tSTART %s\n" % p_new_value["start_value"])
	o_sql_linebuffer.append("\tMINVALUE %s\n" % p_new_value["minimum_value"])
	o_sql_linebuffer.append("\tMAXVALUE %s\n" % p_new_value["maximum_value"])
	if p_new_value["cycle_option"] != "NO":
		o_sql_linebuffer.append("\tCYCLE\n")
	o_sql_linebuffer.append("\tCACHE %s;\n" % p_new_value["cache_value"])

	if "current" in p_new_value.keys():
		o_sql_linebuffer.append("\nSELECT setval('%s.%s', %s, true);\n" % (p_schema, p_name, p_new_value["current"]))
		
	o_sql_linebuffer.append("\nALTER SEQUENCE %s.%s OWNER to %s" % (p_schema, p_name, p_new_value["owner"]))

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
	
def updatedb(p_proj, p_difdict, p_updates_ids_list, limkeys_list, delmode=None, docomment=True):
	
	diff_content = p_difdict["content"]	
	out_sql_src = []
	
	# delmode: NODEL DEL CASCADE 
	if delmode is None or delmode == "NODEL":
		tmpltd = "-- ALTER TABLE %s.%s"
		tmplpd = "-- DROP FUNCTION %s.%s"
		tmplsd = "-- DROP SEQUENCE %s.%s"
	else:
		tmpltd = "ALTER TABLE %s.%s"
		tmplpd = "DROP FUNCTION %s.%s"
		tmplsd = "DROP SEQUENCE %s.%s"
		
	tmplt = "ALTER TABLE %s.%s"

	grpkey = "roles"

	if grpkey in diff_content.keys():	
		
		header_printed = False
	
		currdiff_block = diff_content[grpkey]			
		for role in currdiff_block.keys():
			
			diff_item = currdiff_block[role]
			assert "diffoper" in diff_item.keys()
			
			if len(p_updates_ids_list) < 1 or diff_item["operorder"] in p_updates_ids_list:	

				if docomment and not header_printed:
					out_sql_src.append("\n-- " + "".join(['#'] * 77) + "\n" + "-- Roles\n" + "-- " + "".join(['#'] * 77))
					header_printed = True
					
				out_sql_src.append("-- Op #%d" % diff_item["operorder"])

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

	grpkey = "schemas"
	
	dropped_schemas = []
	
	if grpkey in diff_content.keys():	
		
		header_printed = False
	
		currdiff_block = diff_content[grpkey]			
		for sch in currdiff_block.keys():
			
			diff_item = currdiff_block[sch]
			assert "diffoper" in diff_item.keys()
			
			if len(p_updates_ids_list) < 1 or diff_item["operorder"] in p_updates_ids_list:	

				if docomment and not header_printed:
					out_sql_src.append("\n-- " + "".join(['#'] * 77) + "\n" + "-- Schemas\n" + "-- " + "".join(['#'] * 77))
					header_printed = True

				out_sql_src.append("-- Op #%d" % diff_item["operorder"])
				
				if diff_item["diffoper"] == "insert":
					out_sql_src.append("CREATE SCHEMA %s AUTHORIZATION %s" % (sch, diff_item["newvalue"]["auth"]))
				elif diff_item["diffoper"] == "delete":
					if delmode == "NODEL":
						xtmpl = "-- DROP SCHEMA %s"
					elif delmode == "DEL":
						xtmpl = "DROP SCHEMA %s"
					elif delmode == "CASCADE":
						xtmpl = "DROP SCHEMA %s CASCADE"
					else:
						xtmpl = "??????? %s"
					out_sql_src.append(xtmpl % sch)
					dropped_schemas.append(sch)
	
	grpkey = "sequences"
	
	if grpkey in diff_content.keys():	
	
		header_printed = False

		currdiff_block = diff_content[grpkey]			
		for sch in currdiff_block.keys():
			
			if sch in dropped_schemas:
				raise RuntimeError, "CONFLICT: schema to drop '%s' is in use in sequences." % sch
			
			for sname in currdiff_block[sch].keys():

				diff_item = currdiff_block[sch][sname]	
				assert "diffoper" in diff_item.keys()
				
				if len(p_updates_ids_list) < 1 or diff_item["operorder"] in p_updates_ids_list:	

					if docomment and not header_printed:
						out_sql_src.append("\n-- " + "".join(['#'] * 77) + "\n" + "-- Sequence %s.%s\n" % (sch, sname) + "-- " + "".join(['#'] * 77))
						header_printed = True
					
					out_sql_src.append("-- Op #%d" % diff_item["operorder"])
				
					if diff_item["diffoper"] == "insert":
						flines = []
						create_sequence(sch, sname, diff_item["newvalue"], flines)						
						out_sql_src.append("".join(flines))

					elif diff_item["diffoper"] == "update":
						out_sql_src.append(tmplsd % (sch, sname))
						flines = []
						create_sequence(sch, sname, diff_item["newvalue"], flines)						
						out_sql_src.append("".join(flines))

					elif diff_item["diffoper"] == "delete":
						out_sql_src.append(tmplsd % (sch, sname))
	
	grpkey = "tables"
	
	if grpkey in diff_content.keys():	

		header_printed = [False]
			
		currdiff_block = diff_content[grpkey]			
		for sch in currdiff_block.keys():	

			if sch in dropped_schemas:
				raise RuntimeError, "CONFLICT: schema to drop '%s' is in use in tables." % sch
								
			for tname in currdiff_block[sch].keys():
				
				header_printed[0] = False
				
				diff_item = currdiff_block[sch][tname]	


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
												
						if delmode is None or delmode == "NODEL":
							xtmpl = SQL_DROP_CONSTR + " CASCADE"
						else:
							xtmpl = SQL_DROP_CONSTR	

						for cname in diff_item["check"].keys():	
							di = diff_item["check"][cname]
							if "diffoper" in di.keys():
								if len(p_updates_ids_list) < 1 or di["operorder"] in p_updates_ids_list:
									if di["diffoper"] in ("insert", "update"):
										print_tablehdr(docomment, sch, tname, out_sql_src, header_printed)
										if docomment:
											out_sql_src.append("-- Op #%d" % di["operorder"])
										if di["diffoper"] == "update":
											out_sql_src.append(xtmpl % (tmpltd % (sch, tname), cname))
										nv = di["newvalue"]
										out_sql_src.append(SQL_CREATE_CONSTR % (tmplt % (sch, tname), cname, nv["chkdesc"]))								
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
									if di["diffoper"] in ("insert", "update"):
										print_tablehdr(docomment, sch, tname, out_sql_src, header_printed)
										if docomment:
											out_sql_src.append("-- Op #%d" % di["operorder"])
										if di["diffoper"] == "update":
											out_sql_src.append(xtmpl % (sch, cname))
										nv = di["newvalue"]
										out_sql_src.append(nv["idxdesc"])						
									elif di["diffoper"] == "delete":
										out_sql_src.append(xtmpl % (sch, cname))

					if "unique" in diff_item.keys():							

						if delmode is None or delmode == "NODEL":
							xtmpl = SQL_DROP_CONSTR + " CASCADE"
						else:
							xtmpl = SQL_DROP_CONSTR	

						for pkname in diff_item["unique"].keys():	
							di = diff_item["unique"][pkname]
							if "diffoper" in di.keys():
								if len(p_updates_ids_list) < 1 or di["operorder"] in p_updates_ids_list:
									if di["diffoper"] in ("insert", "update"):
										print_tablehdr(docomment, sch, tname, out_sql_src, header_printed)
										if docomment:
											out_sql_src.append("-- Op #%d" % di["operorder"])
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
									if di["diffoper"] in ("insert", "update"):
										print_tablehdr(docomment, sch, tname, out_sql_src, header_printed)
										if docomment:
											out_sql_src.append("-- Op #%d" % di["operorder"])
										if di["diffoper"] == "update":
											out_sql_src.append(xtmpl % (trname, sch, tname))
										nv = di["newvalue"]
										flines = []
										create_trigger(trname, sch, tname, nv, flines)						
										out_sql_src.append("".join(flines))
									elif di["diffoper"] == "delete":
										out_sql_src.append(xtmpl % (trname, sch, tname))
												
						
	grpkey = "procedures"
	
	if grpkey in diff_content.keys():	

		header_printed = False
			
		currdiff_block = diff_content[grpkey]			
		for sch in currdiff_block.keys():

			if sch in dropped_schemas:
				raise RuntimeError, "CONFLICT: schema to drop '%s' is in use in procedures." % sch
			
			for procname in currdiff_block[sch].keys():
				
				proc_blk = currdiff_block[sch][procname]				
				if "diffoper" in proc_blk.keys():
					
					if len(p_updates_ids_list) < 1 or proc_blk["operorder"] in p_updates_ids_list:
						
						if docomment:
							out_sql_src.append("\n-- " + "".join(['#'] * 77) + "\n" + "-- Function %s.%s\n" % (sch, procname) + "-- " + "".join(['#'] * 77))
							out_sql_src.append("-- Op #%d" % proc_blk["operorder"])
					
						if proc_blk["diffoper"] == "insert":
							
							flines = []
							create_function(sch, procname, proc_blk["newvalue"], flines, replace=False)						
							out_sql_src.append("".join(flines))
							
						elif proc_blk["diffoper"] == "delete":
							
							if delmode == "CASCADE":
								out_sql_src.append((tmplpd + " CASCADE") % (sch, procname))
							else:
								out_sql_src.append(tmplpd % (sch, procname))
					
						elif proc_blk["diffoper"] == "update":
							
							# se os parametros de entrada ou de saida forem diferentes ....
							do_replace = True
							if "return_type" in proc_blk["changedkeys"] or "args" in proc_blk["changedkeys"]:	
								do_replace = False						
								out_sql_src.append(tmplpd % (sch, procname))
							# e "create function"
							
							flines = []
							create_function(sch, procname, proc_blk["newvalue"], flines, replace=do_replace)						
							out_sql_src.append("".join(flines))
							
						else:
							
							raise RuntimeError, "function %s.%s, wrong diffoper: %s" % (sch, procname, proc_blk["diffoper"])
					
						
	return out_sql_src
						
				
				
			
	
