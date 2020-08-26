
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

def col_operation(p_sch, p_tname, p_colname, p_diff_item, p_delmode, p_updates_ids_list, p_out_sql_src):
	
	if p_delmode is None or p_delmode == "NODEL":
		tmpltd = "-- ALTER TABLE %s.%s"
	else:
		tmpltd = "ALTER TABLE %s.%s"
		
	tmplt = "ALTER TABLE %s.%s"
		
	if "diffoper" in p_diff_item.keys():
		
		if len(p_updates_ids_list) < 1 or p_diff_item["operorder"] in p_updates_ids_list:
		
			if p_diff_item["diffoper"] == "delete":
				
				if p_delmode == "CASCADE":
					tmplt2 = "%s DROP COLUMN %s CASCADE"
				else:
					tmplt2 = "%s DROP COLUMN %s"
				p_out_sql_src.append(tmplt2 % (tmpltd % (p_sch, p_tname), p_colname))
				
			elif p_diff_item["diffoper"] == "insert":
				
				colcreatitems = col_create(p_tname, p_colname, p_diff_item["newvalue"])
				cont0 = re.sub("\s\s+", " ",   "\t%s %s %s %s" % tuple(colcreatitems[1:]))
				p_out_sql_src.append("%s ADD COLUMN %s" % (tmplt % (p_sch, p_tname), cont0.strip()))

			if p_diff_item["diffoper"] == "update":			
				## TODO - implementar o reconhecimento que uma coluna foi renomeada em SRC
				p_out_sql_src.append("%s RENAME COLUMN %s TO %s" % (tmpltd % (p_sch, p_tname), p_colname, p_diff_item["newvalue"]))

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
			
			# remover ...
			p_out_sql_src.append(tmpltd % (p_sch, p_tname))
			
			# ... e recriar
			colcreatitems = col_create(p_tname, p_colname, p_diff_item["newvalue"])
			cont0 = re.sub("\s\s+", " ",   "%s %s %s %s" % tuple(colcreatitems[1:]))
			p_out_sql_src.append("%s ADD COLUMN %s" % (tmplt % (p_sch, p_tname), cont0.strip()))
					
					
			
def updatedb(p_proj, p_difdict, p_updates_ids_list, limkeys_list, delmode=None):
	
	diff_content = p_difdict["content"]	
	out_sql_src = []
	
	# delmode: NODEL DEL CASCADE 
	if delmode is None or delmode == "NODEL":
		tmpltd = "-- ALTER TABLE %s.%s"
	else:
		tmpltd = "ALTER TABLE %s.%s"
		
	tmplt = "ALTER TABLE %s.%s"
	
	grpkey = "tables"
	
	if grpkey in diff_content.keys():	
			
		currdiff_block = diff_content[grpkey]			
		for sch in currdiff_block.keys():	
					
			for tname in currdiff_block[sch].keys():
				
				diff_item = currdiff_block[sch][tname]	
							
				if "diffoper" in diff_item.keys():						
					
					if len(p_updates_ids_list) < 1 or diff_item["operorder"] in p_updates_ids_list:						
						table_operation(sch, tname, diff_item, delmode, out_sql_src)
					
				else:
					
					if "cols" in diff_item.keys():							
						for colname in diff_item["cols"].keys():						
							col_operation(sch, tname, colname, diff_item["cols"][colname], delmode, p_updates_ids_list, out_sql_src)

					if "pkey" in diff_item.keys():							
						if "diffoper" in diff_item["pkey"].keys():	
							if len(p_updates_ids_list) < 1 or diff_item["pkey"]["operorder"] in p_updates_ids_list:						
								if diff_item["pkey"]["diffoper"] == "insert":
									for cnstrname in diff_item["pkey"]["newvalue"].keys():
										nv = diff_item["pkey"]["newvalue"][cnstrname]
										out_sql_src.append(SQL_CREATE_PK % (tmplt % (sch, tname), cnstrname, ",".join(nv["columns"]), nv["index_tablespace"]))	
						else:
							for pkname in diff_item["pkey"].keys():	
								di = diff_item["pkey"][pkname]
								if "diffoper" in di.keys():
									if di["diffoper"] in ("insert", "update"):
										if di["diffoper"] == "update":
											out_sql_src.append(SQL_DROP_CONSTR % (tmpltd % (sch, tname), pkname))
										nv = di["newvalue"]
										out_sql_src.append(SQL_CREATE_PK % (tmplt % (sch, tname), pkname, ",".join(nv["columns"]), nv["index_tablespace"]))									

					if "check" in diff_item.keys():	
						if "diffoper" in diff_item["check"].keys():	
							if len(p_updates_ids_list) < 1 or diff_item["check"]["operorder"] in p_updates_ids_list:						
								if diff_item["check"]["diffoper"] == "insert":
									for cnstrname in diff_item["check"]["newvalue"].keys():
										nv = diff_item["check"]["newvalue"][cnstrname]
										out_sql_src.append(SQL_CREATE_CONSTR % (tmplt % (sch, tname), cnstrname, nv["chkdesc"]))	
						else:
							for cname in diff_item["check"].keys():	
								di = diff_item["check"][cname]
								if "diffoper" in di.keys():
									if di["diffoper"] in ("insert", "update"):
										if di["diffoper"] == "update":
											out_sql_src.append(SQL_DROP_CONSTR % (tmpltd % (sch, tname), cname))
										nv = di["newvalue"]
										out_sql_src.append(SQL_CREATE_CONSTR % (tmplt % (sch, tname), cname, nv["chkdesc"]))								

					if "index" in diff_item.keys():	
						if "diffoper" in diff_item["index"].keys():	
							if len(p_updates_ids_list) < 1 or diff_item["index"]["operorder"] in p_updates_ids_list:						
								if diff_item["index"]["diffoper"] == "insert":
									for cnstrname in diff_item["index"]["newvalue"].keys():
										nv = diff_item["index"]["newvalue"][cnstrname]
										out_sql_src.append(nv["idxdesc"])
						else:
							for cname in diff_item["index"].keys():	
								di = diff_item["index"][cname]
								if "diffoper" in di.keys():
									if di["diffoper"] in ("insert", "update"):
										if di["diffoper"] == "update":
											if delmode is None or delmode == "NODEL":
												xtmpl = "-- DROP INDEX %s.%s"
											else:
												xtmpl = "DROP INDEX %s.%s"
											out_sql_src.append(xtmpl % (sch, cname))
										nv = di["newvalue"]
										out_sql_src.append(nv["idxdesc"])						

					if "unique" in diff_item.keys():							
						if "diffoper" in diff_item["unique"].keys():	
							if len(p_updates_ids_list) < 1 or diff_item["unique"]["operorder"] in p_updates_ids_list:						
								if diff_item["unique"]["diffoper"] == "insert":
									for cnstrname in diff_item["unique"]["newvalue"].keys():
										nv = diff_item["unique"]["newvalue"][cnstrname]
										out_sql_src.append(SQL_CREATE_UNIQUE % (tmplt % (sch, tname), cnstrname, ",".join(nv["columns"]), nv["index_tablespace"]))	
						else:
							for pkname in diff_item["unique"].keys():	
								di = diff_item["unique"][pkname]
								if "diffoper" in di.keys():
									if di["diffoper"] in ("insert", "update"):
										if di["diffoper"] == "update":
											out_sql_src.append(SQL_DROP_CONSTR % (tmpltd % (sch, tname), pkname))
										nv = di["newvalue"]
										out_sql_src.append(SQL_CREATE_UNIQUE % (tmplt % (sch, tname), pkname, ",".join(nv["columns"]), nv["index_tablespace"]))									
						
						
									
								
						
	return out_sql_src
						
				
				
			
	
