
from src.fileandpath import load_currentref
from src.common import CFG_GROUPS, CFG_LISTGROUPS

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
								# raise RuntimeError, "stop"
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
			
			
			
	
