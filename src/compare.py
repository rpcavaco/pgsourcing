

import json
import re
import logging

from os.path import exists
from copy import copy, deepcopy
from difflib import unified_diff as dodiff

from src.common import PROC_SRC_BODY_FNAME, CFG_GROUPS, CFG_DEST_GROUPS, CFG_LISTGROUPS, UPPERLEVELOPS, CFG_SHALLOW_GROUPS, SHALLOW_DEPTH
from src.fileandpath import load_currentref

def do_transformschema(p_transformschema, p_obj, p_k):
	if p_k == "idxdesc":
		if "tables" in p_transformschema["types"] or "indexes" in p_transformschema["types"]:
			for trans in p_transformschema["trans"]:
				p_obj[p_k] = p_obj[p_k].replace(trans["src"], trans["dest"])
	elif p_k == "function_schema":
		if "procedures" in p_transformschema["types"] or "triggers" in p_transformschema["types"]:
			for trans in p_transformschema["trans"]:
				p_obj[p_k] = p_obj[p_k].replace(trans["src"], trans["dest"])
	elif p_k == "index_tablespace":
		if "tables" in p_transformschema["types"] or "indexes" in p_transformschema["types"]:
			for trans in p_transformschema["trans"]:
				p_obj[p_k] = p_obj[p_k].replace(trans["src"], trans["dest"])
	elif p_k == "chkdesc":
		if "tables" in p_transformschema["types"] or "indexes" in p_transformschema["types"]:
			for trans in p_transformschema["trans"]:
				p_obj[p_k] = p_obj[p_k].replace(trans["src"], trans["dest"])
	elif p_k == "default":
		if "tables" in p_transformschema["types"]:
			for trans in p_transformschema["trans"]:
				p_obj[p_k] = p_obj[p_k].replace(trans["src"], trans["dest"])
					
def traverse_replaceval(p_transformschema, p_obj, p_mode):

	if not p_transformschema:
		return
		
	if not isinstance(p_obj, dict):
		return
		
	for k in p_obj.keys():	
		
		do_transformschema(p_transformschema, p_obj, k)	

		if isinstance(p_obj[k], dict):
			traverse_replaceval(p_transformschema, p_obj[k], p_mode)		
	
def get_diff_item(p_fase, p_diff_dict, p_grpkeys, b_leaf_is_list=False):
	
	diff_dict = p_diff_dict
	for ki, k in enumerate(p_grpkeys):
		if not k in diff_dict.keys():
			if b_leaf_is_list and ki == len(p_grpkeys) - 1:
				diff_dict[k] = []
			else:
				diff_dict[k] = {}
		diff_dict = diff_dict[k]
		
	return diff_dict		

def sourcediff(p_srca, p_srcb, p_transformschema, out_dellist): #, out_addlist):
	
	del out_dellist[:]

	if "unicode" in str(type(p_srca)):
		srca = p_srca
	else:
		srca = p_srca.decode('utf-8')

	if "unicode" in str(type(p_srcb)):
		srcb = p_srcb
	else:
		srcb = p_srcb.decode('utf-8')
		
	if p_transformschema:
		if "procedures" in p_transformschema["types"]:
			for trans in p_transformschema["trans"]:
				srca = srca.replace(trans["src"], trans["dest"])
		
	rawlistA= srca.splitlines(True)
	rawlistB = srcb.splitlines(True)
	
	patt = r"^--(.*)$"
	substitute = r"/* \1 */"
	#listA = [re.sub(' +', ' ', ln.strip()).lower() for ln in rawlistA if len(ln.strip()) > 0]
	#listB = [re.sub(' +', ' ', ln.strip()).lower() for ln in rawlistB if len(ln.strip()) > 0]
	#listA = [ln.strip().lower() for ln in rawlistA if len(ln.strip()) > 0]
	#listB = [ln.strip().lower() for ln in rawlistB if len(ln.strip()) > 0]
	# startswith
	listA = [re.sub(patt, substitute, ln.strip()).lower() for ln in rawlistA if len(ln.strip()) > 0]
	listB = [re.sub(patt, substitute, ln.strip()).lower() for ln in rawlistB if len(ln.strip()) > 0]
	
	diff = [l.strip() for l in list(dodiff(listA, listB)) if l.strip()]
	
	out_dellist.extend(diff)
	
	return srca

def keychains(p_dict, o_resultlist, prev=None):
	
	if not isinstance(p_dict, dict):
		return None
		
	if prev is None:
		curr = []
	else:
		curr = prev
			
	for k in p_dict.keys():
		tst = curr + [k]
		o_resultlist.append(copy(tst))
		keychains(p_dict[k], o_resultlist, prev=tst)
		
	
def dictupdate(p_d1, p_d2, lvl=0):
	
	if not isinstance(p_d2, dict):
		return
	
	l = p_d1
	r = p_d2
	
	for k in r.keys():
		if k not in l.keys():
			l[k] = deepcopy(r[k])
		dictupdate(l[k], r[k], lvl=lvl+1)	
		
def subtree_fromkeychain(p_dict, p_keychainlist):

	if not isinstance(p_dict, dict):
		return None
		
	ret = p_dict
		
	for k in p_keychainlist:
		ret = ret[k]
		
	return ret
				
def gen_update(p_fase, p_transformschema, p_opordmgr, p_upperlevel_ops, p_keychain, p_diff_dict, p_raw_newvalue):
	
	lower_ops = subtree_fromkeychain(p_upperlevel_ops, p_keychain)
	assert not lower_ops is None
	
	#print("174", p_raw_newvalue_parent.keys(), p_keychain[-1])
		
	diff_item_parent = get_diff_item('b3', p_diff_dict, p_keychain[:-1])
	diff_item = diff_item_parent[p_keychain[-1]] = {  }
	
	p_opordmgr.setord(diff_item)

	diff_item["changedkeys"] =  ", ".join(lower_ops.keys())
	
	op = None
	difflist = None
	for lok in lower_ops.keys():
		if "op" in lower_ops[lok].keys():
			assert lower_ops[lok]["op"] != "delete"
			if op is None:
				op = lower_ops[lok]["op"]
			else:
				if op != "insert": # update ou insert, insert prevalece
					op = lower_ops[lok]["op"]
		if difflist is None and "difflist" in lower_ops[lok].keys():
			difflist = lower_ops[lok]["difflist"]

	if not difflist is None and len(difflist) > 0:
		diff_item["difflist"] = difflist
					
	diff_item["diffoper"] = op

	newvalue = deepcopy(p_raw_newvalue)
	traverse_replaceval(p_transformschema, newvalue, "gen_update %s" % p_fase)
	diff_item["newvalue"] = newvalue
				
		
																			
def comparegrp(p_leftdic, p_rightdic, grpkeys, p_transformschema, p_opordmgr, o_diff_dict, level=0): 
	
	logger = logging.getLogger('pgsourcing')
	
	grpkey = grpkeys[-1]
	
	try:
		tmp_l = p_leftdic[grpkey]
	except:
		logger.exception("comparegrp, error retrieving key from leftdict: '%s', level %d" % (grpkey, level))
		raise
	
	diff_dict = o_diff_dict
	
	ret_upperlevel_ops = {}
		
	# print("comparegrp", grpkeys) #, diff_dict)
	
	if not grpkey in p_rightdic.keys():

		try:
			
			diff_item = get_diff_item('a', diff_dict, grpkeys)

			p_opordmgr.setord(diff_item)
			diff_item["diffoper"] = "insert"   
			newvalue = deepcopy(tmp_l)
			traverse_replaceval(p_transformschema, newvalue, "insert A")
			diff_item["newvalue"] = newvalue
			
		except:
			logger.exception("comparegrp insert A, group: '%s', level %d" % (grpkey, level))
			raise
		
	else:
	
		tmp_r = p_rightdic[grpkey]
		keyset = set(tmp_l.keys())	
		keyset.update(tmp_r.keys())
		
		skeys = sorted(keyset)
		
		for k in skeys:
			
			klist  = grpkeys+[k]

			if k in tmp_l.keys() and not k in tmp_r.keys():
				# left only
				diff_item = get_diff_item('b', diff_dict, klist)
				
				# If starting a new group from scratch
				#  avoid inserting the whole group as a single insert operation
				
				if len(klist) == SHALLOW_DEPTH and not klist[0] in CFG_SHALLOW_GROUPS:

					for newkey in tmp_l[k].keys():
					
						newklist  = klist+[newkey]		
						upperlevel_ops = comparegrp(tmp_l[k], tmp_r, newklist, p_transformschema, p_opordmgr, diff_dict, level=level+1)
						if upperlevel_ops:
							dictupdate(ret_upperlevel_ops, upperlevel_ops)
				
				else:
				
					newvalue = deepcopy(tmp_l[k])
						
					p_opordmgr.setord(diff_item)
					diff_item["diffoper"] = "insert"				
					traverse_replaceval(p_transformschema, newvalue, "insert B")
					diff_item["newvalue"] = newvalue
				
			elif k in tmp_r.keys() and not k in tmp_l.keys():
				# right only
				diff_item = get_diff_item('b1', diff_dict, klist)
				p_opordmgr.setord(diff_item)
				diff_item["diffoper"] = "delete"
			
			else:
				
				if isinstance(tmp_l[k], dict) and isinstance(tmp_r[k], dict):
					upperlevel_ops = comparegrp(tmp_l, tmp_r, klist, p_transformschema, p_opordmgr, diff_dict, level=level+1)
					if upperlevel_ops:
						dictupdate(ret_upperlevel_ops, upperlevel_ops)

				elif isinstance(tmp_l[k], list) and isinstance(tmp_r[k], list):
					upperlevel_ops = comparegrp_list(tmp_l, tmp_r, klist, p_opordmgr, diff_dict)
					if upperlevel_ops:
						dictupdate(ret_upperlevel_ops, upperlevel_ops)
						
				elif isinstance(tmp_l[k], dict):
					assert not isinstance(tmp_r[k], list), "dict comparing to list, key: %s" % k
					diff_item = get_diff_item('c', diff_dict, klist)
					p_opordmgr.setord(diff_item)
					diff_item["diffoper"] = "replace value with dict"
					newvalue = deepcopy(tmp_l[k])
					traverse_replaceval(p_transformschema, newvalue, "replace A")
					diff_item["newvalue"] = newvalue
				elif isinstance(tmp_r[k], dict):
					assert not isinstance(tmp_l[k], list), "list a comparar com dict, chave: %s" % k
					diff_item = get_diff_item('d', diff_dict, klist)
					p_opordmgr.setord(diff_item)
					newvalue = deepcopy(tmp_l[k])
					traverse_replaceval(p_transformschema, newvalue, "replace B")
					diff_item["newvalue"] = newvalue

				else:
					if k == PROC_SRC_BODY_FNAME:
						
						# source de funcao / procedimento
						difflist = []
						newleft = sourcediff(tmp_l[k], tmp_r[k], p_transformschema, difflist)
						if len(difflist) > 0:
							# ki = grpkeys.index("procedures")
							curr_ulop = ret_upperlevel_ops
							for kc in grpkeys:
								if not kc in curr_ulop.keys():
									curr_ulop[kc] = {}
								curr_ulop = curr_ulop[kc]
							assert k not in curr_ulop.keys()
							curr_ulop[k] = {
								"op": "update",
								"difflist": difflist
							}
								
					else:

						rightval = tmp_r[k]						
						if p_transformschema:
							do_transformschema(p_transformschema, tmp_l, k)	
						leftval = tmp_l[k]	
											
						if leftval != rightval:
							
							pass_diff_construction = False
							for ulk in UPPERLEVELOPS.keys():
								if ulk in grpkeys:
									
									curr_ulop = ret_upperlevel_ops
									for kc in grpkeys:
										if not kc in curr_ulop.keys():
											curr_ulop[kc] = {}
										curr_ulop = curr_ulop[kc]
									assert k not in curr_ulop.keys()
									curr_ulop[k] = {
										"op": "update"
									}

									pass_diff_construction = True
									break
									
							if not pass_diff_construction:
							
								diff_item = get_diff_item('f', diff_dict, klist)
								p_opordmgr.setord(diff_item)
								diff_item["diffoper"] = "update"
								diff_item["newvalue"] = leftval
								diff_item["oldvalue"] = rightval
	
	if ret_upperlevel_ops:
		#for ulk in UPPERLEVELOPS.keys():
		kcl = []
		keychains(ret_upperlevel_ops, kcl)
		
		for ulk in UPPERLEVELOPS.keys():
			offset = UPPERLEVELOPS[ulk]
			for kc in kcl:
				if ulk in kc:
					ki = kc.index(ulk)
					if len(kc) == (offset + ki + 1):
						if kc[-1] == grpkey:
							gen_update('geral', p_transformschema, p_opordmgr, ret_upperlevel_ops, kc, diff_dict, tmp_l)
							break
							

			
		
		#print("antes ret lvl:", level, l)
							
	return ret_upperlevel_ops
						


def comparegrp_list(p_leftdic, p_rightdic, grpkeys, p_opordmgr, o_diff_dict): 

	grpkey = grpkeys[-1]
	tmp_l = p_leftdic[grpkey]
	diff_dict = o_diff_dict
	root_diff_item = None

	ret_upperlevel_ops = {}
	
	ret = None
	
	if not grpkey in p_rightdic.keys():
		
		diff_item = get_diff_item('A', diff_dict, grpkeys)
		p_opordmgr.setord(diff_item)
		diff_item["diffoper"] = "insert"
		diff_item["newvalue"] = deepcopy(tmp_l)
		
	else:
		
		tmp_r = p_rightdic[grpkey]
	
		keyset = set(tmp_l)	
		keyset.update(tmp_r)
				
		for k in sorted(keyset):
				
			pass_diff_construction = False
			
			if (k in tmp_l and not k in tmp_r) or (k in tmp_r and not k in tmp_l):
				
				for ulk in UPPERLEVELOPS.keys():
					if ulk in grpkeys:
						curr_ulop = ret_upperlevel_ops
						for kc in grpkeys[:-1]:
							if not kc in curr_ulop.keys():
								curr_ulop[kc] = {}
							curr_ulop = curr_ulop[kc]
						if not grpkey in curr_ulop.keys():
							curr_ulop[grpkey] = {
								"op": "update"
							}
						pass_diff_construction = True
						break
					
			if not pass_diff_construction:
				break

			if k in tmp_l and not k in tmp_r:					
				# left only
				diff_item = {}
				p_opordmgr.setord(diff_item)
				diff_item["diffoper"] = "addtolist"
				diff_item["newvalue"] = k
				if root_diff_item is None:
					root_diff_item = get_diff_item('B', diff_dict, grpkeys, b_leaf_is_list=True)
				root_diff_item.append(deepcopy(diff_item))
				
			elif k in tmp_r and not k in tmp_l:			
				# right only
				diff_item = {}
				p_opordmgr.setord(diff_item)
				diff_item["diffoper"] = "removefrom"
				diff_item["oldvalue"] = k
				if root_diff_item is None:
					root_diff_item = get_diff_item('B', diff_dict, grpkeys, b_leaf_is_list=True)
				root_diff_item.append(deepcopy(diff_item))
				
	return ret_upperlevel_ops
										

def comparing(p_proj, p_check_dict, p_comparison_mode, p_transformschema, p_opordmgr, o_diff_dict):
	
	raw_ref_json = load_currentref(p_proj)
		
	assert not raw_ref_json is None
	
	ref_json = raw_ref_json["content"]
	
	upperlevel_ops = {}
	
	# print("comparison_mode:", p_comparison_mode)

	if p_comparison_mode == "From SRC": 
				
		l_dict = p_check_dict
		r_dict = ref_json

		grplist = CFG_GROUPS
		
	else:	
			
		r_dict = p_check_dict
		
		l_dict = ref_json
		
		if p_transformschema:
			
			
			for sch in l_dict["schemas"].keys():
				for trans in p_transformschema["trans"]:
					if sch == trans["src"]:
						l_dict["schemas"][trans["dest"]] = l_dict["schemas"].pop(sch)
						break

			for tk in p_transformschema["types"]:
				if tk == "schemas":
					continue
				if tk in l_dict.keys():
					for sch in l_dict[tk].keys():
						for trans in p_transformschema["trans"]:
							if sch == trans["src"]:
								l_dict[tk][trans["dest"]] = l_dict[tk].pop(sch)
								break			

		grplist = CFG_DEST_GROUPS
	
	for grp in grplist:
		
		if grp in CFG_LISTGROUPS:
			ret = comparegrp_list(l_dict, r_dict, [grp], p_opordmgr, o_diff_dict)
		else:	
			ret = comparegrp(l_dict, r_dict, [grp], p_transformschema, p_opordmgr, o_diff_dict)
		
		if ret:
			dictupdate(upperlevel_ops, ret)
	
	# with open("lixodiff.json", "w") as of:
		# json.dump(upperlevel_ops, of, indent=2, sort_keys=True)
			
	chdict = o_diff_dict	

