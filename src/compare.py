

import json
import re

from os.path import exists
from copy import deepcopy
from difflib import unified_diff as dodiff

from src.common import PROC_SRC_BODY_FNAME, CFG_GROUPS, CFG_LISTGROUPS
from src.fileandpath import load_currentref

# def _get_diff_item(p_fase, p_parentgrp, p_diff_dict, p_grpkey, p_k=None):
	# # print("..... _get_diff_item ",p_fase,p_parentgrp, p_diff_dict, p_grpkey, 'p_k:', p_k)
	# if not p_parentgrp is None:
		# if not p_parentgrp in p_diff_dict.keys():
			# p_diff_dict[p_parentgrp] = {}
		# root = p_diff_dict[p_parentgrp]
	# else:
		# root = p_diff_dict
	# if not p_grpkey in root.keys():
		# root[p_grpkey] = {}
	# if p_k is None:
		# ret = root[p_grpkey]
	# else:
		# if not p_k in root[p_grpkey].keys():
			# root[p_grpkey][p_k] = {}
		# ret = root[p_grpkey][p_k]
	# return ret

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
		

def sourcediff(p_srca, p_srcb, p_replaces, out_dellist): #, out_addlist):
	
	del out_dellist[:]

	if "unicode" in str(type(p_srca)):
		srca = p_srca
	else:
		srca = p_srca.decode('utf-8')

	if "unicode" in str(type(p_srcb)):
		srcb = p_srcb
	else:
		srcb = p_srcb.decode('utf-8')
		
	for from_schema, to_schema in p_replaces:
		srca = srca.replace(to_schema, from_schema)
		
	rawlistA= srca.splitlines(True)
	rawlistB = srcb.splitlines(True)
	listA = [re.sub(' +', ' ', ln.strip()).lower() for ln in rawlistA if len(ln.strip()) > 0]
	listB = [re.sub(' +', ' ', ln.strip()).lower() for ln in rawlistB if len(ln.strip()) > 0]
	
	diff = [l.strip() for l in list(dodiff(listA, listB)) if l.strip()]
	
	out_dellist.extend(diff)
	
	return srca
	
def comparegrp(p_leftdic, p_rightdic, grpkeys, p_replaces, p_opordmgr, o_diff_dict): 
		
	grpkey = grpkeys[-1]
	tmp_l = p_leftdic[grpkey]
	
	diff_dict = o_diff_dict
		
	#print("comparegrp", grpkeys, diff_dict)
	
	if not grpkey in p_rightdic.keys():

		diff_item = get_diff_item('a', diff_dict, grpkeys)

		#diff_item = _get_diff_item('a',newparentgroup, diff_dict, grpkey)
		p_opordmgr.setord(diff_item)
		diff_item["diffoper"] = "insert"
		diff_item["value"] = deepcopy(tmp_l)
		
	else:
	
		tmp_r = p_rightdic[grpkey]
		keyset = set(tmp_l.keys())	
		keyset.update(tmp_r.keys())
		
		for k in sorted(keyset):
			klist  = grpkeys+[k]
			if k in tmp_l.keys() and not k in tmp_r.keys():
				# left only
				#diff_item = _get_diff_item('b',newparentgroup, diff_dict, grpkey, k)
				diff_item = get_diff_item('b', diff_dict, klist)
				p_opordmgr.setord(diff_item)
				diff_item["diffoper"] = "insert"
				diff_item["newvalue"] = deepcopy(tmp_l[k])
			elif k in tmp_r.keys() and not k in tmp_l.keys():
				# right only
				#diff_item = _get_diff_item('b',newparentgroup, diff_dict, grpkey, k)
				diff_item = get_diff_item('b1', diff_dict, klist)
				p_opordmgr.setord(diff_item)
				diff_item["diffoper"] = "delete"
			else:
				if isinstance(tmp_l[k], dict) and isinstance(tmp_r[k], dict):
					comparegrp(tmp_l, tmp_r, klist, p_replaces, p_opordmgr, diff_dict)
				elif isinstance(tmp_l[k], list) and isinstance(tmp_r[k], list):
					comparegrp_list(tmp_l, tmp_r, klist, p_opordmgr, diff_dict)
				elif isinstance(tmp_l[k], dict):
					assert not isinstance(tmp_r[k], list), "dict a comparar com list, chave: %s" % k
					diff_item = get_diff_item('c', diff_dict, klist)
					p_opordmgr.setord(diff_item)
					diff_item["diffoper"] = "replace value with dict"
					diff_item["newvalue"] = deepcopy(tmp_l[k])
				elif isinstance(tmp_r[k], dict):
					assert not isinstance(tmp_l[k], list), "list a comparar com dict, chave: %s" % k
					diff_item = get_diff_item('d', diff_dict, klist)
					p_opordmgr.setord(diff_item)
					diff_item["diffoper"] = "replace dict with value"
					diff_item["newvalue"] = deepcopy(tmp_l[k])
				else:
					if k == PROC_SRC_BODY_FNAME:
						
						# source de funcao / procedimento
						difflist = []
						newleft = sourcediff(tmp_l[k], tmp_r[k], p_replaces, difflist)
						if len(difflist) > 0:
							diff_item = get_diff_item('e', diff_dict, klist)
							diff_item["difflines"] = difflist
							p_opordmgr.setord(diff_item)
							diff_item["diffoper"] = "update"
							diff_item["newvalue"] = newleft
							
					else:

						rightval = tmp_r[k]
						leftval = tmp_l[k]
						
						if grpkeys[-1] == "index":
							for from_schema, to_schema in p_replaces:
								leftval = leftval.replace(to_schema, from_schema)
						elif grpkeys[-2] == "trigger":
							if k == "function_schema":
								for from_schema, to_schema in p_replaces:
									leftval = leftval.replace(to_schema, from_schema)

						if leftval != rightval:
							diff_item = get_diff_item('f', diff_dict, klist)
							p_opordmgr.setord(diff_item)
							diff_item["diffoper"] = "update"
							diff_item["newvalue"] = leftval
							diff_item["oldvalue"] = rightval
						


def comparegrp_list(p_leftdic, p_rightdic, grpkeys, p_opordmgr, o_diff_dict): 

	grpkey = grpkeys[-1]
	tmp_l = p_leftdic[grpkey]
	diff_dict = o_diff_dict
	root_diff_item = None
	
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
										

def comparing(p_proj, p_check_dict, p_comparison_mode, p_replaces, p_opordmgr, o_diff_dict):
	
	raw_ref_json = load_currentref(p_proj)
		
	assert not raw_ref_json is None
	
	ref_json = raw_ref_json["content"]
	
	if p_comparison_mode == "From SRC": 
		l_dict = p_check_dict
		r_dict = ref_json
	else:
		r_dict = p_check_dict
		l_dict = ref_json
		
	# Coparar lista schemas~
	
	for grp in CFG_GROUPS:
		
		if grp in CFG_LISTGROUPS:
			comparegrp_list(l_dict, r_dict, [grp], p_opordmgr, o_diff_dict)
		else:	
			comparegrp(l_dict, r_dict, [grp], p_replaces, p_opordmgr, o_diff_dict)
			
	chdict = o_diff_dict	
	for from_schema, to_schema in p_replaces:

		for grp in chdict.keys():
			
			if not grp in CFG_GROUPS:
				continue
			if grp in CFG_LISTGROUPS:
				continue
				
			for sch in chdict[grp].keys():

				if sch == to_schema:					
					chdict[grp][from_schema] = chdict[grp].pop(sch)
			
			
