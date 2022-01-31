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

import json
import re
import logging

from os.path import exists
from copy import copy, deepcopy
from difflib import unified_diff as dodiff

from src.common import PROC_SRC_BODY_FNAME, CFG_GROUPS, \
		CFG_DEST_GROUPS, CFG_LISTGROUPS, UPPERLEVELOPS, \
		CFG_SHALLOW_GROUPS, SHALLOW_DEPTH, STORAGE_VERSION, \
			gen_proc_fname_row
		
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
	elif p_k == "cdef":
		if "tables" in p_transformschema["types"] or "indexes" in p_transformschema["types"]:
			for trans in p_transformschema["trans"]:
				p_obj[p_k] = p_obj[p_k].replace(trans["src"], trans["dest"])
	elif p_k == "vdef":
		if "views" in p_transformschema["types"]:
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

def get_prevlevel_diff(p_diff_dict, p_grpkeys):

	# logger = logging.getLogger('pgsourcing')

	ret = None
	diff_dict = p_diff_dict
	for ki, k in enumerate(p_grpkeys):
		try:
			if ki < len(p_grpkeys) - 2:
				# try:
				diff_dict = diff_dict[k]
				# except:
				# 	# print(ki, k, diff_dict.keys(), p_diff_dict.keys())
				# 	raise
			else:
				ret = diff_dict[k]
				break
		except KeyError:
			# logger.error(f"get_prevlevel_diff error, ki:{ki}, k:{k}, diff_dict.keys():{diff_dict.keys()}, grpkeys:{p_grpkeys}")
			# raise
			ret = None
			break

	return ret	

def get_diff_item(p_diff_dict, p_grpkeys, opt_leaf_keys=None, b_leaf_is_list=False):
	
	diff_dict = p_diff_dict
	for ki, k in enumerate(p_grpkeys):
		if not k in diff_dict.keys():
			if b_leaf_is_list and ki == len(p_grpkeys) - 1:
				diff_dict[k] = []
			else:
				diff_dict[k] = {}
		diff_dict = diff_dict[k]
		# print("    ... 123 diff_dict", diff_dict.keys())

	if not opt_leaf_keys is None and len(opt_leaf_keys) == 1 and not b_leaf_is_list:
		k = opt_leaf_keys[0]
		diff_dict[k] = {}
		diff_dict = diff_dict[k]
		
	return diff_dict		

def sources_to_lists(p_src_a, p_src_b, p_list_a, p_list_b):
	
	del p_list_a[:]
	del p_list_b[:]

	rawlistA = p_src_a.splitlines(True)
	rawlistB = p_src_b.splitlines(True)
	
	patt = r"^--(.*)$"
	substitute = r"/* \1 */"
	p_list_a.extend([re.sub(patt, substitute, ln.strip()).lower() for ln in rawlistA if len(ln.strip()) > 0])
	p_list_b.extend([re.sub(patt, substitute, ln.strip()).lower() for ln in rawlistB if len(ln.strip()) > 0])
	
def sourcediff(p_srca, p_srcb, p_transformschema, out_dellist): #, out_addlist):
	
	del out_dellist[:]

	try:
		if "unicode" in str(type(p_srca)):
			srca = p_srca
		else:
			srca = p_srca.decode('utf-8')
		if "unicode" in str(type(p_srcb)):
			srcb = p_srcb
		else:
			srcb = p_srcb.decode('utf-8')

	except AttributeError:
		srca = p_srca
		srcb = p_srcb
		
	if p_transformschema:
		if "procedures" in p_transformschema["types"]:
			for trans in p_transformschema["trans"]:
				srca = srca.replace(trans["src"], trans["dest"])
		
	# rawlistA= srca.splitlines(True)
	# rawlistB = srcb.splitlines(True)
	
	listA = []
	listB = []
	
	sources_to_lists(srca, srcb, listA, listB)
	
	# patt = r"^--(.*)$"
	# substitute = r"/* \1 */"
	# #listA = [re.sub(' +', ' ', ln.strip()).lower() for ln in rawlistA if len(ln.strip()) > 0]
	# #listB = [re.sub(' +', ' ', ln.strip()).lower() for ln in rawlistB if len(ln.strip()) > 0]
	# #listA = [ln.strip().lower() for ln in rawlistA if len(ln.strip()) > 0]
	# #listB = [ln.strip().lower() for ln in rawlistB if len(ln.strip()) > 0]
	# # startswith
	# listA = [re.sub(patt, substitute, ln.strip()).lower() for ln in rawlistA if len(ln.strip()) > 0]
	# listB = [re.sub(patt, substitute, ln.strip()).lower() for ln in rawlistB if len(ln.strip()) > 0]
	
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

# Generate upper level update				
def gen_update(p_transformschema, p_opordmgr, p_upperlevel_ops, p_keychain, p_diff_dict, p_raw_parent_newvalue, p_raw_newvalue, stepback=False):
	
	lower_ops = subtree_fromkeychain(p_upperlevel_ops, p_keychain)
	assert not lower_ops is None

#	print("** lower_ops:", lower_ops)
#	print("** lower_ops:", lower_ops)

	# TODO : prolongado -- verificar impacto
	# if only change is in 'ordpos' attribute ignore, is only used for internal ordering of items
	if ", ".join(lower_ops.keys()) == "ordpos":
		return
	
	diff_item_parent = get_diff_item(p_diff_dict, p_keychain[:-1])
	if stepback:
		diff_item = diff_item_parent
	else:
		diff_item = diff_item_parent[p_keychain[-1]] = {  }

	
	# upper level update 'newvalue' includes grants to set
	if "grants" in diff_item.keys():
		del diff_item["grants"]

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

	if stepback:
		newvalue = deepcopy(p_raw_parent_newvalue)
	else:
		newvalue = deepcopy(p_raw_newvalue)
	traverse_replaceval(p_transformschema, newvalue, "doing gen_update")

	# Prevent sequences with 'serialcols_dependencies' to be updated if only diiference is 
	#  the current sequence value
	do_continue = True
	if op == "update" and p_keychain[0] == "sequences":
		if "serialcols_dependencies" in newvalue.keys() and "current" in diff_item["changedkeys"].split(", "):
			do_continue = False

	if do_continue:
		diff_item["newvalue"] = newvalue
				
def check_col_renaming_right(p_tmp_l, p_tmp_r, p_k):		

	ret = None
	assert "ordpos" in p_tmp_r[p_k].keys(), "check_col_renaming_right: 'ordpos' not in {}".format(", ".p_tmp_r[p_k].keys())

	for lft_fldname in p_tmp_l.keys():
		if p_tmp_r[p_k]["ordpos"] == p_tmp_l[lft_fldname]["ordpos"]:
			if p_tmp_r[p_k] == p_tmp_l[lft_fldname]:
				ret = lft_fldname
				break

	return ret

def check_tbl_renaming_right(p_tmp_l, p_tmp_r, p_k):

	ret = None 
	tmp = None
	eq_cnt = 0

	for lft_tblname in p_tmp_l.keys():
		if p_tmp_l[lft_tblname] == p_tmp_r[p_k]:
			tmp = lft_tblname
			eq_cnt += 1

	if eq_cnt == 1:
		ret = tmp

	return ret
																			
def comparegrp(p_leftdic, p_rightdic, grpkeys, p_transformschema, p_opordmgr, o_diff_dict, o_cd_ops, level=0): 
	
	logger = logging.getLogger('pgsourcing')

	grpkey = grpkeys[-1]	
	# print("::330:: grpkey:", grpkey, grpkeys)
	ret_upperlevel_ops = {}
	
	try:
		tmp_l = p_leftdic[grpkey]
	except:
		if level > 0:
			logger.exception("comparegrp, error retrieving key from leftdict: '%s', level %d" % (grpkey, level))
			raise
		else:
			logger.info("comparegrp, no key on leftdict: '%s'" % (grpkey,))
			return ret_upperlevel_ops
		
	diff_dict = o_diff_dict	
		
	if "error" in tmp_l.keys():
		return ret_upperlevel_ops
		
	# print("comparegrp:", grpkeys, p_rightdic.keys(), level) #, diff_dict)
	# print("    p_rightdict:", p_rightdic.keys()) #, diff_dict)
	
	printdbg = False

	# TODO: permanente -- verificar que está vazio em produção
	if grpkey in []: # ("sequences",):
		printdbg = True
	
	if not grpkey in p_rightdic.keys():
		
		if printdbg:
			print("not in right keys:", grpkey)
		
		try:
			diff_item = get_diff_item(diff_dict, grpkeys)
			
			if level == 0:
				# lzero_key is a schema, if not exists is created elsewhere
				for lzero_key in tmp_l.keys(): 
					diff_item[lzero_key] = {}
					for lk in tmp_l[lzero_key].keys():
						diff_item[lzero_key][lk] = {}
						p_opordmgr.setord(diff_item[lzero_key][lk])
						diff_item[lzero_key][lk]["diffoper"] = "insert"   
						newvalue = deepcopy(tmp_l[lzero_key][lk])
						traverse_replaceval(p_transformschema, newvalue, "insert A0")
						diff_item[lzero_key][lk]["newvalue"] = newvalue
						new_grpkeys = grpkeys + [lzero_key] + [lk]
						o_cd_ops["insert"].append(('a', new_grpkeys))
			else:
				p_opordmgr.setord(diff_item)
				diff_item["diffoper"] = "insert"   
				newvalue = deepcopy(tmp_l)
				traverse_replaceval(p_transformschema, newvalue, "insert A1")
				diff_item["newvalue"] = newvalue
				
				if grpkeys[0] == "procedures" and "procedure_name" in newvalue.keys():
					procedure_key = gen_proc_fname_row(newvalue)
					new_grpkeys = grpkeys[:-1] + [procedure_key]
					o_cd_ops["insert"].append(('b', new_grpkeys)) #, newvalue["args"], newvalue["return_type"]))
				else:
					new_grpkeys = grpkeys
					o_cd_ops["insert"].append(('c', new_grpkeys))
			
		except:
			logger.exception("comparegrp insert A, group: '%s', level %d" % (grpkey, level))
			raise
		
	else:

		tmp_r = p_rightdic[grpkey]
		keyset = set(tmp_l.keys())	
		keyset.update(tmp_r.keys())
		
		skeys = sorted(keyset)
		
		for k in skeys:
			
			klist = grpkeys+[k]
			rkeys = tmp_r.keys()

			if k in tmp_l.keys() and not k in rkeys:
				
				if printdbg:
					print("*** left only:", grpkey, k, level, tmp_l.keys(), rkeys, ",".join(diff_dict.keys()))
					# print("    UPPERLEVELOPS.keys():", UPPERLEVELOPS.keys())
				# left only

				# If starting a new group from scratch
				#  avoid inserting the whole group as a single insert operation				
				if klist[-1] in UPPERLEVELOPS.keys() or (len(klist) == SHALLOW_DEPTH and not klist[0] in CFG_SHALLOW_GROUPS):

					diff_item = get_diff_item(diff_dict, klist)

					for newkey in tmp_l[k].keys():
					
						newklist  = klist+[newkey]		
						upperlevel_ops = comparegrp(tmp_l[k], tmp_r, newklist, p_transformschema, p_opordmgr, diff_dict, o_cd_ops, level=level+1)
						if upperlevel_ops:
							dictupdate(ret_upperlevel_ops, upperlevel_ops)
				
				else:

					# Renamed columns - prevent the insertion of a new column 
					#  (using renamed name) when corresponding renaming operation already exists
					#  if diff dict
					do_continue = True
					if grpkey == "cols":

						if diff_dict:

							prevlevel_diff = get_prevlevel_diff(diff_dict, klist)
							if not prevlevel_diff is None:
								for exist_colname in prevlevel_diff.keys():
									if prevlevel_diff[exist_colname]["diffoper"] == "rename":
										if prevlevel_diff[exist_colname]["newvalue"] == k:
											do_continue = False
											break

					elif grpkey not in CFG_GROUPS and klist[0] == "tables" and len(klist) == 3:

						if diff_dict and "tables" in diff_dict.keys():

							prevlevel_diff = get_prevlevel_diff(diff_dict, klist)
							if not prevlevel_diff is None:
								for exist_tblname in prevlevel_diff.keys():
									if prevlevel_diff[exist_tblname]["diffoper"] == "rename":
										if prevlevel_diff[exist_tblname]["newvalue"] == k:
											do_continue = False
											break		

					# print(" *** left only B:", grpkey, k, level, "do_continue:", do_continue)

					if do_continue:				
					
						newvalue = deepcopy(tmp_l[k])
						if not isinstance(newvalue, dict) or not "error" in newvalue.keys():

							diff_item = get_diff_item(diff_dict, klist)

							p_opordmgr.setord(diff_item)
							diff_item["diffoper"] = "insert"							
							traverse_replaceval(p_transformschema, newvalue, "insert B")
							diff_item["newvalue"] = newvalue

							new_grpkeys = grpkeys + [k]
							# print("***", grpkeys, '+', k)
							if isinstance(newvalue, dict) and grpkeys[0] == "procedures":
								assert "args" in newvalue.keys() and "return_type" in newvalue.keys(), newvalue.keys()
								o_cd_ops["insert"].append(('d1', new_grpkeys, newvalue["args"], newvalue["return_type"]))
							else:
								o_cd_ops["insert"].append(('d2', new_grpkeys))
										
			elif k in rkeys and not k in tmp_l.keys():

				if isinstance(tmp_r[k], dict):
					rightkeys = list(tmp_r[k].keys())
				else:
					rightkeys = None

				if printdbg:
					print("*** right only:", grpkey, k, level, "tmp_r['"+k+"'].keys():", rightkeys)

				# right only
				diff_item = get_diff_item(diff_dict, klist, opt_leaf_keys=rightkeys)

				# HANDLE column renaming
				# Must compare columns in same orderpos, to check if name is the single difference
				changed = False
				if grpkey == "cols" and "ordpos" in rightkeys:
					
					renamed_colname = check_col_renaming_right(tmp_l, tmp_r, k)
					if not renamed_colname is None:

						p_opordmgr.setord(diff_item)
						diff_item["diffoper"] = "rename"
						diff_item["newvalue"] = renamed_colname
						changed = True


				elif grpkey == "tables" or (grpkey not in CFG_GROUPS and klist[0] == "tables"):

					renamed_tablename = check_tbl_renaming_right(tmp_l, tmp_r, k)
					if not renamed_tablename is None:

						p_opordmgr.setord(diff_item)
						diff_item["diffoper"] = "rename"
						diff_item["newvalue"] = renamed_tablename
						changed = True

				if not changed:

					p_opordmgr.setord(diff_item)
					diff_item["diffoper"] = "delete"

					o_cd_ops["delete"].append(('e', klist))
					
					# if grpkeys[0] == "procedures":
						# diff_item["procedure_name"] = tmp_r[k]["procedure_name"]
						# diff_item["args"] = tmp_r[k]["args"]
			
			else:

				if isinstance(tmp_l[k], dict) and isinstance(tmp_r[k], dict):
					upperlevel_ops = comparegrp(tmp_l, tmp_r, klist, p_transformschema, p_opordmgr, diff_dict, o_cd_ops, level=level+1)
					if upperlevel_ops:
						# if "tables" in upperlevel_ops.keys():
							# print(".. 292 ..", upperlevel_ops)
						dictupdate(ret_upperlevel_ops, upperlevel_ops)

				elif isinstance(tmp_l[k], list) and isinstance(tmp_r[k], list):
					upperlevel_ops = comparegrp_list(tmp_l, tmp_r, klist, p_opordmgr, diff_dict) #, o_cd_ops, level=level+1)
					if upperlevel_ops:
						# print(".. 298 ..", upperlevel_ops)
						dictupdate(ret_upperlevel_ops, upperlevel_ops)
						
				elif isinstance(tmp_l[k], dict):
					assert not isinstance(tmp_r[k], list), "dict comparing to list, key: %s" % k
					diff_item = get_diff_item(diff_dict, klist)
					p_opordmgr.setord(diff_item)
					diff_item["diffoper"] = "replace value with dict"
					newvalue = deepcopy(tmp_l[k])
					traverse_replaceval(p_transformschema, newvalue, "replace A")
					diff_item["newvalue"] = newvalue
				elif isinstance(tmp_r[k], dict):
					assert not isinstance(tmp_l[k], list), "list a comparar com dict, chave: %s" % k
					diff_item = get_diff_item(diff_dict, klist)
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

		kcl = []
		keychains(ret_upperlevel_ops, kcl)

		# print("------ ret_upperlevel_ops --------")
		# print(ret_upperlevel_ops)
		# print("----------------------------------")
		
		for ulk in UPPERLEVELOPS.keys():
			dostepback = False
			if isinstance(UPPERLEVELOPS[ulk], int):
				offset = UPPERLEVELOPS[ulk]
			else:
				offset, dostepback = UPPERLEVELOPS[ulk]
			testkey = grpkey
			for kc in kcl:
				if ulk in kc:
					# print("..348..", ulk, kc)
					ki = kc.index(ulk)
					# if ulk == "mvdetails":
						# print("     381:", len(kc), "==", (offset + ki + 1), kc[-1], "==", testkey)
					if len(kc) == (offset + ki + 1):
						if kc[-1] == testkey:
							gen_update(p_transformschema, p_opordmgr, ret_upperlevel_ops, kc, diff_dict, p_leftdic, tmp_l, stepback=dostepback)
							break

	return ret_upperlevel_ops
						


def comparegrp_list(p_leftdic, p_rightdic, grpkeys, p_opordmgr, o_diff_dict): #, o_cd_ops, level=0): 

	grpkey = grpkeys[-1]
	tmp_l = p_leftdic[grpkey]
	diff_dict = o_diff_dict
	root_diff_item = None

	ret_upperlevel_ops = {}
	
	ret = None
	
	if not grpkey in p_rightdic.keys():
		
		diff_item = get_diff_item(diff_dict, grpkeys)
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
					root_diff_item = get_diff_item(diff_dict, grpkeys, b_leaf_is_list=True)
				root_diff_item.append(deepcopy(diff_item))
				
			elif k in tmp_r and not k in tmp_l:			
				# right only
				diff_item = {}
				p_opordmgr.setord(diff_item)
				diff_item["diffoper"] = "removefrom"
				diff_item["oldvalue"] = k
				if root_diff_item is None:
					root_diff_item = get_diff_item(diff_dict, grpkeys, b_leaf_is_list=True)
				root_diff_item.append(deepcopy(diff_item))
				
	return ret_upperlevel_ops
										

def comparing(p_proj, p_check_dict, p_comparison_mode, p_transformschema, p_opordmgr, o_diff_dict, o_cd_ops):

	logger = logging.getLogger('pgsourcing')
	raw_ref_json = load_currentref(p_proj)

	# print("--------")
	# print(json.dumps(raw_ref_json, indent=4))
	# print("--------")
	
	assert not raw_ref_json is None
	if raw_ref_json["pgsourcing_storage_ver"] < STORAGE_VERSION:
		logger.warning("DEPRECATED STORAGE format version %s < %s. Must delete reference. Please run, in sequence, 'dropref' and 'chksrc' ASAP" % (raw_ref_json["pgsourcing_storage_ver"], STORAGE_VERSION)) 
	if raw_ref_json["pgsourcing_storage_ver"] > STORAGE_VERSION:
		raise RuntimeError("INVALID STORAGE format version %s > %s!" % (raw_ref_json["pgsourcing_storage_ver"], STORAGE_VERSION)) 
	
	ref_json = raw_ref_json["content"]
	
	upperlevel_ops = {}
	
	logger.info(f"comparison_mode: {p_comparison_mode}")

	if p_comparison_mode == "From SRC": 
				
		l_dict = p_check_dict
		r_dict = ref_json

		grplist = CFG_GROUPS
		
	else:	
			
		r_dict = p_check_dict		
		l_dict = ref_json
		
		if p_transformschema:
						
			first_schema_names = list(l_dict["schemata"].keys())
			for sch in first_schema_names:
				for trans in p_transformschema["trans"]:
					if sch == trans["src"]:
						l_dict["schemata"][trans["dest"]] = l_dict["schemata"].pop(sch)
						break

			for tk in p_transformschema["types"]:
				if tk == "schemata":
					continue
				first_types = list(l_dict.keys())
				if tk in first_types:
					first_schema_names = list(l_dict[tk].keys())
					for sch in first_schema_names:
						for trans in p_transformschema["trans"]:
							if sch == trans["src"]:
								l_dict[tk][trans["dest"]] = l_dict[tk].pop(sch)
								break			

		grplist = CFG_DEST_GROUPS
	
	for grp in grplist:
		
		if grp in CFG_LISTGROUPS:
			ret = comparegrp_list(l_dict, r_dict, [grp], p_opordmgr, o_diff_dict)
		else:	
			ret = comparegrp(l_dict, r_dict, [grp], p_transformschema, p_opordmgr, o_diff_dict, o_cd_ops)
		
		if ret:
			dictupdate(upperlevel_ops, ret)
			
	# chdict = o_diff_dict	

