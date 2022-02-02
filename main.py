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

# -------------------------------------------------- #
# pgsourcing/main.py - Versão 2.0 - Python 3         #
# -------------------------------------------------- #
# Rui Pedro Cavaco Barrosa, Porto, Setembro de 2020  #
# -------------------------------------------------- #

# ----------------------------------------------------------------------
# ==== pgsourcing: 'migrations tool' para PostgreSQL ====
# ----------------------------------------------------------------------
#
# Objectivo geral: gerir versões de estrutura de base de dados e 
#	código fonte de procedimentos.
#
#
# Ficheiros desta solução:
#	- main.py
#	- src/*.py (varios)
#
# ----------------------------------------------------------------------

#from __future__ import print_function
from os import scandir, mkdir, makedirs, walk, remove as removefile
from os.path import abspath, dirname, exists, splitext, join as path_join
from datetime import datetime as dt
from copy import deepcopy, copy
from difflib import unified_diff as dodiff
from subprocess import check_call

#################
# import pdb
#################


# from psycopg2.extras import execute_batch
from psycopg2.errors import InvalidFunctionDefinition

import argparse
import logging
import logging.config
import json
import codecs
import re
# import pprint as pp

from src.common import LOG_CFG, LANG, OPS, OPS_INPUT, \
		OPS_OUTPUT, OPS_HELP, OPS_CHECK, OPS_DBCHECK, OPS_CODE, OPS_PRECEDENCE, \
		SETUP_ZIP, BASE_CONNCFG, PROC_SRC_BODY_FNAME, STORAGE_VERSION  #  BASE_FILTERS_RE

from src.common import gen_proc_fname, reverse_proc_fname		
from src.read import dbreader
from src.connect import Connections
from src.compare import comparing, sources_to_lists
from src.zip import gen_setup_zip
from src.fileandpath import get_conn_cfg_path, get_filters_cfg, \
		exists_currentref, to_jsonfile, save_ref, get_refcodedir, \
		get_destcodedir, save_warnings, get_srccodedir, \
		get_reftablesdir, dropref, genprojectarchive
from src.write import updateref, updatedb, create_function_items
from src.sql import SQL

from io import StringIO, IOBase
from shutil import copyfile 

file_types = (IOBase,)



    
class Singleton(object):
	_instances = {}
	def __new__(class_, *args, **kwargs):
		if class_ not in class_._instances:
			class_._instances[class_] = super(Singleton, class_).__new__(class_, *args, **kwargs)
		return class_._instances[class_]



# ######################################################################
# Config, setup, bootstrap
# ######################################################################		

def parse_args():
	
	parser = argparse.ArgumentParser(description='Diff and definition migrations on PostgreSQL')
	
	projdir = path_join(dirname(abspath(__file__)), 'projetos')	
	projetos = [ f.name for f in scandir(projdir) if f.is_dir() ]
	
	ops_help = OPS_HELP[LANG]
	ops_input = ",".join(OPS_INPUT)
	
	project_oriented = parser.add_argument_group('project_oriented', 'Project oriented arguments')
	project_oriented.add_argument("proj", nargs="?", action="store", help="Project name, chosen from these: %s" % str(projetos))
	project_oriented.add_argument("oper", nargs="?", action="store", help="Operation name, chosen from these: %s" % json.dumps(ops_help, indent=4))
	
	project_oriented.add_argument("-o", "--output", help="Output file", action="store")
	project_oriented.add_argument("-i", "--input", help="Input file (REQUIRED on 'oper' %s)" % ops_input, action="store")
	project_oriented.add_argument("-c", "--connkey", help="Database connection key on project config (default is 'src' for 'chksrc' op)", action="store")
	project_oriented.add_argument("-p", "--includepublic", help="Include public schema", action="store_true")
	project_oriented.add_argument("-r", "--removecolorder", help="Remove table column ordering", action="store_true")
	project_oriented.add_argument("-g", "--genprocsdir", help="Procedure source folder name to create", action="store")
	project_oriented.add_argument("-d", "--updateids", help="Update ids list, to filter tasks in update operation", action="store")
	project_oriented.add_argument("-a", "--addnewproc", help="Generate a new empty procedure source file", action="store_true")
	project_oriented.add_argument("-t", "--addnewtrig", help="Generate a new empty trigger source file", action="store_true")
	project_oriented.add_argument("-u", "--simulupdcode", help="Activate simulation mode on 'updcodeinsrc' operation: actual update is replaced by stdout messages (without DDL)", action="store_true")

	non_project = parser.add_argument_group('non_project', 'Non-project generic command options')
	non_project.add_argument("-s", "--setup", help="Just generate a setup ZIP (ZIP will include the whole pgsourcing software and existing projects)", action="store_true")
	non_project.add_argument("-n", "--newproj", help="Create empty project", action="store")

	expert_group = parser.add_argument_group('expert_group', 'Expert use options')
	expert_group.add_argument("-k", "--limkeys", help="[EXPERT USE] object type keys list to filter update op, only the types listed will be changed (comma, semicolon and optional additional space separated list)", action="store")
	expert_group.add_argument("-m", "--delmode", help="[EXPERT USE] -- CAUTION - you may destroy data -- delete mode: NODEL (default), DEL, CASCADE", action="store")

	args = parser.parse_args()

	if args.delmode != "NODEL" and args.oper == "upddestdirect":
		raise RuntimeError(f"cannot use delmode != 'NODEL' when using 'upddestdirect', directly changing dest db is too dangerous with active deletion on. Update first to script using 'upddestscript' and check all risky steps")
	
	logger = logging.getLogger('pgsourcing')	
	logger.info("Starting, args: %s" % args)
	
	# mutually exclusive flags
	mutexflags = set()
	if args.setup:
		mutexflags.add("setup")
	if not args.newproj is None:
		mutexflags.add("newproj")
	
	if len(mutexflags) > 1:
		parser.print_help()
		raise RuntimeError("options -s e -n are mutually exclusive")
	
	proj = None
	if len(mutexflags) == 0:
		if args.proj is None:
			parser.print_help()
			raise RuntimeError("Project name required (parameter proj), except when using options -s e -n")
		if args.proj in projetos:
			proj = args.proj
		else:
			# Tentar encontrar por prefixo
			possibs = []
			for prj in projetos:
				if prj.startswith(args.proj):
					possibs.append(prj)
					
			if len(possibs) == 1:
				proj = possibs[0]
			else:
				raise RuntimeError("Project '%s' is missing, found projects: %s" % (args.proj, str(projetos)))
				
		if not args.oper is None and not args.addnewproc and not args.addnewtrig and not args.oper in OPS:

			startwiths = []
			for a_op in OPS:
				if a_op.startswith(args.oper):
					startwiths.append(a_op)
			if len(startwiths) == 1:
				resp = input(f"- operation '{args.oper}' not found, you meant '{startwiths[0]}' (y/other) ? ")
				if resp.lower() == 'y':	
					args.oper = startwiths[0]		
			else:
				if args.oper == "chkref":
					resp = input(f"- operation '{args.oper}' not found, you meant 'chksrc' (y/other) ? ")
					if resp.lower() == 'y':	
						args.oper = "chksrc"		
			
		if not args.addnewproc and not args.addnewtrig and not args.oper in OPS:

			logger.error("--- available ops --")
			logger.error(json.dumps(list(ops_help.keys()), indent=4))
			logger.error(json.dumps(ops_help, indent=4))
			raise RuntimeError("Invalid operation '%s'" % (args.oper,))

		# if args.oper in OPS_INPUT and args.input is None:
		# 	parser.print_help()
		# 	raise RuntimeError("Operacao '%s' exige indicacao ficheiro de entrada com opcao -i" % args.oper)

		if args.oper in OPS_OUTPUT and args.output is None:
			parser.print_help()
			raise RuntimeError("Operation '%s' requires output file name, given with option -o" % args.oper)
			
	if args.input:
		assert exists(args.input), "Ficheiro de entrada inexistente: '%s'" % args.input
		
	return args, proj

def log_setup(tosdout=False):
	
	logger = logging.getLogger('pgsourcing')
	logger.setLevel(LOG_CFG["level"])
	logfmt = logging.Formatter(LOG_CFG["format"])
	fh = logging.FileHandler(LOG_CFG["filename"])
	fh.setFormatter(logfmt)
	logger.addHandler(fh)
	if tosdout:
		ch = logging.StreamHandler()
		logger.addHandler(ch)

def do_output(p_obj, output=None, interactive=False, diff=False):

	if p_obj:
		
		if output is None:
			if interactive and diff:
				json_out = json.dumps(p_obj, indent=2, sort_keys=True)
				print(json_out) 
		else:
			dosave = False
			if not isinstance(output, StringIO) and exists(output) and interactive:
				prompt = "Out file exists, overwrite ? (y/other) "
				resp = input(prompt)
				if resp.lower() == 'y':
					dosave = True
			else:
				dosave = True
					
			if dosave:
				to_jsonfile(p_obj, output)
				
def do_linesoutput(p_obj, output=None, interactive=False):
	
	outer_sep = ";\n"

	if len(p_obj) > 0:
		
		finallines = []
		for item in p_obj:

			try:
				flagv = isinstance(item, basestring)
			except NameError:
				flagv = isinstance(item, str)

			if flagv:
				if len(item) > 0:
					if item.strip().endswith('##'):
						finallines.append(item + "\n")
					else:
						if item.strip().endswith(';'):
							finallines.append(item + "\n")
						else:
							finallines.append(item + outer_sep)
			elif isinstance(item, list):
				finallines.append("\n".join(item) + outer_sep)
		
		if output is None:
			if interactive:
				out_str = "\n".join(finallines[:5])
				print(out_str) 
		else:
			dosave = False

			try:
				flagv = isinstance(output, basestring)
			except NameError:
				flagv = isinstance(output, str)

			if flagv:
				
				if exists(output) and interactive:
					prompt = "Out file exists, overwrite ? (y/other) "
					resp = input(prompt)
					if resp.lower() == 'y':
						dosave = True
				else:
					dosave = True
					
				if dosave:
					with codecs.open(output, "w", encoding="utf-8") as fj:
						fj.write("\n".join(finallines))
							
			elif isinstance(output, file_types):
				output.write("\n".join(finallines))
					

			
def check_filesystem():
	
	the_dir = path_join(dirname(abspath(__file__)), "projetos")	
	if not exists(the_dir):
		mkdir(the_dir)

	the_dir = path_join(dirname(abspath(__file__)), "log")	
	if not exists(the_dir):
		mkdir(the_dir)
	
	zip_file_path = path_join(dirname(abspath(__file__)), SETUP_ZIP)
	if exists(zip_file_path):
		removefile(zip_file_path)			
	
def create_new_proj(p_newproj):

	root_dir = path_join(dirname(abspath(__file__)), "projetos")	
	if not exists(root_dir):
		mkdir(root_dir)
		
	proj_dir = path_join(root_dir, p_newproj)
	if not exists(proj_dir):
		mkdir(proj_dir)

	the_dir = path_join(proj_dir, "reference")
	if not exists(the_dir):
		mkdir(the_dir)
		
	with open(path_join(proj_dir, "conncfg.json"), "w") as fl:
		json.dump(BASE_CONNCFG, fl, indent=2)
		
	# with open(path_join(proj_dir, "filtros.json"), "w") as fl:
		# json.dump(BASE_FILTERS_RE, fl, indent=2)


# ######################################################################


# ######################################################################
# MAIN LOGIC
# ##################################################_###################		

def check_oper_handler(p_proj, p_oper, p_outprocsdir, p_outtables_dir, 
		p_outprocsdestdir, o_checkdict, o_replaces, p_connkey=None, 
		include_public=False, include_colorder=False):
	
	logger = logging.getLogger('pgsourcing')	
	
	ret = "None"

	conns = None
	connkey = None
	
	if p_oper in OPS_DBCHECK:
	
		cfgpath = get_conn_cfg_path(p_proj)
		conns = Connections(cfgpath, subkey="conn")
		
		if p_connkey is None:	
			if p_oper == "chksrc":
				if not conns.checkConn("src"):
					raise RuntimeError("Chksrc, implicit 'src' connection is not defined, must provide an explicit conn key using -c / --connkey option")
				else:
					connkey = 'src'
			elif p_oper == "chkdest":
				if not conns.checkConn("dest"):
					raise RuntimeError("Chkdest, implicit 'dest' connection is not defined, must provide an explicit conn key using -c / --connkey option")
				else:
					connkey = 'dest'
		else:	
			connkey = p_connkey		

		is_upstreamdb = None
		
		if p_oper == "chksrc":

			logger.info("checking source, proj:%s  oper:%s" % (p_proj,p_oper))
			outprocs_dir = p_outprocsdir
			ret = "From SRC"
			is_upstreamdb = True
			
		elif p_oper == "chkdest":
			
			logger.info("checking dest, proj:%s  oper:%s" % (p_proj,p_oper))
			outprocs_dir = p_outprocsdestdir
			ret = "From REF"
			is_upstreamdb = False
		
		if not connkey is None:
			
			filters_cfg = get_filters_cfg(p_proj, connkey)

			o_replaces.clear()
			if "transformschema" in filters_cfg.keys():
				o_replaces.update(filters_cfg["transformschema"])

			dbreader(conns.getConn(connkey), filters_cfg, o_checkdict, p_outtables_dir, outprocs_dir=outprocs_dir, include_public=include_public, include_colorder=include_colorder, is_upstreamdb=is_upstreamdb)
		
	return ret, connkey

def process_intervals_string(p_input_str):
	
	sequence = set()
	
	regroups = re.split("[,;/#]", p_input_str)
	
	for grp in regroups:
		interv = re.split("[:\-.]+", grp)
		if len(interv) == 1:
			try:
				val = int(interv[0])
				sequence.add(val)
			except ValueError:
				pass
		elif len(interv) == 2:
			try:
				if len(interv[0]) == 0:
					bot = 1
				else:
					bot = int(interv[0])
				top = int(interv[1])
				if bot > top:
					tmp = bot
					bot = top
					top = tmp
				if bot < 1:
					bot = 1	
				sequence.update(range(bot, top+1))
			except ValueError:
				pass
		
	return sorted(sequence)
	
def update_oper_handler(p_proj, p_oper, diffdict, 
		updates_ids=None, p_connkey=None, limkeys=None, 
		include_public=False, include_colorder=False,
		output=None, canuse_stdout=False, delmode="NODEL"):

	# delmode: NODEL DEL CASCADE 
	
	# updates_ids - se None ou lista vazia, aplicar todo o diffdict
	
	logger = logging.getLogger('pgsourcing')	
	logger.info("updating, proj:%s  oper:%s" % (p_proj,p_oper))

	conns = None
	connkey = None
	ret_changed = "None" # Valor de retorno nao esta a ser usado
	
	upd_ids_list = []
	if not updates_ids is None:

		flagv = isinstance(updates_ids, str)
		if flagv:
			upd_ids_list = process_intervals_string(updates_ids)
		elif isinstance(updates_ids, list):
			upd_ids_list = updates_ids

	limkeys_list = []
	if not limkeys is None:
		limkeys_list.extend(re.split("[ \,;]+", limkeys))

	now_dt = dt.now()
	base_ts = now_dt.strftime('%Y%m%dT%H%M%S')

	cfgpath = get_conn_cfg_path(p_proj)
	conns = Connections(cfgpath, subkey="conn")

	if p_connkey is None:
		if not conns.checkConn("dest"):
			raise RuntimeError("default 'dest' connection not found, need to pass connection key to use")
		else:
			connkey = 'dest'
	else:	
		connkey = p_connkey		
	
	if p_oper == "updref":

		assert not diffdict is None
		# print("diffdict:", json.dumps(diffdict, indent=4))
				
		newref_dict = updateref(p_proj, diffdict, upd_ids_list, limkeys_list)
		if not newref_dict is None:
		
			newref_dict["timestamp"] = base_ts			
			newref_dict["project"] = p_proj		
			newref_dict["pgsourcing_output_type"] = "reference"	
			newref_dict["pgsourcing_storage_ver"] = STORAGE_VERSION		 	

			save_ref(p_proj, newref_dict, now_dt)

			logger.info("reference changed, proj:%s" % (p_proj,))

		else:

			logger.info(f"reference NOT changed, proj:{p_proj}")
		
	elif p_oper == "upddestscript":

		assert not diffdict is None
		
		out_sql_src = updatedb(diffdict, upd_ids_list, limkeys_list, delmode=delmode)			
		do_linesoutput(out_sql_src, output=output, interactive=canuse_stdout)

		logger.info("dest change script for proj. %s, %s" % (p_proj,output))
		
	elif p_oper == "upddestdirect":

		assert not diffdict is None
		
		output = StringIO()
		out_sql_src = updatedb(diffdict, upd_ids_list, limkeys_list, delmode=delmode, docomment=False)			
		#do_linesoutput(out_sql_src, output=output, interactive=False)		
		#script = output.getvalue()
		
		cnobj = conns.getConn(connkey)
		cn = cnobj.getConn()
		with cn.cursor() as cr:
			# execute_batch(cr, script, (), page_size=10)
			for ln in out_sql_src:
				if isinstance(ln, list):
					strcontent = '\n'.join(ln)
				else:
					strcontent = ln
				# print('0>>', strcontent, '<<||')
				try:
					cr.execute(strcontent)
				except:
					logger.error("--------- statement execution ---------")
					logger.error(strcontent)
					raise
		cn.commit()
			
		logger.info("direct dest change for proj. %s" % p_proj)
		
	elif p_oper == "fillparamdata":
		
		filters_cfg = get_filters_cfg(p_proj, connkey)
		reftablesdir = get_reftablesdir(p_proj)

		cnobj = conns.getConn(connkey)
		cn = cnobj.getConn()
		needscommit = False
		with cn.cursor() as cr:

			for sch in filters_cfg["parameterstables"].keys():	

				trschema = sch
				if "transformschema" in filters_cfg.keys():					
					if "tables" in filters_cfg["transformschema"]["types"]:
						for trans in filters_cfg["transformschema"]["trans"]:
							if trans["dest"] == sch:
								trschema = trans["src"]
								break

				tablecont_files = [ 
					{ "name": f.name, "path": f.path } 
					for f in 
						scandir(reftablesdir) 
					if f.is_file and f.name.lower().endswith(".copy")  
				]
				remove_indices = []
				for ti, tfile in enumerate(tablecont_files):

					tname = splitext(tfile["name"])[0]
					tschema, tbasename = tname.split('.')
					if not tschema == trschema:
						continue
					tfile["desttable"] = f"{sch}.{tbasename}"
					tfile["destschema"] = sch
					tfile["tablebasename"] = tbasename

					found = False
					for filter_patt_tname in filters_cfg["parameterstables"][sch]:
						mo = re.search(filter_patt_tname, tbasename)
						if not mo is None:
							found = True
							break
					if not found:
						remove_indices.append(ti)

				for idx in sorted(remove_indices, reverse=True):
					del tablecont_files[idx]

				for tfile in tablecont_files:
					try:

						cr.execute(f"delete from { tfile['desttable'] }")
						with codecs.open(tfile["path"], "r", "utf-8") as fp:
							cr.copy_from(fp, tfile["desttable"])
						needscommit = True

						sql = SQL["CHANGE_CURR_SEQVAL"].format(tfile["destschema"], tfile["tablebasename"])
						# print("sql:", sql)
						cr.execute(sql)

					except:
						cn.rollback()
						logger.exception("fillparamdata")
						raise

		if needscommit:			
			cn.commit()
			
	return ret_changed

def chkcode_handler(p_proj, p_outprocsdir, p_opordmgr, p_connkey=None, output=None, interactive=False):

	logger = logging.getLogger('pgsourcing')	
		
	cfgpath = get_conn_cfg_path(p_proj)
	srccodedir = None
	if not p_connkey is None:
		ck = p_connkey
	else:
		ck = "src"

	now_dt = dt.now()

	# Fetch procedures existing on source files
	base_ts = now_dt.strftime('%Y%m%dT%H%M%S')		
	srccodedir = get_srccodedir(cfgpath, ck)				
	if not srccodedir is None:
		
		try:
			check_dict = { "content": { } }
			procs_dict = None

			assert exists(srccodedir), "Missing source code dir: %s" % srccodedir			
			for r, d, fs in walk(srccodedir):
				for fl in fs:
					
					fll = fl.lower()
					if not fll.endswith(".sql"):
						continue
						
					frompath = path_join(r, fl)
					assert exists(frompath), f"missing source file: {frompath}"

					topath = path_join(p_outprocsdir, fll)
					# print("  topath:", topath)

					fname, ext = splitext(fll)
					revdict = {}
					reverse_proc_fname(fname, revdict)

					assert "procschema" in revdict.keys()
					assert "procname" in revdict.keys()

					try:
						with codecs.open(frompath, "r", "utf-8") as flA:
							srca = flA.read()
					except:
						raise RuntimeError("Read error on %s" % frompath)

					patt1 = "function[\s]+([^\)]+\))"
					patt2 = "\(([^\)]+)\)"

					mo = re.search(patt1, srca, re.I)
					function_complete_name = None
					args = None
					if not mo is None:
						function_complete_name = mo.group(1)
					else:
						raise RuntimeError(f"impossible to retrieve function name from source file: {fl}")
					
					assert not function_complete_name is None
					mo2 = re.search(patt2, function_complete_name, re.I)
					if not mo2 is None:
						args = mo2.group(1)

					if not args is None:
						args_str = args
					else:
						args_str = ""

					# print(f" function '{fl}' args: '{args_str}'")

					# if procedure doesn't exist in 'code' transient folder
					if not exists(topath):

						if procs_dict is None:
							check_dict["content"]["procedures"] = {}
							procs_dict = check_dict["content"]["procedures"]

						if not revdict["procschema"] in procs_dict.keys():
							procs_dict[revdict["procschema"]] = {}
						if not revdict["procname"] in procs_dict[revdict["procschema"]].keys():
							di = procs_dict[revdict["procschema"]][revdict["procname"]] = {
								"diffoper": "insert",
								"fname": fll,
								"args": args
							}
							p_opordmgr.setord(di)
						
					else: # if procedure DOES exist in 'code' transient folder, let's check if is equal to source (meaning it's equal in source database)
						try:
							with codecs.open(topath, "r", "utf-8") as flB:
								srcb = flB.read()
						except:
							raise RuntimeError("Read error on %s" % topath)
							
						listA = []
						listB = []						
						sources_to_lists(srca, srcb, listA, listB)	
						diff = [l.strip() for l in list(dodiff(listB, listA)) if l.strip()]

						if len(diff) > 0:

							if procs_dict is None:
								check_dict["content"]["procedures"] = {}
								procs_dict = check_dict["content"]["procedures"]

							if not revdict["procschema"] in procs_dict.keys():
								procs_dict[revdict["procschema"]] = {}
							if not revdict["procname"] in procs_dict[revdict["procschema"]].keys():
								di = procs_dict[revdict["procschema"]][revdict["procname"]] = {
									"diffoper": "update",
									"difflines": copy(diff),
									"fname": fll,
									"args": args
								}
								p_opordmgr.setord(di)

				break
				
			if check_dict["content"]:

				check_dict["project"] = p_proj
				check_dict["timestamp"] = base_ts
				check_dict["pgsourcing_output_type"] = "codediff"
				check_dict["pgsourcing_storage_ver"] = STORAGE_VERSION		
				do_output(check_dict, output=output, interactive=interactive, diff=True)

			else:

				logger.info("result chksrccode: NO DIFF (proj '%s')" % p_proj)

		except AssertionError as err:
			logger.exception("Source code dir exists test")


def updcode_handler(p_proj, p_diffdict, updates_ids=None, p_connkey=None, 
		delmode=None, canuse_stdout=False, simulupdcode=False):
			
	if simulupdcode and not canuse_stdout:
		raise RuntimeError("'simulupdcode' option requires access to shell interactivity (stdout)")
	
	logger = logging.getLogger('pgsourcing')	
		
	cfgpath = get_conn_cfg_path(p_proj)
	srccodedir = None
	if not p_connkey is None:
		ck = p_connkey
	else:
		ck = "src"

	cfgpath = get_conn_cfg_path(p_proj)
	conns = Connections(cfgpath, subkey="conn")		

	upd_ids_list = []
	if not updates_ids is None:

		flagv = isinstance(updates_ids, str)

		if flagv:
			upd_ids_list = process_intervals_string(updates_ids)
		elif isinstance(updates_ids, list):
			upd_ids_list = updates_ids

	if p_connkey is None:
	
		if not conns.checkConn("dest"):
			raise RuntimeError("default 'dest' connection not found, need to pass connection key to use")
		else:
			connkey = 'dest'

	else:	
		connkey = p_connkey	

	connobj = conns.getConn(connkey)
	
	# pdb.set_trace()

	# now_dt = dt.now()
	# base_ts = now_dt.strftime('%Y%m%dT%H%M%S')		
	srccodedir = get_srccodedir(cfgpath, ck)				
	if not srccodedir is None:
		
		assert "content" in p_diffdict.keys()
		assert "procedures" in p_diffdict["content"].keys()
		procs_dict = p_diffdict["content"]["procedures"]
		
		try:
			with connobj as con:
				
				cn = con.getConn()
				changed = False
				
				for sch in procs_dict.keys():
					for pname in procs_dict[sch].keys():
						diff_item = procs_dict[sch][pname]
						if len(upd_ids_list) < 1 or diff_item["operorder"] in upd_ids_list:	
							if diff_item["diffoper"] in ("insert", "update"):
								full_path = path_join(srccodedir, diff_item["fname"])
								assert exists(full_path)
								with codecs.open(full_path, "r", "utf-8") as fl:
									src = fl.read()
									if simulupdcode and canuse_stdout:
										logger.info("insert or update, in database, src of ", src[:60])
									else:
										
										dropcmd = f"DROP FUNCTION {sch}.{pname}({diff_item['args']})"
										drop_first = False 

										count = 0
										while True:

											count += 1
											if count > 2:
												raise RuntimeError(f"excessive cycling ({count} cycles) on updcode_handler, create function code execution block")

											with cn.cursor() as cr:

												try:
													if drop_first:
														cr.execute(dropcmd)
														drop_first = False
													cr.execute(src)
													changed = True
													break
												except InvalidFunctionDefinition:
													drop_first = True
													cn.rollback()
													# (continue)
												except:
													logger.error("file %s" % diff_item["fname"])
													raise

										logger.info("inserting script for proj. %s, %s.%s" % (p_proj,sch,pname))	

							elif diff_item["diffoper"] == "delete":
								if delmode == "DEL":
									fmt = "DROP FUNCTION %s"
								elif delmode == "CASCADE":
									fmt = "DROP FUNCTION %s CASCADE"
								else:
									fmt = None
								if not fmt is None:
									sqlstr = fmt % ("%s(%s)" % (diff_item["procname"], diff_item["args"]))
									if simulupdcode and canuse_stdout:
										logger.info("delete sqlstr", sqlstr)
									else:
										with cn.cursor() as cr:
											cr.execute(sqlstr)
											changed = True
										logger.info("deleting script for proj. %s, %s.%s" % (p_proj,sch,pname))	
										
				if changed:
					logger.info("commiting changes to scripts")
					cn.commit()
		
		except AssertionError as err:
			logger.exception("updcode_handler")
		
		
	
class OpOrderMgr(Singleton):
	
	def __init__(self):
		self.ord = 0
		
	def setord(self, p_dict):	
		# Se linha abaixo falhar, ord nao e' incrementado desnecessariamente	
		p_dict["operorder"] = self.ord + 1
		self.ord = self.ord + 1

def check_objtype(p_relkind, p_typestr):
	ret = False

	if p_relkind.lower() == 's' and p_typestr.lower() == 'sequences':
		ret = True
	elif p_relkind.lower() == 'r' and p_typestr.lower() == 'tables':
		ret = True
	elif p_relkind.lower() == 'm' and p_typestr.lower() == 'matviews':
		ret = True
	elif p_relkind.lower() == 'v' and p_typestr.lower() == 'views':
		ret = True
	elif p_relkind.lower() == 'f' and p_typestr.lower() == 'fortables':
		ret = True

	# print("check_objtype -- p_relkind, p_typestr:", p_relkind, p_typestr, ret)
		
	return ret
		
# Cleaning up unnecessary diff items and branches that become empty 
#  during cleaning
def erase_diff_item(p_diff_dict, p_grpkeys):

	# print("erase_diff_item:", p_diff_dict, p_grpkeys)
	logger = logging.getLogger('pgsourcing')	
	
	diff_dict = p_diff_dict
	last_key = p_grpkeys[-1]
	for k in p_grpkeys:
		if k != last_key:
			diff_dict = diff_dict[k]
		else:
			if k in diff_dict.keys():
				del diff_dict[k]
			# try:
			# 	del diff_dict[k]
			# except:
			# 	logger.error(f"** k: {k}, lastk: {last_key}, allkeys:{p_grpkeys}")
			# 	logger.error(f"** initial diff_dict keys: {p_diff_dict.keys()}")
			# 	logger.error(f"** diff_dict keys: {diff_dict.keys()}")
			# 	raise

	# Clean empty branches
	count = 0
	# max diff tree depth never bigger than 15
	while count < 15:
		
		outerbreak = True
		count += 1
		diff_dict = p_diff_dict
		
		for k in p_grpkeys:
			if k in diff_dict.keys():
				if len(diff_dict[k].keys()) == 0:
					del diff_dict[k]
					outerbreak = False
					break
				else:
					diff_dict = diff_dict[k]
				
		if outerbreak:
			break
				
# Check change dict ops, clearing redundancies and spurious (mostly incomplete) entries		
def checkCDOps(p_proj, p_cd_ops, p_connkey, p_diff_dict):

	def should_remove(p_isinsert, p_cr, p_op, p_pdiff_dict):

		grpkeys = p_op[1]
		if len(grpkeys) < 3:
			erase_diff_item(p_pdiff_dict, grpkeys)
			return

		if grpkeys[0] == "schemata":
			assert len(grpkeys) >= 2, f"grpkeys: {grpkeys}"
			sch = grpkeys[1]
			name = None
		else:
			try:
				sch, name = grpkeys[1:3]
			except ValueError as e:
				raise RuntimeError(f"checkCDOps, should_remove, invalid grpkeys: {grpkeys}") from e

		obj_exists = False
		ret = False
		_do_show = False

		if grpkeys[0] == "schemata":
			if len(grpkeys) == 2:
				p_cr.execute(SQL["SCHEMA_CHK"], (sch,))
				row = p_cr.fetchone()
				obj_exists = (row[0] == 1)
			else:
				obj_exists = not p_isinsert

		elif grpkeys[0] == "procedures":
			
			if not p_isinsert:
				obj_exists = False
				
			else:

				try:
					_phase_label, op_content = p_op
				except:
					logger = logging.getLogger('pgsourcing')	
					logger.error("p_op:", p_op)
					raise

				assert len(op_content) == 3, f"length of {op_content} != 3"

				# print("op_content:", op_content)
				# print("p_diff_dict:", json.dumps(p_diff_dict, indent=4))

				curr_dicttree_level = p_diff_dict
				for li in range(3):
					
					# if li == 2:
					# 	new_proc_dict = {curr_dicttree_level[k]["newvalue"]["procedure_name"]:curr_dicttree_level[k] for k in curr_dicttree_level.keys()}	
					# 	assert op_content[li] in new_proc_dict.keys(), f"{op_content[li]} not in {new_proc_dict.keys()}"
					# 	curr_dicttree_level = new_proc_dict[op_content[li]]
					# else:

					assert op_content[li] in curr_dicttree_level.keys(), f"{op_content[li]} not in {curr_dicttree_level.keys()}"
					curr_dicttree_level = curr_dicttree_level[op_content[li]]

				assert 'newvalue' in curr_dicttree_level.keys(), f"{'newvalue'} not in {curr_dicttree_level.keys()}"
				curr_dicttree_level = curr_dicttree_level['newvalue']

				assert 'args' in curr_dicttree_level.keys(), f"'args' not in {curr_dicttree_level.keys()}"
				assert 'return_type' in curr_dicttree_level.keys(), f"'return_type' not in {curr_dicttree_level.keys()}"

				args = curr_dicttree_level['args']
				return_type = curr_dicttree_level['return_type']

				p_cr.execute(SQL["PROC_CHECK"], (sch, name))
				row = p_cr.fetchone()
				if not row is None:
					obj_exists = (row[0] == args and row[1] == return_type)
			
		else:

			# repr de um índice
			# ['tables', 'devestagio', 'import_payshop', 'index', 'ix_estagio_import_payshop_index']

			already_tested = False
			if grpkeys[0] == "tables" and len(grpkeys) > 3 and grpkeys[3] == "index":

				sch = grpkeys[1]
				tabname = grpkeys[2]
				idxname = grpkeys[4]
				p_cr.execute(SQL["INDEX_CHECK"], (sch, tabname, idxname))
				row = p_cr.fetchone()
				if row[0] > 0:
					obj_exists = True
				already_tested = True

			elif grpkeys[0] == "tables" and len(grpkeys) > 3 and grpkeys[3] == "pkey":

				## No need to test existance of pkey, not itself an object e.g. like an inedex
				_do_show = True
				if not p_isinsert:
					obj_exists = True
				already_tested = True

			if not already_tested:

				if grpkeys[0] in ("tables", "sequences", "views", "matviews") and len(grpkeys) == 3:
			
					p_cr.execute(SQL["GENERIC_CHECK"], (sch, name))
					row = p_cr.fetchone()
					if not row is None:
						obj_exists = check_objtype(row[0], grpkeys[0])

		if _do_show:
			print("::1055:: p_isinsert, obj_exists:", p_isinsert, obj_exists)
		
		if (p_isinsert and obj_exists) or \
			(not p_isinsert and not obj_exists):
				ret = True
				
		if ret:
			erase_diff_item(p_pdiff_dict, grpkeys)
				

	cfgpath = get_conn_cfg_path(p_proj)
	conns = Connections(cfgpath, subkey="conn")
	
	cnobj = conns.getConn(p_connkey)	
	cn = cnobj.getConn()
	with cn.cursor() as cr:

		# print("p_cd_ops:", p_cd_ops)

		ops = p_cd_ops["insert"]
		for op in ops:
			should_remove(True, cr, op, p_diff_dict)

		ops = p_cd_ops["delete"]
		for op in ops:
			should_remove(False, cr, op, p_diff_dict)


def main(p_proj, p_oper, p_connkey, newgenprocsdir=None, output=None, inputf=None, 
		canuse_stdout=False, include_public=False, include_colorder=False, 
		updates_ids=None, limkeys=None, delmode=None, simulupdcode=False):
	
	opordmgr = OpOrderMgr()
	
	logger = logging.getLogger('pgsourcing')	
	
	# pp = pprint.PrettyPrinter()

	if p_oper == "dropref":
		if not canuse_stdout or input("dropping reference data, are you sure? (enter 'y' to drop, any other key to exit): ").lower() == "y":
			projnotempty = genprojectarchive(p_proj)
			if projnotempty:
				dropref(p_proj)
				logger.info("reference dropped, proj:%s" % (p_proj,))
			else:
				logger.info("empty reference, nothing to do on proj:%s" % (p_proj,))				
		return
	
	refcodedir = get_refcodedir(p_proj)
	destcodedir = get_destcodedir(p_proj)
	reftablesdir = get_reftablesdir(p_proj)
	
	check_dict = { }
	root_diff_dict = { "content": {} }
	#ordered_diffkeys = {}
	replacements = {}

	comparison_mode, connkey = check_oper_handler(p_proj, p_oper, 
		refcodedir, reftablesdir, destcodedir, check_dict, \
		replacements, p_connkey=p_connkey, include_public=include_public, 
		include_colorder=include_colorder)

	# print("check_dict:", p_oper, check_dict.keys())
	
	# Se a operacao for chksrc ou chkdest o dicionario check_dict sera 
	#  preenchido.

	if check_dict:
		
		now_dt = dt.now()
		
		base_ts = now_dt.strftime('%Y%m%dT%H%M%S')
		check_dict["project"] = p_proj
		check_dict["timestamp"] = base_ts
		check_dict["pgsourcing_output_type"] = "rawcheck"
		check_dict["pgsourcing_storage_ver"] = STORAGE_VERSION		 	

		root_diff_dict["project"] = p_proj
		root_diff_dict["timestamp"] = base_ts
		root_diff_dict["pgsourcing_output_type"] = "diff"
		root_diff_dict["pgsourcing_storage_ver"] = STORAGE_VERSION		 	
		
		do_compare = False
		if comparison_mode == "From SRC":
			
			if not exists_currentref(p_proj):
				
				ref_dict = {
					"pgsourcing_output_type": "reference",
					"pgsourcing_storage_ver": STORAGE_VERSION		 	
				}
				
				# Extrair warnings antes de gravar
				for k in check_dict:
					if not k.startswith("warning") and k != "pgsourcing_output_type":
						ref_dict[k] = check_dict[k]
				save_ref(p_proj, ref_dict, now_dt)

				logger.info("reference created, proj:%s" % (p_proj,))
				
			else:
				
				do_compare = True
				
			# Separar warnings para ficheiro proprio
			
			if "warnings" in check_dict.keys():
				wout_json = {
					"project": p_proj,
					"pgsourcing_output_type": "warnings_only",
					"pgsourcing_storage_ver": STORAGE_VERSION,	 	
					"timestamp": base_ts,
					"content": check_dict["warnings"] 
				}			
				save_warnings(p_proj, wout_json)	
			
		elif comparison_mode == "From REF":
			
			if not exists_currentref(p_proj):
				raise RuntimeError("Project '%s' has no reference data, 'chksrc' must be run first" % p_proj)
			else:
				do_compare = True
		
		cd_ops = { "insert": [], "delete": [] }
		if do_compare:

			comparing(p_proj, check_dict["content"], 
				comparison_mode, replacements, opordmgr, 
				root_diff_dict["content"], cd_ops)

			if replacements:
				root_diff_dict["transformschema"] = replacements
				
			# # Lista "crua" das operações
			# print(f"--------- conn-key: {connkey:20} ----------------------")
			# pp.pprint(cd_ops)
			# print("----------------------------------------------------------------")

			# pp.pprint(root_diff_dict["content"]["tables"]["estagio"]['import_stcp_nos'])

			if comparison_mode != "From SRC":
				checkCDOps(p_proj, cd_ops, connkey, root_diff_dict["content"])

		# print("::1200::")
		# pp.pprint(root_diff_dict["content"])
			
		## TODO: permanente - Verificacao final de coerencia - 
		## Sequencias - tipo da seq. == tipo do campo serial em que e usada, etc. -- DONE

		if "content" in root_diff_dict.keys() and root_diff_dict["content"]:

			# pp.pprint(root_diff_dict["content"]["tables"]["estagio"]['import_stcp_nos'])
			
			logger.info("result: DIFF FOUND (proj '%s')" % p_proj)
			out_json = deepcopy(root_diff_dict)
			out_json["pgsourcing_output_type"] = "diff"
			out_json["pgsourcing_storage_ver"] = STORAGE_VERSION		 	
			out_json["comparison_dir"] = comparison_mode
			out_json["connkey"] = connkey
			do_output(out_json, output=output, interactive=canuse_stdout, diff=True)
			
			if not newgenprocsdir is None:
				
				if not "procedures" in root_diff_dict["content"].keys():					
					
					logger.warning("No procedures to dump on '{}'".format(newgenprocsdir))
					
				else:
									
					if not exists(newgenprocsdir):
						makedirs(newgenprocsdir)
				
					for sch in root_diff_dict["procedures"].keys():
						for proc in root_diff_dict["procedures"][sch]:
							procel = root_diff_dict["procedures"][sch][proc]
							if PROC_SRC_BODY_FNAME in procel.keys():
								if "diffoper" in procel[PROC_SRC_BODY_FNAME].keys() and \
										procel[PROC_SRC_BODY_FNAME]["diffoper"] == "update":
									src = procel[PROC_SRC_BODY_FNAME]["newvalue"]
									with codecs.open(path_join(newgenprocsdir, "%s.%s.sql" % (sch, proc)), "w", "utf-8") as fl:
										fl.write(src)									
				
		else:
			
			logger.info("result: NO DIFF (proj '%s')" % p_proj)
			
	else: # if not check_dict

		diffdict = None
		if p_oper in OPS_INPUT:

			flagv = isinstance(inputf, str)
			if flagv:
				if exists(inputf):
					with open(inputf, "r") as fj:
						diffdict = json.load(fj)
			elif isinstance(inputf, file_types):
				if isinstance(inputf, StringIO):
					diffdict = json.loads(inputf.getvalue())
				else:
					diffdict = json.load(inputf)
				inputf.close()
				
		if delmode is None:
			dlmd = "NODEL"
		else:
			dlmd = 	delmode
		
		if p_oper in OPS_CODE:
			
			if p_oper == "chksrccode":

				chkcode_handler(p_proj, refcodedir, opordmgr, 
					p_connkey=p_connkey, output=output, 
					interactive=canuse_stdout)

			elif p_oper == "updcodeinsrc":
			
				updcode_handler(p_proj, diffdict, 
					updates_ids=updates_ids, p_connkey=p_connkey, 
					delmode=dlmd, canuse_stdout=canuse_stdout, simulupdcode=simulupdcode)
			
		else:

			# Se a operacao for updref ou chkdest o dicionario check_dict sera 
			#  preenchido.
			
			update_oper_handler(p_proj, p_oper,  
				diffdict, updates_ids=updates_ids, p_connkey=p_connkey, 
				limkeys=limkeys, include_public=include_public, 
				include_colorder=include_colorder, output=output, 
				canuse_stdout=canuse_stdout, delmode=dlmd)
					
	
# ######################################################################

def gen_newprocfile_items():

	ret = None
	prlist = [
		"New procedure schema (enter 'x' or blank to terminate):",
		"New procedure name:",
		"Return type:",
		"Owner:"
		]
		
	prfinal = "data type of argument #%s (enter 'x' or blank to terminate):"
	
	doexit = False
	tiposargs = []
	sch = None
	nome = None
	rettipo = None
	ownership = None
			
	for pri, pr in enumerate(prlist):		
		resp = input(pr)
		if len(resp) < 1 or resp.lower() == 'x':
			doexit = True
			break
		if pri == 0:
			sch = resp.strip().lower()
		elif pri == 1:
			nome = resp.strip().lower()
		elif pri == 2:
			rettipo = resp.strip().lower()
		elif pri == 3:
			ownership = resp.strip().lower()
			
	if not doexit:

		count = 0
		while count <= 20:
			count += 1
			resp = input(prfinal % count)
			if len(resp) < 1 or resp.lower() == 'x':
				break
			tiposargs.append(resp.strip().lower())
	
		fname = gen_proc_fname(nome, rettipo, tiposargs)
		ret = [fname, sch, nome, rettipo, tiposargs, ownership] 

	return ret

def gen_newtrigger_items():

	logger = logging.getLogger('pgsourcing')		

	ret = None
	prlist = [
		"Table schema (enter 'x' or blank to terminate): ",
		"Table name: ",
		"Trigger suffix (after table name): ",
		"Is BEFORE trigger? (y/other): ",
		"Is on INSERT? (y/other): ",
		"Is on UPDATE? (y/other): ",
		"Is on DELETE? (y/other): ",
		"Is ROW trigger (y/other): ",
		"procedure name (with schema): "
		]
		
	doexit = False
	tsch = None
	tnome = None
	suffix = None
	is_before = None
	is_insert = None
	is_update = None
	is_delete = None
	is_row = None
	proc_name = None
			
	for pri, pr in enumerate(prlist):		
		resp = input(pr)
		if len(resp) < 1 or resp.lower() == 'x':
			doexit = True
			break
		if pri == 0:
			tsch = resp.strip().lower()
		elif pri == 1:
			tnome = resp.strip().lower()
		elif pri == 2:
			suffix = resp.strip().lower()
		elif pri == 3:
			is_before = resp.strip().lower()
		elif pri == 4:
			is_insert = resp.strip().lower()
		elif pri == 5:
			is_update = resp.strip().lower()
		elif pri == 6:
			is_delete = resp.strip().lower()
		elif pri == 7:
			is_row = resp.strip().lower()
		elif pri == 8:
			proc_name = resp.strip().lower()
			count = 0
			while not '.' in proc_name:
				count += 1
				if count > 3:
					doexit = True
					break
				logger.warning(f"gen_newtrigger_items, '{proc_name}' has no schema (dot separated)")
				resp = input(pr)
				proc_name = resp.strip().lower()
			
	if not doexit:

		ret = [tsch, tnome, suffix, is_before, is_insert, is_update, is_delete, is_row, proc_name] 

	return ret

def addnewtrigger(p_proj, conn=None, conf_obj=None):
	
	out_buf = []

	logger = logging.getLogger('pgsourcing')		

	newitems = None
	if conf_obj is None:
		## Accesses stdin and stdout to query user
		newitems = gen_newtrigger_items()

	if newitems is None:
		
		logger.info("Nothing to do, new trigger creation terminated")

	else:

		if conf_obj is None:
			tsch, tnome, suffix, is_before, is_insert, is_update, is_delete, is_row, proc_name = newitems		
		else:
			tsch = conf_obj["tableschema"]
			tnome = conf_obj["tablename"]
			is_before = conf_obj["isbefore"]
			is_insert = conf_obj["isinsert"]
			is_update = conf_obj["isupdate"]
			is_delete = conf_obj["isdelete"]
			is_row = conf_obj["isrow"]
			proc_name = conf_obj["proc_name"]

		assert '.' in proc_name
		proc_name = proc_name.replace("(", "")
		proc_name = proc_name.replace(")", "")

		trig_name = f"{tnome}_{suffix}"

		if is_before == "y":
			when_str = "BEFORE"
		else:
			when_str = "AFTER"

		event_buf = []
		if is_insert == "y":
			event_buf.append("INSERT")
		if is_update == "y":
			event_buf.append("UPDATE")
		if is_delete == "y":
			event_buf.append("DELETE")

		evt_line = f"{when_str} {' OR '.join(event_buf)}"

		out_buf.append(f"CREATE TRIGGER {trig_name}")
		out_buf.append(evt_line)
		out_buf.append(f"ON {tsch}.{tnome}")
		if is_row  == "y":
			out_buf.append("FOR EACH ROW")
		else:
			out_buf.append("FOR EACH STATEMENT")
		out_buf.append(f"EXECUTE PROCEDURE {proc_name}()")

	trig_sql = "\n".join(out_buf)

	cfgpath = get_conn_cfg_path(p_proj)
	if not conn is None:
		connkey = conn
	else:
		connkey = "src"

	conns = Connections(cfgpath, subkey="conn")

	cnobj = conns.getConn(connkey)
	cn = cnobj.getConn()
	with cn.cursor() as cr:

		try:
			cr.execute(trig_sql)
		except:
			logger.error("--------- statement execution ---------")
			logger.error(trig_sql)
			raise
	cn.commit()
		
	logger.info(f"trigger '{trig_name}' created, on table '{tsch}.{tnome}', in source database, proj: {p_proj}")



	
def addnewprocedure_file(p_proj, conn=None, conf_obj=None):

	logger = logging.getLogger('pgsourcing')		
	
	newitems = None
	if conf_obj is None:
		## Accesses stdin and stdout to query user
		newitems = gen_newprocfile_items()
		
	if newitems is None:
		
		logger.info("Nothing to do, new procedure creation terminated")
		
	else:
	
		cfgpath = get_conn_cfg_path(p_proj)
		srccodedir = None
		newfname = None
		if not conn is None:
			ck = conn
		else:
			ck = "src"
			
		srccodedir = get_srccodedir(cfgpath, ck)	
		fullenewpath = None			
		if not srccodedir is None:
			
			try:
				assert exists(srccodedir), "addnewprocedure_file, missing source code dir: %s" % srccodedir	
				
				if conf_obj is None:
					newfname, sch, nome, rettipo, tiposargs, ownership = newitems		
				else:
					newfname = conf_obj["newfname"]
					sch = conf_obj["sch"]
					nome = conf_obj["nome"]
					rettipo = conf_obj["rettipo"]
					tiposargs = conf_obj["tiposargs"]
					ownership = conf_obj["ownership"]
					
				complnewname = "%s.%s" % (sch, newfname) 
				complfname = "%s.sql" % (complnewname,) 
					
				fullenewpath = path_join(srccodedir, complfname)				
				if exists(fullenewpath):					
					logger.info("addnewprocedure_file, cannot create new procedure in '%s': filename in use -- %s" % (p_proj, complfname))
				else:
					sql_linebuffer = []
					argslist = ["%s %s" % (chr(97+ti), ta) for ti, ta in enumerate(tiposargs)] 
					args = ", ".join(argslist)
					create_function_items(sch, nome, args, rettipo, "plpgsql", ownership, "v", 
						"DECLARE\n\tv_null integer;\nBEGIN\n\n\tv_null := 0;\n\tRETURN null;\n\nEND;", sql_linebuffer)
						
					if len(sql_linebuffer) > 0:
						with codecs.open(fullenewpath, "w", "utf-8") as newfl:
							newfl.write("%s;" % "".join(sql_linebuffer))
						
			except AssertionError as err:
				logger.exception("addnewprocedure_file, source code dir test")
		
		if not fullenewpath is None:
			
			logger.info("New procedure file created: %s" % complnewname)
			
			# pr = "Want to open with default system editor? (y/n)"
			# try:
				# resp = raw_input(pr)
			# except NameError:
				# resp = input(pr)
			# if resp.lower() == 's':
				# check_call(['start', fullenewpath], shell=True)


def cli_main():

	# Config
	check_filesystem()
	
	log_setup(tosdout=True)	
	logger = logging.getLogger('pgsourcing')		
		
	# Bootstrap
	try:
		args, proj = parse_args()
		if args.setup:
			gen_setup_zip(SETUP_ZIP)
		elif not args.newproj is None:
			create_new_proj(args.newproj)
		else:
			assert not proj is None
			
			if args.addnewproc:
				## conf_obj=None forces interaction with stdin and stdout
				addnewprocedure_file(proj, conn=args.connkey, conf_obj=None)				
			elif args.addnewtrig:
				## conf_obj=None forces interaction with stdin and stdout
				addnewtrigger(proj, conn=args.connkey, conf_obj=None)				
			else:

				if args.oper in OPS_INPUT and args.input is None:	

					if not args.oper in OPS_PRECEDENCE.keys():
						raise RuntimeError("Operation '%s' requires input file given using -i option" % args.oper)

					operitem = args.oper
					operlist = [args.oper]
					streams = []
					while operitem in OPS_PRECEDENCE.keys():					
						operitem = OPS_PRECEDENCE[operitem]
						operlist.insert(0, operitem)

					for oi, operitem in enumerate(operlist):

						if operitem in OPS_CHECK:						
							streams = [args.input, StringIO()]
						else:

							if operitem in OPS_INPUT:
								if len(streams) > 0:
									if not streams[0] is None:
										streams[0].close()
									del streams[0]

							if operitem in OPS_OUTPUT:
								if args.output is None:
									streams.append(StringIO())
								else:
									streams.append(args.output)

						if len(streams) < 2:
							streams.append(args.output)

						logger.info("operation:{}, streams input:{}, output:{}".format(operitem, streams[0], streams[1]))

						main(proj, operitem, args.connkey, args.genprocsdir, 
								output=streams[1], inputf=streams[0], 
								canuse_stdout=True, 
								include_public=args.includepublic, 
								include_colorder = not args.removecolorder,
								updates_ids = args.updateids,
								limkeys = args.limkeys,
								delmode = args.delmode,
								simulupdcode = args.simulupdcode)

						if oi < len(operlist)-1:
							
							assert streams[1].tell() > 0, "Nothing to do on op '{} - {}', prev result is empty".format(oi+1, operitem)
							
							# print("----------------------------------->")
							# print(streams[1].getvalue()[:300])
							# print("->--------------------------------||")


				else:   # NOT args.oper in OPS_INPUT OR NOT args.input is None

					main(proj, args.oper, args.connkey, args.genprocsdir, 
							output=args.output, inputf=args.input, 
							canuse_stdout=True, 
							include_public=args.includepublic, 
							include_colorder = not args.removecolorder,
							updates_ids = args.updateids,
							limkeys = args.limkeys,
							delmode = args.delmode,
							simulupdcode = args.simulupdcode)
					
	except:
		logger.exception("")
	

if __name__ == "__main__":
	cli_main()



