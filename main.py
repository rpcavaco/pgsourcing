# -*- coding: utf-8 -*-

from __future__ import print_function
from os import listdir, mkdir, makedirs, walk, remove as removefile
from os.path import abspath, dirname, exists, splitext, join as path_join
from datetime import datetime as dt
from copy import deepcopy
from difflib import unified_diff as dodiff
from subprocess import check_call


from psycopg2.extras import execute_batch

import argparse
import logging
import logging.config
import json
import codecs
import re
import io


from src.common import LOG_CFG, LANG, OPS, OPS_CONNECTED, OPS_INPUT, \
		OPS_OUTPUT, OPS_HELP, OPS_CHECK, SETUP_ZIP, BASE_CONNCFG, \
		BASE_FILTERS_RE, PROC_SRC_BODY_FNAME, STORAGE_VERSION
		
from src.read import srcreader, gen_proc_fname, reverse_proc_fname
from src.connect import Connections
from src.compare import comparing, keychains, sources_to_lists
from src.zip import gen_setup_zip
from src.fileandpath import get_conn_cfg_path, get_filters_cfg, \
		exists_currentref, to_jsonfile, save_ref, get_refcodedir, \
		save_warnings, clear_dir, get_srccodedir
from src.write import updateref, updatedb, create_function_items

try:
	from StringIO import StringIO
except ImportError:
	from io import StringIO

try:
    file_types = (io.IOBase,)
except NameError:
    file_types = (file, StringIO)
    
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
	
	parser = argparse.ArgumentParser(description='Diff e migracao de definicoes de base de dados PostgreSQL')
	
	projdir = path_join(dirname(abspath(__file__)), 'projetos')	
	projetos = listdir(projdir)
	
	ops_help = ["%s: %s" % (op, OPS_HELP[LANG][op]) for op in OPS]
	ops_input = ",".join(OPS_INPUT)
	
	parser.add_argument("proj", nargs="?", action="store", help="Indique um projeto de entre estes: %s" % str(projetos))
	parser.add_argument("oper", nargs="?", action="store", help="Indique uma operacao de entre estas: %s" % str(ops_help))
	parser.add_argument("-o", "--output", help="Ficheiro de saida",
                    action="store")

	parser.add_argument("-i", "--input", help="Ficheiro de entrada (OBRIGATORIO com 'oper' %s)" % ops_input, action="store")
	parser.add_argument("-c", "--connkey", help="Chave da ligacao de base de dados (por defeito 'src' se op for 'chksrc')", action="store")
	parser.add_argument("-s", "--setup", help="Apenas criar ZIP de setup", action="store_true")
	parser.add_argument("-n", "--newproj", help="Apenas criar novo projeto vazio", action="store")
	parser.add_argument("-p", "--includepublic", help="Incluir schema public", action="store_true")
	parser.add_argument("-r", "--removecolorder", help="Remover ordenacao das colunas nas tabelas", action="store_true")
	parser.add_argument("-g", "--genprocsdir", help="Gerar sources dos procedimentos, indicar pasta a criar", action="store")
	parser.add_argument("-d", "--opsorder", help="Lista de operacoes (sequencia de oporder) a efetuar", action="store")
	parser.add_argument("-k", "--limkeys", help="Filtro de atributios a alterar: apenas estes atributos serao alterados", action="store")
	parser.add_argument("-m", "--delmode", help="Modo apagamento: NODEL (default), DEL, CASCADE", action="store")
	parser.add_argument("-a", "--addnweproc", help="Gerar novo ficheiro de procedure", action="store_true")
	parser.add_argument("-t", "--addnewtrig", help="Gerar novo ficheiro de trigger", action="store_true")
	parser.add_argument("-u", "--simulupdcode", help="Simular atualizacao codigo", action="store_true")
	
	args = parser.parse_args()
	
	logger = logging.getLogger('pgsourcing')	
	logger.info("Inicio, args: %s" % args)
	
	mutexflags = set()
	if args.setup:
		mutexflags.add("setup")
	if not args.newproj is None:
		mutexflags.add("newproj")
	
	if len(mutexflags) > 1:
		parser.print_help()
		raise RuntimeError("opcoes -s e -n sao mutuamente exclusivas")
	
	proj = None
	if len(mutexflags) == 0:
		if args.proj is None:
			parser.print_help()
			raise RuntimeError("A indicacao de projeto (proj) obrigatoria exceto nas opcoes -s e -n")
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
				raise RuntimeError("Projeto '%s' nao existe, projetos encontrados: %s" % (args.proj, str(projetos)))
				
		if not args.addnweproc and not args.oper in OPS:
			raise RuntimeError("Operacao '%s' invalida, ops disponiveis: %s" % (args.oper, str(ops_help)))

		if args.oper in OPS_INPUT and args.input is None:
			parser.print_help()
			raise RuntimeError("Operacao '%s' exige indicacao ficheiro de entrada com opcao -i" % args.oper)

		if args.oper in OPS_OUTPUT and args.output is None:
			parser.print_help()
			raise RuntimeError("Operacao '%s' exige indicacao ficheiro de saida com opcao -o" % args.oper)
			
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
			if exists(output) and interactive:
				prompt = "Ficheiro de saida existe, sobreescrever ? (s/n)"
				try:
					resp = raw_input(prompt)
				except NameError:
					resp = input(prompt)
				if resp.lower() == 's':
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
					prompt = "Ficheiro de saida existe, sobreescrever ? (s/n)"
					try:
						resp = raw_input(prompt)
					except NameError:
						resp = input(prompt)
					if resp.lower() == 's':
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

def check_oper_handler(p_proj, p_oper, p_outprocsdir, o_checkdict, o_replaces, p_connkey=None, include_public=False, include_colorder=False):
	
	logger = logging.getLogger('pgsourcing')	
	
	ret = "None"

	conns = None
	connkey = None
	
	if p_oper in OPS_CHECK:
	
		if p_oper in OPS_CONNECTED:

			cfgpath = get_conn_cfg_path(p_proj)
			conns = Connections(cfgpath, subkey="conn")
			
			if p_connkey is None:
			
				if p_oper == "chksrc":

					if not conns.checkConn("src"):
						raise RuntimeError("Chksource: connection identificada com 'src' nao existe, necessario indicar chave respetiva")
					else:
						connkey = 'src'

				elif p_oper == "chkdest":

					if not conns.checkConn("dest"):
						raise RuntimeError("Chksource: connection identificada com 'dest' nao existe, necessario indicar chave respetiva")
					else:
						connkey = 'dest'

			else:	
				connkey = p_connkey		
		
		if p_oper == "chksrc":

			logger.info("checking, proj:%s  oper:%s" % (p_proj,p_oper))
			outprocs_dir = p_outprocsdir
			ret = "From SRC"
			
		elif p_oper == "chkdest":
			
			logger.info("checking, proj:%s  oper:%s" % (p_proj,p_oper))
			outprocs_dir = None
			ret = "From REF"
		
		if not connkey is None:
			
			filters_cfg = get_filters_cfg(p_proj, connkey)

			o_replaces.clear()
			if "transformschema" in filters_cfg.keys():
				o_replaces.update(filters_cfg["transformschema"])

			srcreader(conns.getConn(connkey), filters_cfg, o_checkdict, outprocs_dir=outprocs_dir, include_public=include_public, include_colorder=include_colorder)
		
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
	
def update_oper_handler(p_proj, p_oper, p_opordermgr, diffdict, 
		updates_ids=None, p_connkey=None, limkeys=None, 
		include_public=False, include_colorder=False,
		output=None, canuse_stdout=False, delmode="NODEL"):

	# delmode: NODEL DEL CASCADE 
	
	# updates_ids - se None ou lista vazia, aplicar todo o diffdict
	
	logger = logging.getLogger('pgsourcing')	
	logger.info("updating, proj:%s  oper:%s" % (p_proj,p_oper))

	conns = None
	connkey = None
	ret_changed = "None"
	
	upd_ids_list = []
	if not updates_ids is None:

		try:
			flagv = isinstance(updates_ids, basestring)
		except NameError:
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

	assert not diffdict is None

	if p_oper in OPS_CONNECTED:
		
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
				
		newref_dict = updateref(p_proj, diffdict, upd_ids_list, limkeys_list)
		if not newref_dict is None:
		
			newref_dict["timestamp"] = base_ts			
			newref_dict["project"] = p_proj		
			newref_dict["pgsourcing_output_type"] = "reference"	
			newref_dict["pgsourcing_storage_ver"] = STORAGE_VERSION		 	

			save_ref(p_proj, newref_dict, now_dt)

			logger.info("reference changed, proj:%s" % (p_proj,))
		
	elif p_oper == "upddest":
		
		out_sql_src = updatedb(p_proj, diffdict, upd_ids_list, limkeys_list, delmode=delmode)			
		do_linesoutput(out_sql_src, output=output, interactive=canuse_stdout)

		logger.info("dest change script for proj. %s, %s" % (p_proj,output))
		
	elif p_oper == "upddir":
		
		output = StringIO.StringIO()
		out_sql_src = updatedb(p_proj, diffdict, upd_ids_list, limkeys_list, delmode=delmode, docomment=False)			
		#do_linesoutput(out_sql_src, output=output, interactive=False)		
		#script = output.getvalue()
		
		cnobj = conns.getConn(connkey)
		cn = cnobj.getConn()
		with cn.cursor() as cr:
			# execute_batch(cr, script, (), page_size=10)
			for ln in out_sql_src:
				print('>>', ln)
				cr.execute(ln)
		cn.commit()
			
		logger.info("direct dest change for proj. %s" % p_proj)

		
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

	base_ts = now_dt.strftime('%Y%m%dT%H%M%S')		
	srccodedir = get_srccodedir(cfgpath, ck)				
	if not srccodedir is None:
		
		try:
			check_dict = { "content": {} }

			assert exists(srccodedir), "Missing source code dir: %s" % srccodedir			
			for r, d, fs in walk(srccodedir):
				for fl in fs:
					
					fll = fl.lower()
					if not fll.endswith(".sql"):
						continue
						
					frompath = path_join(r, fl)
					topath = path_join(p_outprocsdir, fll)

					fname, ext = splitext(fll)
					revdict = {}
					reverse_proc_fname(fname, revdict)

					assert "procschema" in revdict.keys()
					assert "procname" in revdict.keys()

					if not exists(topath):
						
						if not revdict["procschema"] in check_dict["content"].keys():
							check_dict["content"][revdict["procschema"]] = {}
						if not revdict["procname"] in check_dict["content"][revdict["procschema"]].keys():
							di = check_dict["content"][revdict["procschema"]][revdict["procname"]] = {
								"diffoper": "insert",
								"fname": fll
							}
							p_opordmgr.setord(di)
						
					else:
					
						with codecs.open(frompath, "r", "utf-8") as flA:
							srca = flA.read()
						with codecs.open(topath, "r", "utf-8") as flB:
							srcb = flB.read()

						listA = []
						listB = []						
						sources_to_lists(srca, srcb, listA, listB)						
						diff = [l.strip() for l in list(dodiff(listA, listB)) if l.strip()]

						if len(diff) > 0:

							if not revdict["procschema"] in check_dict["content"].keys():
								check_dict["content"][revdict["procschema"]] = {}
							if not revdict["procname"] in check_dict["content"][revdict["procschema"]].keys():
								di = check_dict["content"][revdict["procschema"]][revdict["procname"]] = {
									"diffoper": "update",
									"difflines": copy(diff),
									"fname": fll
								}
								p_opordmgr.setord(di)

				break
				
			# reverse			
			for r, d, fs in walk(p_outprocsdir):
				for fl in fs:
					
					fll = fl.lower()
					if not fll.endswith(".sql"):
						continue

					frompath = path_join(r, fl)
					topath = path_join(srccodedir, fll)

					fname, ext = splitext(fll)
					revdict = {}
					reverse_proc_fname(fname, revdict)

					assert "procschema" in revdict.keys()
					assert "procname" in revdict.keys()
					
					if revdict["procschema"] == "":
						sch = "public"
					else:
						sch = revdict["procschema"]

					if not exists(topath):
						
						with codecs.open(frompath, "r", "utf-8") as open_fl:
							src = open_fl.read()
							m = re.match("[\s]?(CREATE|CREATE[\s]+OR[\s]+REPLACE)[\s]+FUNCTION[\s]+[^\(]+\s?\(\)", src, re.MULTILINE)
							args = ""
							if m is None:
								m2 = re.search("[\s]?(CREATE|CREATE[\s]+OR[\s]+REPLACE)[\s]+FUNCTION[\s]+[^\(]+\s?\(([^\)]+)\)", src, re.MULTILINE)
								if not m2 is None:
									args = m2.group(2).strip()									

						if not sch in check_dict["content"].keys():
							check_dict["content"][sch] = {}
						if not revdict["procname"] in check_dict["content"][sch].keys():
							di = check_dict["content"][sch][revdict["procname"]] = {
								"diffoper": "delete",
								"filename": fll,
								"procname": ".".join((sch, revdict["procname"])),
								"args": args
							}
							p_opordmgr.setord(di)
			
			if check_dict["content"]:

				check_dict["project"] = p_proj
				check_dict["timestamp"] = base_ts
				check_dict["pgsourcing_output_type"] = "codediff"
				check_dict["pgsourcing_storage_ver"] = STORAGE_VERSION		
				do_output(check_dict, output=output, interactive=interactive, diff=True)

		except AssertionError as err:
			logger.exception("Source code dir test")


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

		try:
			flagv = isinstance(updates_ids, basestring)
		except NameError:
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

	# now_dt = dt.now()
	# base_ts = now_dt.strftime('%Y%m%dT%H%M%S')		
	srccodedir = get_srccodedir(cfgpath, ck)				
	if not srccodedir is None:
		
		assert "content" in p_diffdict.keys()
		
		try:
			with connobj as con:
				
				cn = con.getConn()
				changed = False
				
				for sch in p_diffdict["content"].keys():
					for pname in p_diffdict["content"][sch].keys():
						diff_item = p_diffdict["content"][sch][pname]
						if len(upd_ids_list) < 1 or diff_item["operorder"] in upd_ids_list:	
							if diff_item["diffoper"] in ("insert", "update"):
								full_path = path_join(srccodedir, diff_item["fname"])
								assert exists(full_path)
								with codecs.open(full_path, "r", "utf-8") as fl:
									src = fl.read()
									if simulupdcode and canuse_stdout:
										print("insert or update src of ", src[:60])
									else:
										with cn.cursor() as cr:
											cr.execute(src)
											changed = True
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
										print("delete sqlstr", sqlstr)
									else:
										with cn.cursor() as cr:
											cr.execute(sqlstr)
											changed = True
										logger.info("deleting script for proj. %s, %s" % (p_proj,sch,pname))	
										
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

	
def main(p_proj, p_oper, p_connkey, newgenprocsdir=None, output=None, inputf=None, 
		canuse_stdout=False, include_public=False, include_colorder=False, 
		updates_ids=None, limkeys=None, delmode=None, simulupdcode=False):
	
	opordmgr = OpOrderMgr()
	
	logger = logging.getLogger('pgsourcing')	
	
	refcodedir = get_refcodedir(p_proj)
	
	check_dict = { }
	root_diff_dict = { "content": {} }
	#ordered_diffkeys = {}
	replaces = {}
	
	comparison_mode, connkey = check_oper_handler(p_proj, p_oper, refcodedir, check_dict, \
		replaces, p_connkey=p_connkey, include_public=include_public, 
		include_colorder=include_colorder)
	
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
				raise RuntimeError("Referencia do projeto '%s' em falta, necessario correr chksrc primeiro" % p_proj)
			else:
				do_compare = True
		
		if do_compare:

			comparing(p_proj, check_dict["content"], 
				comparison_mode, replaces, opordmgr, root_diff_dict["content"])
						
		## TODO - deve haver uma verificacao final de coerencia
		## Sequencias - tipo da seq. == tipo do campo serial em que e usada
		
		if "content" in root_diff_dict.keys() and root_diff_dict["content"]:
			
			logger.info("result: DIFF FOUND (proj '%s')" % p_proj)
			out_json = deepcopy(root_diff_dict)
			out_json["pgsourcing_output_type"] = "diff"
			out_json["pgsourcing_storage_ver"] = STORAGE_VERSION		 	
			out_json["comparison_dir"] = comparison_mode
			out_json["connkey"] = connkey
			do_output(out_json, output=output, interactive=canuse_stdout, diff=True)
			
			if not newgenprocsdir is None:
				
				if not "procedures" in root_diff_dict["content"].keys():					
					
					logger.warning("Sem procedimentos para colocar em '%s'" % newgenprocsdir)
					
				else:
									
					if not exists(newgenprocsdir):
						makedirs(newgenprocsdir)
				
					for sch in diff_dict["procedures"].keys():
						for proc in diff_dict["procedures"][sch]:
							procel = diff_dict["procedures"][sch][proc]
							if PROC_SRC_BODY_FNAME in procel.keys():
								if "diffoper" in procel[PROC_SRC_BODY_FNAME].keys() and \
										procel[PROC_SRC_BODY_FNAME]["diffoper"] == "update":
									src = procel[PROC_SRC_BODY_FNAME]["newvalue"]
									with codecs.open(path_join(newgenprocsdir, "%s.%s.sql" % (sch, proc)), "w", "utf-8") as fl:
										fl.write(src)									
				
		else:
			
			logger.info("result: NO DIFF (proj '%s')" % p_proj)
			# do_output(check_dict, output=output, interactive=canuse_stdout)
			
	else:

		diffdict = None
		if p_oper in ("updcode", "updref", "upddest", "upddir"):

			try:
				flagv = isinstance(inputf, basestring)
			except NameError:
				flagv = isinstance(inputf, str)

			if flagv:
				if exists(inputf):
					with open(inputf, "r") as fj:
						diffdict = json.load(fj)
			elif isinstance(inputf, file_types):
				diffdict = json.load(inputf)
				
			if delmode is None:
				dlmd = "NODEL"
			else:
				dlmd = 	delmode
				
		
		if p_oper in ("chkcode", "updcode"):
			
			if p_oper == "chkcode":

				chkcode_handler(p_proj, refcodedir, opordmgr, 
					p_connkey=p_connkey, output=output, 
					interactive=canuse_stdout)

			elif p_oper == "updcode":
			
				updcode_handler(p_proj, diffdict, 
					updates_ids=updates_ids, p_connkey=p_connkey, 
					delmode=dlmd, canuse_stdout=canuse_stdout, simulupdcode=simulupdcode)
			
		else:

			# Se a operacao for updref ou chkdest o dicionario check_dict sera 
			#  preenchido.

			update_oper_handler(p_proj, p_oper, opordmgr, 
				diffdict, updates_ids=updates_ids, p_connkey=p_connkey, 
				limkeys=limkeys, include_public=include_public, 
				include_colorder=include_colorder, output=output, 
				canuse_stdout=canuse_stdout, delmode=dlmd)
					
	
# ######################################################################

def gen_newprocfile_items():

	ret = None
	prlist = [
		"Schema do novo procedimento (x ou vazio para sair sem criar):",
		"Nome do novo procedimento:",
		"Tipo de dados de retorno:",
		"Login proprietario:"
		]
		
	prfinal = "tipo de dados %so. argumento (x ou vazio para terminar):"
	
	doexit = False
	tiposargs = []
	sch = None
	nome = None
	rettipo = None
	ownership = None
			
	for pri, pr in enumerate(prlist):		
		try:
			resp = raw_input(pr)
		except NameError:
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
			try:
				resp = raw_input(prfinal % count)
			except NameError:
				resp = input(prfinal % count)
			if len(resp) < 1 or resp.lower() == 'x':
				break
			tiposargs.append(resp.strip().lower())
	
		fname = gen_proc_fname(sch, nome, rettipo, tiposargs)
		ret = [fname, sch, nome, rettipo, tiposargs, ownership] 
		
	return ret

def addnewtrigger_file(p_proj, conn=None, conf_obj=None):
	
	## TODO
	raise NotImplementedError

	
def addnewprocedure_file(p_proj, conn=None, conf_obj=None):

	logger = logging.getLogger('pgsourcing')		
	
	newitems = None
	if conf_obj is None:
		## Accesses stdin and stdout to query user
		newitems = gen_newprocfile_items()
		
	if newitems is None:
		
		logger.info("New procedure creation terminated")
		
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
					
				fullenewpath = path_join(srccodedir, newfname)
				
				if exists(fullenewpath):
					
					logger.info("addnewprocedure_file, cannot create new procedure in '%s': filename in use -- %s" % (proj, fname))
					
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
			
			logger.info("New procedure file created: %s" % newfname)
			
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
			
			if args.addnweproc:
				## conf_obj=None forces interaction with stdin and stdout
				addnewprocedure_file(proj, conn=args.connkey, conf_obj=None)				
			elif args.addnewtrig:
				## conf_obj=None forces interaction with stdin and stdout
				addnewtrigger_file(proj, conn=args.connkey, conf_obj=None)				
			else:			
				main(proj, args.oper, args.connkey, args.genprocsdir, 
						output=args.output, inputf=args.input, 
						canuse_stdout=True, 
						include_public=args.includepublic, 
						include_colorder = not args.removecolorder,
						updates_ids = args.opsorder,
						limkeys = args.limkeys,
						delmode = args.delmode,
						simulupdcode = args.simulupdcode)
					
	except:
		logger.exception("")
	

if __name__ == "__main__":
	cli_main()



