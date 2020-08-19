# -*- coding: utf-8 -*-

from __future__ import print_function
from os import listdir, mkdir, makedirs, remove as removefile
from os.path import abspath, dirname, exists, join as path_join
from datetime import datetime as dt
from copy import deepcopy

import argparse
import logging
import logging.config
import json
import codecs
import re
import io

from src.common import LOG_CLI_CFG, LANG, OPS, OPS_CONNECTED, OPS_INPUT, OPS_HELP, SETUP_ZIP, BASE_CONNCFG, BASE_FILTERS_RE, PROC_SRC_BODY_FNAME
from src.read import srcreader
from src.connect import Connections
from src.compare import comparing
from src.zip import gen_setup_zip
from src.fileandpath import get_conn_cfg_path, get_filters_cfg, exists_currentref, to_jsonfile, save_ref, get_refcodedir, save_warnings
from src.write import updateref

try:
    file_types = (file, io.IOBase)
except NameError:
    file_types = (io.IOBase,)
    
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
	
	args = parser.parse_args()
	
	logger = logging.getLogger('pgsourcing')	
	logger.info("Inicio, args: %s" % args)
	
	if args.setup and not args.newproj is None:
		parser.print_help()
		raise RuntimeError("opcoes -s e -n sao mutuamente exclusivas")
	
	proj = None
	if not args.setup and args.newproj is None:
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
				
		if not args.oper in OPS:
			raise RuntimeError("Operacao '%s' invalida, ops disponiveis: %s" % (args.oper, str(ops_help)))

		if args.oper in OPS_INPUT and args.input is None:
			parser.print_help()
			raise RuntimeError("Operacao '%s' exige indicacao ficheiro de entrada com opcao -i" % args.oper)
		
	return args, proj

def log_cli_setup(tosdout=False):
	
	logger = logging.getLogger('pgsourcing')
	logger.setLevel(LOG_CLI_CFG["level"])
	logfmt = logging.Formatter(LOG_CLI_CFG["format"])
	fh = logging.FileHandler(LOG_CLI_CFG["filename"])
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

	the_dir = path_join(proj_dir, "referencia")
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
	
	if p_oper in OPS_CONNECTED:
		cfgpath = get_conn_cfg_path(p_proj)
		conns = Connections(cfgpath, subkey="conn")
	
	if p_oper == "chksrc":

		logger.info("checking, proj:%s  oper:%s" % (p_proj,p_oper))
		
		if p_connkey is None:
			if not conns.checkConn("src"):
				raise RuntimeError("Chksource: connection identificada com 'src' nao existe, necessario indicar chave respetiva")
			else:
				connkey = 'src'
		else:	
			connkey = p_connkey		
				
		ret = "From SRC"
		
	elif p_oper == "chkdest":
		
		logger.info("checking, proj:%s  oper:%s" % (p_proj,p_oper))

		if p_connkey is None:
			if not conns.checkConn("dest"):
				raise RuntimeError("Chkdest: connection identificada com 'dest' nao existe, necessario indicar chave respetiva")
			else:
				connkey = 'dest'
		else:	
			connkey = p_connkey		

		ret = "From REF"
		
	if not connkey is None:
		
		filters_cfg = get_filters_cfg(p_proj, connkey)

		del o_replaces[:]
		if "replace" in filters_cfg.keys():
			o_replaces.extend(filters_cfg["replace"])

		srcreader(conns.getConn(connkey), filters_cfg, p_outprocsdir, o_checkdict, include_public=include_public, include_colorder=include_colorder)
		
	return ret

def process_intervals_string(p_input_str):
	
	sequence = set()
	
	groups = re.split("[,;/#]", p_input_str)
	
	for grp in groups:
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
	
def update_oper_handler(p_proj, p_oper, p_difdict, updates_ids=None, p_connkey=None):
	
	# updates_ids - se None ou lista vazia, aplicar todo o diffdict
	
	logger = logging.getLogger('pgsourcing')	
	logger.info("updating, proj:%s  oper:%s" % (p_proj,p_oper))

	conns = None
	
	upd_ids_list = []
	if not updates_ids is None:
		if isinstance(updates_ids, str):
			upd_ids_list = process_intervals_string(updates_ids)
		elif isinstance(updates_ids, list):
			upd_ids_list = updates_ids
	
	if p_oper == "updref":
		
		updateref(p_proj, p_difdict, upd_ids_list)
		
	elif p_oper == "upddest":

		if p_oper in OPS_CONNECTED:
			cfgpath = get_conn_cfg_path(p_proj)
			conns = Connections(cfgpath)

		if p_connkey is None:
			if not conns.checkConn("dest"):
				raise RuntimeError("Chkdest: connection identificada com 'dest' nao existe, necessario indicar chave respetiva")
			else:
				connkey = 'dest'
		else:	
			connkey = p_connkey		
		#srcreader(conns.getConn(p_connkey), o_checkdict)


class OpOrderMgr(Singleton):
	
	def __init__(self):
		self.ord = 0
		
	def setord(self, p_dict):	
		# Se linha abaixo falhar, ord nao e' incrementado desnecessariamente	
		p_dict["oporder"] = self.ord + 1
		self.ord = self.ord + 1

	
def main(p_proj, p_oper, p_connkey, newgenprocsdir=None, output=None, inputf=None, canuse_stdout=False, include_public=False, include_colorder=False, updates_ids=None):
	
	opordmgr = OpOrderMgr()
	
	logger = logging.getLogger('pgsourcing')	
	
	refcodedir = get_refcodedir(p_proj)
	
	check_dict = { }
	root_diff_dict = { "content": {} }
	#ordered_diffkeys = {}
	replaces = []
	comparison_mode = check_oper_handler(p_proj, p_oper, refcodedir, check_dict, replaces, p_connkey=p_connkey, include_public=include_public, include_colorder=include_colorder)
	
	# Se a operacao for chksrc ou chkdest o dicionario check_dict sera 
	#  preenchido.
	
	if check_dict:
		
		check_dict["pgsourcing_output_type"] = "rawcheck"
		
		now_dt = dt.now()
		
		base_ts = now_dt.strftime('%Y%m%dT%H%M%S')
		check_dict["project"] = p_proj
		check_dict["timestamp"] = base_ts
		root_diff_dict["project"] = p_proj
		root_diff_dict["timestamp"] = base_ts
		
		do_compare = False
		if comparison_mode == "From SRC":
			
			if not exists_currentref(p_proj):
				
				ref_dict = {
					"pgsourcing_output_type": "reference"
				}
				
				# Extrair warnings antes de gravar
				for k in check_dict:
					if not k.startswith("warning"):
						ref_dict[k] = check_dict[k]
				save_ref(p_proj, ref_dict, now_dt)
				
			else:
				
				do_compare = True
				
			# Separar warnings para ficheiro proprio
			
			wout_json = {
				"project": p_proj,
				"pgsourcing_output_type": "warnings_only",
				"timestamp": base_ts,
				"content": check_dict["warnings"] 
			}			
			save_warnings(p_proj, wout_json)	
			
		elif comparison_mode == "From REF":
			
			if not exists_currentref(p_proj):
				raise RuntimeError, "Referencia do projeto '%s' em falta, necessario correr chksrc primeiro" % p_proj
			else:
				do_compare = True
		
		comparing(p_proj, check_dict["content"], 
						comparison_mode, replaces, opordmgr, root_diff_dict["content"])
						
		## TODO - deve haver uma verificacao final de coerencia
		## Sequencias - tipo da seq. == tipo do campo serial em que e usada

		if root_diff_dict["content"]:
			
			logger.info("result: DIFF FOUND (proj '%s')" % p_proj)
			out_json = deepcopy(root_diff_dict)
			out_json["pgsourcing_output_type"] = "diff"
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
			do_output(check_dict, output=output, interactive=canuse_stdout)
			
	else:
		
		# Se a operacao for updref ou chkdest o dicionario check_dict sera 
		#  preenchido.

		diffdict = None
		if isinstance(inputf, str):
			if exists(inputf):
				with open(inputf, "r") as fj:
					diffdict = json.load(fj)
		elif isinstance(inputf, file_types):
			diffdict = json.load(inputf)			
					
		assert not diffdict is None
		
		update_oper_handler(p_proj, p_oper, diffdict, updates_ids=updates_ids, p_connkey=p_connkey)
					

		
# ######################################################################

def cli_main(canuse_stdout=False):

	# Config
	check_filesystem()
	
	log_cli_setup(tosdout=canuse_stdout)	
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
			main(proj, args.oper, args.connkey, args.genprocsdir, 
					output=args.output, inputf=args.input, 
					canuse_stdout=canuse_stdout, 
					include_public=args.includepublic, 
					include_colorder = not args.removecolorder,
					updates_ids = args.opsorder)
					
	except:
		logger.exception("")
	

if __name__ == "__main__":
	cli_main(canuse_stdout=True)
	

## incluir Postgis_full_version(), version() 
## Select table() pgr_version()


