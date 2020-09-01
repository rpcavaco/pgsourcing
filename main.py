# -*- coding: utf-8 -*-

from __future__ import print_function
from os import listdir, mkdir, makedirs, remove as removefile
from os.path import abspath, dirname, exists, join as path_join
from datetime import datetime as dt
from copy import deepcopy

from psycopg2.extras import execute_batch

import argparse
import logging
import logging.config
import json
import codecs
import re
import io
import StringIO

from src.common import LOG_CLI_CFG, LANG, OPS, OPS_CONNECTED, OPS_INPUT, \
		OPS_OUTPUT, OPS_HELP, OPS_CHECK, SETUP_ZIP, BASE_CONNCFG, \
		BASE_FILTERS_RE, PROC_SRC_BODY_FNAME
from src.read import srcreader
from src.connect import Connections
from src.compare import comparing, keychains
from src.zip import gen_setup_zip
from src.fileandpath import get_conn_cfg_path, get_filters_cfg, exists_currentref, to_jsonfile, save_ref, get_refcodedir, save_warnings
from src.write import updateref, updatedb

try:
    file_types = (file, io.IOBase, StringIO.StringIO)
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
	parser.add_argument("-k", "--limkeys", help="Filtro de atributios a alterar: apenas estes atributos serao alterados", action="store")
	parser.add_argument("-m", "--delmode", help="Modo apagamento: NODEL (default), DEL, CASCADE", action="store")
	
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

		if args.oper in OPS_OUTPUT and args.output is None:
			parser.print_help()
			raise RuntimeError("Operacao '%s' exige indicacao ficheiro de saida com opcao -o" % args.oper)
			
	if args.input:
		assert exists(args.input), "Ficheiro de entrada inexistente: '%s'" % args.input
		
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
				
def do_linesoutput(p_obj, output=None, interactive=False):
	
	outer_sep = ";\n"

	if len(p_obj) > 0:
		
		finallines = []
		for item in p_obj:
			if isinstance(item, basestring):
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
			if isinstance(output, basestring):
				
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
		if isinstance(updates_ids, basestring):
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
				raise RuntimeError("Chkdest: connection identificada com 'dest' nao existe, necessario indicar chave respetiva")
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


class OpOrderMgr(Singleton):
	
	def __init__(self):
		self.ord = 0
		
	def setord(self, p_dict):	
		# Se linha abaixo falhar, ord nao e' incrementado desnecessariamente	
		p_dict["operorder"] = self.ord + 1
		self.ord = self.ord + 1

	
def main(p_proj, p_oper, p_connkey, newgenprocsdir=None, output=None, inputf=None, 
		canuse_stdout=False, include_public=False, include_colorder=False, 
		updates_ids=None, limkeys=None, delmode=None):
	
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
		
		check_dict["pgsourcing_output_type"] = "rawcheck"
		
		now_dt = dt.now()
		
		base_ts = now_dt.strftime('%Y%m%dT%H%M%S')
		check_dict["project"] = p_proj
		check_dict["timestamp"] = base_ts
		check_dict["pgsourcing_output_type"] = "rawcheck"

		root_diff_dict["project"] = p_proj
		root_diff_dict["timestamp"] = base_ts
		root_diff_dict["pgsourcing_output_type"] = "diff"
		
		do_compare = False
		if comparison_mode == "From SRC":
			
			if not exists_currentref(p_proj):
				
				ref_dict = {
					"pgsourcing_output_type": "reference"
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
				"timestamp": base_ts,
				"content": check_dict["warnings"] 
			}			
			save_warnings(p_proj, wout_json)	
			
		elif comparison_mode == "From REF":
			
			if not exists_currentref(p_proj):
				raise RuntimeError, "Referencia do projeto '%s' em falta, necessario correr chksrc primeiro" % p_proj
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

		# Se a operacao for updref ou chkdest o dicionario check_dict sera 
		#  preenchido.

		diffdict = None
		if isinstance(inputf, basestring):
			if exists(inputf):
				with open(inputf, "r") as fj:
					diffdict = json.load(fj)
		elif isinstance(inputf, file_types):
			diffdict = json.load(inputf)
			
		if delmode is None:
			dlmd = "NODEL"
		else:
			dlmd = 	delmode
			
		update_oper_handler(p_proj, p_oper, opordmgr, 
			diffdict, updates_ids=updates_ids, p_connkey=p_connkey, 
			limkeys=limkeys, include_public=include_public, 
			include_colorder=include_colorder, output=output, 
			canuse_stdout=canuse_stdout, delmode=dlmd)
					
	
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
					updates_ids = args.opsorder,
					limkeys = args.limkeys,
					delmode = args.delmode)
					
	except:
		logger.exception("")
	

if __name__ == "__main__":
	cli_main(canuse_stdout=True)



