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

from os import listdir, makedirs, walk, remove as removefile
from os.path import exists, dirname, abspath, join as path_join
from shutil import rmtree, make_archive
from re import search
from datetime import datetime as dt


from src.common import PROJECTDIR

# ######################################################################
# Filename and path methods
# ######################################################################

def _get_projdir(p_proj):	
	return path_join(dirname(dirname(abspath(__file__))), PROJECTDIR, p_proj)

def _get_refdir(p_proj):	
	return path_join(_get_projdir(p_proj), "reference")

def get_conn_cfg_path(p_proj):	
	return path_join(_get_projdir(p_proj), "conncfg.json")

def get_filters_cfg(p_proj, p_key):	
	path = path_join(_get_projdir(p_proj), "conncfg.json")
	ret = None
	with open(path, "r") as fj:
		ret = json.load(fj)
	return ret[p_key]["filters"]
			
def _get_timedref(p_proj, p_dt):
	return path_join(_get_refdir(p_proj), "REF%s.json" % p_dt.strftime('%Y%m%dT%H%M%S'))
	
def get_currentref(p_proj):
	return path_join(_get_refdir(p_proj), "current.json")

def get_refwarnings(p_proj):
	return path_join(_get_refdir(p_proj), "warnings.json")

def get_refcodedir(p_proj):
	pth = path_join(_get_refdir(p_proj), "code")
	if not exists(pth):
		makedirs(pth)
	return pth
	
def get_destcodedir(p_proj):
	pth = path_join(_get_refdir(p_proj), "code_dest")
	if not exists(pth):
		makedirs(pth)
	return pth
	
def get_reftablesdir(p_proj):
	pth = path_join(_get_refdir(p_proj), "parameterstables")
	if not exists(pth):
		makedirs(pth)
	return pth

def exists_currentref(p_proj):
	return exists(get_currentref(p_proj))

def load_currentref(p_proj):
	curr_ref_path = get_currentref(p_proj)
	assert exists(curr_ref_path)
	ret = None
	with open(curr_ref_path, "r") as fl:
		ret = json.load(fl)
	return ret	
	
def to_jsonfile(p_obj, p_output):
	flagv = isinstance(p_output, str)
	if flagv:
		with open(p_output, "w") as fj:
			json.dump(p_obj, fj, indent=2, sort_keys=True)
	else:
		json.dump(p_obj, p_output, indent=2, sort_keys=True)

def from_jsonfile(p_input, o_obj):
	if not exists(p_input):
		raise RuntimeError("input file not found: '%s'" % p_input)
	with open(p_input, "r") as fj:
		o_obj.update(json.load(fj))
				
def save_ref(p_proj, p_obj, p_dt):
	to_jsonfile(p_obj, _get_timedref(p_proj, p_dt))
	to_jsonfile(p_obj, get_currentref(p_proj))

def save_warnings(p_proj, p_obj):
	to_jsonfile(p_obj, get_refwarnings(p_proj))

def clear_dir(p_path, ext=None):	
	for root, dirs, files in walk(p_path):
		for f in files:
			if ext is None or f.lower().endswith(ext.lower()):
				removefile(path_join(root, f))
		if ext is None:
			for d in dirs:
				rmtree(path_join(root, d))

def get_srccodedir(p_cfgpath, p_key):	
	cfgdict = None
	if exists(p_cfgpath):
		with open(p_cfgpath) as cfgfl:
			cfgdict = json.load(cfgfl)		
	assert not cfgdict is None, "get_srccodedir, missing config in: %s" % p_cfgpath
	assert "srccodedir" in cfgdict[p_key].keys(), "get_srccodedir, missing source code dir in config, key: %s" % p_key
		
	return path_join(cfgdict[p_key]["srccodedir"], "procedures")

def get_srccodedir_trigger(p_cfgpath, p_key):	
	cfgdict = None
	if exists(p_cfgpath):
		with open(p_cfgpath) as cfgfl:
			cfgdict = json.load(cfgfl)		
	assert not cfgdict is None, "get_srccodedir_trigger, missing config in: %s" % p_cfgpath
	assert "srccodedir" in cfgdict[p_key].keys(), "get_srccodedir_trigger, missing trigger source code dir in config, key: %s" % p_key
		
	return path_join(cfgdict[p_key]["srccodedir"], "triggers")
		
def dropref(p_proj):

	rd = _get_refdir(p_proj)
	patt = "REF[0-9T]+.json"

	for currfile in ("current.json", "warnings.json"):
		currpath = path_join(rd, currfile)
		if exists(currpath):
			removefile(currpath)

	for fl in listdir(rd):
		if search(patt, fl):
			flfull = path_join(rd, fl)
			removefile(flfull)

	the_dirs = ["code", "code_dest", "parameterstables", "tables"]
	for dirname in the_dirs:
		the_dir = path_join(rd, dirname)
		if exists(the_dir):
			rmtree(the_dir)

def genprojectarchive(p_proj):

	projdir = _get_projdir(p_proj)
	projroot = dirname(projdir)

	now_dt = dt.now()

	zipfname = f"{p_proj}_{now_dt.strftime('%Y%m%dT%H%M%S')}"
	zippath = path_join(projroot, zipfname)

	make_archive(
		zippath, 
		"zip", 
		root_dir=projroot,
		base_dir=p_proj
	)


# ######################################################################


