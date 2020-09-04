
import json

from os import makedirs, walk, remove as removefile
from os.path import exists, dirname, abspath, join as path_join
from shutil import rmtree


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
	with open(p_output, "w") as fj:
		json.dump(p_obj, fj, indent=2, sort_keys=True)

def from_jsonfile(p_input, o_obj):
	if not exists(p_input):
		raise RuntimeError, "Ficheiro de entrada inexistente: '%s'" % p_input
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
	assert "srccodedir" in cfgdict, "get_srccodedir, missing source code dir in config, key: %s" % p_key
		
	return path_join(cfgdict["srccodedir"], "procedures")

def get_srccodedir_trigger(p_cfgpath, p_key):	
	cfgdict = None
	if exists(p_cfgpath):
		with open(p_cfgpath) as cfgfl:
			cfgdict = json.load(cfgfl)		
	assert not cfgdict is None, "get_srccodedir_trigger, missing config in: %s" % p_cfgpath
	assert "srccodedir" in cfgdict, "get_srccodedir_trigger, missing trigger source code dir in config, key: %s" % p_key
		
	return path_join(cfgdict["srccodedir"], "triggers")
		

# ######################################################################


