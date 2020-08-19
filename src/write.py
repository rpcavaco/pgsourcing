
from src.fileandpath import load_currentref



def updateref(p_proj, p_difdict, updates_ids=None):
	
	root_ref_json = load_currentref(p_proj)
	ref_json = root_ref_json["content"]
	
	for k in p_difdict.keys():
		print(k)
	
