
from src.fileandpath import load_currentref


def updateref(p_proj, p_difdict, updates_ids_list):
	
	print("updates_ids_list:", updates_ids_list)
	
	root_ref_json = load_currentref(p_proj)
	ref_json = root_ref_json["content"]
	
	diff_content = p_difdict["content"]
	
	for k in diff_content.keys():
		print(k)
	
