
from os import walk
from os.path import abspath, dirname, sep, join as path_join

import zipfile

def zipdir(path, ziph):
	# ziph is zipfile handle
	for root, dirs, files in walk(path):
		for fl in files:
			if fl != "__init__.py" and (fl.startswith("_") or fl.endswith(".pyc")):
				continue
			ziph.write(path_join(root, fl), arcname=path_join("src", fl))

def gen_setup_zip(p_fname):
	
	src_dir = dirname(abspath(__file__))
	main_file = path_join(dirname(src_dir), "main.py")
		
	zipf = zipfile.ZipFile(p_fname, 'w', zipfile.ZIP_DEFLATED)
	zipdir(src_dir, zipf)
	zipf.write(main_file, arcname="main.py")
	zipf.close()
