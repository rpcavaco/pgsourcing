import json    
    
from base64 import b64decode
from os.path import exists

class ConnectionError(Exception):
    """Exception raised for bad database connections"""

# postgresql: Dict with 
	# dbname="test", user="postgres", password="secret"
	# dbname="test", user="postgres", password="secret", port="5431"

class Conn(object):

	def __init__(self, p_dict):

		self.conn = None

		self.dict_cursor_factory = None
		
		if "password" not in p_dict.keys():
			raise ConnectionError("Conn: no password")

		passw = b64decode(p_dict["password"]).decode("utf-8") 
		
		newdict = {}
		for k in p_dict.keys():
			if k == "password":
				newdict[k] = passw
			else:
				newdict[k] = p_dict[k]
				
		# print(newdict)				
		try:
			import psycopg2
			import psycopg2.extras
			self.conn = psycopg2.connect(**newdict)	
			self.dict_cursor_factory = psycopg2.extras.DictCursor
		except ImportError:
			raise ConnectionError("Conn: no psycopg2 driver installed")
			
	def getConn(self):
		if self.conn is None:
			raise ConnectionError("Conn: getConn on NULL connection")
		return self.conn
						
	def _test(self):
		ret = False
		cur = self.conn.cursor()
		cur.execute("SELECT 1")
		row = cur.fetchone()
		if row[0] == 1:
			ret = True
		return ret
					
	def __enter__(self):
		if not self.conn is None:
			if not self._test():
				raise ConnectionError("Conn: __enter__ self test failed ")
		else:
			raise ConnectionError("Conn: getConn on NULL connection")
		return self
		
	def __exit__(self, exc_type, exc_value, traceback):
		if not self.conn is None and self.conn.closed == 0:
			self.conn.close()
		else:
			raise ConnectionError("Conn: __exit__ getConn on NULL or closed connection")
		self.conn = None
		return False
		
	def init(self):
		return self.__enter__

	def terminate(self):
		if not self.conn is None:
			self.__exit__(None, None, None)
	
class DBI(object):
	
	def __init__(self, p_cfgjson_file):
		self.connobj = None
		if not exists(p_cfgjson_file):
			raise RuntimeError("DBI: no cfg file '%s' found" % p_cfgjson_file)
		with open(p_cfgjson_file) as cfgfl:
			cfgdict = json.load(cfgfl)
			self.connobj = Conn(cfgdict)	
		if not self.connobj is None:
			self.connobj.init()
			
	def __del__(self):
		if not self.connobj is None:
			self.connobj.terminate()
			
	def getConn(self):
		ret = None
		if not self.connobj is None:
			ret = self.connobj.getConn()
		else:
			raise RuntimeError("DBI: no Conn")		
		return ret

class Connections(object):
	
	def __init__(self, p_db_cfg_json, rootjsonkey=None, subkey=None):
		self.currkey = None
		self.conns = {}
		with open(p_db_cfg_json) as cfgfl:
			cfgdict = json.load(cfgfl)
			if rootjsonkey is None:
				root = cfgdict
			else:
				root = cfgdict[rootjsonkey]
			for k in root.keys():
				if subkey is None:
					self.conns[k] = Conn(root[k])
				else:
					self.conns[k] = Conn(root[k][subkey])
				
	def __del__(self):
		for k in self.conns.keys():
			self.conns[k].terminate()
			
	def getKeys(self):
		return self.conns.keys()
		
	def getConn(self, p_key):
		return self.conns[p_key]

	def checkConn(self, p_key):
		return p_key in self.conns.keys()

	def getConns(self):
		for k in self.conns.keys():
			yield k, self.conns[k]
		
						
def testConn():
	try:
		
		try:
			con = Conn({"host": "aa", "sid": "aa", "usr": "xxx", "password": "eXk=" })
		except Exception as e:
			if not str(e).startswith("invalid dsn: invalid connection option \"sid\""):
				raise

		try:
			con = Conn({"host": "aa", "dbname": "aa", "usr": "xxx", "password": "eXk=" })
		except Exception as e:
			if not str(e).startswith("invalid dsn: invalid connection option \"usr\""):
				raise

		# try:
			# con = Conn("POSTGRESQL", {"host": "aa", "dbname": "aa", "user": "xxx", "password": "yy" })
		# except Exception as e:
			# if not str(e).startswith("invalid dsn: invalid connection option \"pwd\""):
				# raise

				
		print("class Conn successfully tested")
	except:
		print("Test failed")
		raise
		
if __name__ == "__main__":
	## first remove module from from synpgcode.common
	testConn()
		

				
				
		

		
		
