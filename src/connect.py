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
import logging
    
from base64 import b64decode
from os.path import exists

class ConnectionError(Exception):
    """Exception raised for bad database connections"""


class Conn(object):

	def __init__(self, p_dict):

		self.conn = None
		self.db = None
		self.cndict = {}

		self.dict_cursor_factory = None
		
		if "password" not in p_dict.keys():
			raise ConnectionError("Conn: no password")

		try:
			passw = b64decode(p_dict["password"]).decode("utf-8") 
		except AttributeError:
			passw = str(b64decode(p_dict["password"]))
		except:
			raise RuntimeError(f"pass: {p_dict['password']}")

			
		cndict = {}
		for k in p_dict.keys():
			if k == "password":
				cndict[k] = passw
			elif k == "database":
				cndict[k] = p_dict[k]
				self.cndict[k] = p_dict[k]
				self.db = p_dict[k]
			else:
				cndict[k] = p_dict[k]
				self.cndict[k] = p_dict[k]
		if self.db is None:
			raise ConnectionError("Conn: missing 'database' parameter")
				
		# print(newdict)				
		try:
			import psycopg2
			import psycopg2.extras
			self.conn = psycopg2.connect(**cndict)	
			self.dict_cursor_factory = psycopg2.extras.DictCursor
		except ImportError:
			raise ConnectionError("Conn: no psycopg2 driver installed")

	def __repr__(self) -> str:
		return str(self.cndict)
			
	def getConn(self):
		if self.conn is None:
			raise ConnectionError("Conn: getConn on NULL connection")
		return self.conn
						
	def getDb(self):
		if self.db is None:
			raise ConnectionError("Conn: self.db in getDb is NULL")
		return self.db
						
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

		logger = logging.getLogger('pgsourcing')	

		self.currkey = None
		self.conns = {}
		with open(p_db_cfg_json) as cfgfl:
			cfgdict = json.load(cfgfl)
			if rootjsonkey is None:
				root = cfgdict
			else:
				root = cfgdict[rootjsonkey]
			for k in root.keys():
				try:
					if subkey is None:
						self.conns[k] = Conn(root[k])
					else:
						self.conns[k] = Conn(root[k][subkey])
				except Exception as e:
					logger.exception(f"Connections: Conn creation error for key '{k}'")


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
			con = Conn({"host": "aa", "database": "aa", "usr": "xxx", "password": "eXk=" })
		except Exception as e:
			if not str(e).startswith("invalid dsn: invalid connection option \"usr\""):
				raise

		# try:
			# con = Conn("POSTGRESQL", {"host": "aa", "database": "aa", "user": "xxx", "password": "yy" })
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
		

				
				
		

		
		
