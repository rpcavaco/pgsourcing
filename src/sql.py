
from src.common import PROC_SRC_BODY_FNAME
#from common import PROC_SRC_BODY_FNAME

SQL = {
	"PROC_CHECK": """select 
			pg_get_function_identity_arguments(p.oid) args,
			t2.typname return_type
		from pg_proc p
		JOIN pg_namespace n1
		ON p.pronamespace = n1.oid
		LEFT JOIN pg_type t2 ON p.prorettype=t2.oid
		where nspname = %s
		and p.proname = %s""",
	"GENERIC_CHECK": """select relkind
		from pg_class t1
		JOIN pg_namespace n1
		ON t1.relnamespace = n1.oid
		where nspname = %s
		and t1.relname = %s""",
	"INDEX_CHECK": """SELECT count(*)
		FROM pg_indexes
		WHERE schemaname = %s  
		AND tablename = %s and tablename = %s""",
	"SCHEMA_CHK": """select count(*)
		from information_schema.schemata
		where schema_name = %s""", 
	"SCHEMAS": """select schema_name, schema_owner
		from information_schema.schemata""", 
	"PROCOWNERS": """select distinct proowner::regrole as ownr
		from pg_proc
		where pronamespace::regnamespace::text = %s
		and not proname ~* 'i[0-9]+_(get|return)_ids'""",
	"PROC_SU_OWNED": """select proname
		from pg_proc
		where pronamespace::regnamespace::text = %s
		and not proname ~* 'i[0-9]+_(get|return)_ids'
		and proowner::regrole = 'postgres'::regrole""",
	"TABLEOWNERS": """select distinct tableowner as ownr from pg_tables
		where schemaname = %s""",	
	"TABLE_SU_OWNED": """select tablename from pg_tables
		where schemaname = %s and tableowner = 'postgres'""",	
	"ROLES": "select * from pg_roles",
	"SEQUENCES": """select 
			start_value, minimum_value, maximum_value, increment, cycle_option
			from information_schema.sequences
			where sequence_schema = %s
			and sequence_name = %s""",
	"SEQ_CURRVAL": """select 
			setval(seqname, 
					CASE WHEN nextval(seqname) > 1 
						THEN currval(seqname)-1 
						ELSE 1 
						END, 
					true) current_value
			from
			(
				select relnamespace::regnamespace::text || '.' || c.relname::text seqname
				FROM pg_class c WHERE c.relkind='S'
				and relnamespace::regnamespace::text = %s
				and c.relname = %s
			) a""",
	"SEQ_CACHEVALUE_PRE10": "SELECT cache_value from %s.%s",
	"SEQ_CACHEVALUE_FROM10": "SELECT seqcache from pg_sequence where seqrelid = '%s.%s'::regclass",
	"SEQ_OWNER": """select a.rolname
		from pg_class c
		join pg_namespace n
		on c.relnamespace = n.oid
		join pg_roles a
		on c.relowner = a.oid
		where n.nspname = %s
		and c.relname = %s""",
	"TABLES": """SELECT schemaname, tablename, tableowner, tablespace
		FROM pg_tables
		WHERE schemaname NOT LIKE 'pg\_%'""",
	"VIEWS": """select schemaname,
		viewname,
		viewowner, definition
		from pg_views""",
	"MATVIEWS": """select schemaname,
		matviewname,
		matviewowner as viewowner, definition,
		tablespace
		from pg_matviews""",
	"COLUMNS": """SELECT column_name, ordinal_position,
			column_default, is_nullable, data_type,
			character_maximum_length, numeric_precision,
			numeric_precision_radix, numeric_scale, datetime_precision,
			udt_name
		FROM information_schema.columns
		WHERE table_schema = %s
			AND table_name = %s
		ORDER BY ordinal_position""",
	"PKEYS": """select idxtblspc, conname as constraint_name, json_agg(attname) column_names
		from
		(select ts.spcname as idxtblspc, c.conname, t.oid,
		unnest(c.conkey) attnum
		FROM pg_constraint c 
		JOIN pg_class t
			ON c.conrelid = t.oid
		JOIN pg_namespace n2
		  ON n2.oid = t.relnamespace
		JOIN pg_class i
			ON c.conindid = i.oid
		LEFT JOIN pg_tablespace ts
		  ON ts.oid = i.reltablespace 
		WHERE contype IN ('p')
		AND n2.nspname = %s
		AND t.relname = %s) a
		JOIN pg_attribute atr
			ON a.oid = atr.attrelid 
			and a.attnum = atr.attnum
		GROUP BY idxtblspc, conname""",
	# "PKEY_EXISTS": """select count(*)
	# 	from
	# 	(select distinct conname as constraint_name
	# 			from
	# 			(select ts.spcname as idxtblspc, c.conname, t.oid,
	# 			unnest(c.conkey) attnum
	# 			FROM pg_constraint c 
	# 			JOIN pg_class t
	# 				ON c.conrelid = t.oid
	# 			JOIN pg_namespace n2
	# 			ON n2.oid = t.relnamespace
	# 			JOIN pg_class i
	# 				ON c.conindid = i.oid
	# 			LEFT JOIN pg_tablespace ts
	# 			ON ts.oid = i.reltablespace 
	# 			WHERE contype IN ('p')
	# 			AND n2.nspname = %s
	# 			AND t.relname = %s) a
	# 			JOIN pg_attribute atr
	# 				ON a.oid = atr.attrelid 
	# 				and a.attnum = atr.attnum
	# 			where conname = %s
	# 			GROUP BY idxtblspc, conname) d", 
	"FKEYS": """SELECT 
		n2.nspname AS schema_ref,
		t2.relname AS table_ref,
		conname, 
		pg_get_constraintdef(c.oid) AS cdef,
		CASE c.confmatchtype 
			WHEN 'f' THEN 'FULL'
			WHEN 'p' THEN 'PARTIAL'
			WHEN 's' THEN 'SIMPLE'
			ELSE ''
		END matchtype,
		CASE c.confupdtype 
			WHEN 'a' THEN 'NO ACTION'
			WHEN 'r' THEN 'RESTRICT'
			WHEN 'c' THEN 'CASCADE'
			WHEN 'n' THEN 'SET NULL'
			WHEN 'd' THEN 'SET DEFAULT'
			ELSE ''
		END updtype,
		CASE c.confdeltype 
			WHEN 'a' THEN 'NO ACTION'
			WHEN 'r' THEN 'RESTRICT'
			WHEN 'c' THEN 'CASCADE'
			WHEN 'n' THEN 'SET NULL'
			WHEN 'd' THEN 'SET DEFAULT'
			ELSE ''
		END deltype
		FROM pg_constraint c 
		JOIN pg_class t1
			on t1.oid = c.conrelid
		JOIN pg_namespace n1
			ON t1.relnamespace = n1.oid
		JOIN pg_class t2
			on t2.oid = c.confrelid
		JOIN pg_namespace n2
			ON t2.relnamespace = n2.oid
		WHERE contype IN ('f') 
		AND n1.nspname = %s
		AND t1.relname = %s""",
	"CHECKS": r"""select c.conname, pg_get_constraintdef(c.oid) AS cdef
		FROM pg_constraint c 
		JOIN pg_class t
			ON c.conrelid = t.oid
		JOIN pg_namespace n2
		  ON n2.oid = t.relnamespace
		WHERE contype IN ('c')
		AND n2.nspname = %s
		AND t.relname = %s""",
	"UNIQUE": """select idxtblspc, conname as constraint_name, json_agg(attname) column_names
		from
		(select n3.nspname as idxtblspc, c.conname, t.oid, 
		unnest(c.conkey) attnum
		FROM pg_constraint c 
		JOIN pg_class t
			ON c.conrelid = t.oid
		JOIN pg_namespace n2
		  ON n2.oid = t.relnamespace
		JOIN pg_class i
			ON c.conindid = i.oid
		JOIN pg_namespace n3
		  ON n3.oid = i.relnamespace 
		WHERE contype IN ('u')
		AND n2.nspname = %s
		AND t.relname = %s) a
		JOIN pg_attribute atr
			ON a.oid = atr.attrelid 
			and a.attnum = atr.attnum
		GROUP BY idxtblspc, conname""",		
	"INDEXES": """SELECT indexname, indexdef, tablespace
			FROM
				pg_indexes
			WHERE
				schemaname = %s and tablename = %s""",
	"PROCS_PRE11": """SELECT procedure_schema, procedure_name, args, fargs, return_type,
				procedure_owner, language_type, %s, provolatile
			FROM
			  (SELECT p.pronamespace::regnamespace::text as procedure_schema, 
				p.proname AS procedure_name,
				pg_get_function_identity_arguments(p.oid) args, 
				pg_get_function_arguments(p.oid) fargs, 
				t1.typname AS return_type,
				proowner::regrole::text AS procedure_owner,
				l.lanname AS language_type,
				prosrc AS %s,
				p.provolatile
			FROM pg_proc p
			LEFT JOIN pg_type t1 ON p.prorettype=t1.oid   
			LEFT JOIN pg_language l ON p.prolang=l.oid
			WHERE l.lanname not in ('internal', 'c')
				-- and p.prokind in ('f', 'p')
				AND NOT p.proisagg) a
				where not procedure_schema in ('pg_catalog', 'information_schema')""" % (PROC_SRC_BODY_FNAME,PROC_SRC_BODY_FNAME),
	"PROCS_FROM11": """SELECT procedure_schema, procedure_name, args, fargs, return_type,
				procedure_owner, language_type, %s, provolatile
			FROM
			  (SELECT p.pronamespace::regnamespace::text as procedure_schema, 
				p.proname AS procedure_name,
				pg_get_function_identity_arguments(p.oid) args, 
				pg_get_function_arguments(p.oid) fargs, 
				t1.typname AS return_type,
				proowner::regrole::text AS procedure_owner,
				l.lanname AS language_type,
				prosrc AS %s,
				p.provolatile
			FROM pg_proc p
			LEFT JOIN pg_type t1 ON p.prorettype=t1.oid   
			LEFT JOIN pg_language l ON p.prolang=l.oid
			WHERE l.lanname not in ('internal', 'c')
				and p.prokind in ('f', 'p')) a
				where not procedure_schema in ('pg_catalog', 'information_schema')""" % (PROC_SRC_BODY_FNAME,PROC_SRC_BODY_FNAME),
	"PROCS_RETTYPE_TABLE": """select string_agg(t.column_name || ' '|| t.arg_type::regtype::text, ', ')
		from pg_proc p
		cross join unnest(proargnames, proargmodes, proallargtypes) 
		with ordinality as t(column_name, arg_mode, arg_type, col_num)
		where p.pronamespace::regnamespace::text = %s
		and p.proname = %s
		and t.arg_mode = 't'""",
	"TRIGGERS": """select
				 		ns1.nspname as table_schema,
				 		c.relname as table_name,
						t.tgname as trigger_name,
						t.tgenabled = 'D' as disabled,
						CASE t.tgtype::integer & 66
										WHEN 2 THEN 'BEFORE'
										WHEN 64 THEN 'INSTEAD OF'
										ELSE 'AFTER'
						end as trigger_activation,
						case t.tgtype::integer & cast(28 as int2)
										when 16 then 'UPDATE'
										when 8 then 'DELETE'
										when 4 then 'INSERT'
										when 20 then 'INSERT OR UPDATE'
										when 28 then 'INSERT OR UPDATE OR DELETE'
										when 24 then 'UPDATE OR DELETE'
										when 12 then 'INSERT OR DELETE'
						end as trigger_event,
						case t.tgtype::integer & 1
										when 1 then 'ROW'::text
										else 'STATEMENT'::text
						end as trigger_level,
				 		ns2.nspname as function_schema,
						p.proname as function_name
				 from pg_trigger t
				 left join pg_class c on t.tgrelid = c.oid
				 left join pg_namespace ns1 on c.relnamespace = ns1.oid
				 left join pg_proc p on t.tgfoid = p.oid
				 left join pg_namespace ns2 on p.pronamespace = ns2.oid
				 where not tgisinternal""",
	"GRANTS": """with ps as (select regexp_split_to_array(unnest(c.relacl)::text, '=|/') AS acl
		from pg_class c
		join pg_namespace ns
			on c.relnamespace = ns.oid
		where ns.nspname = %s
		and c.relname = %s)
		select coalesce(nullif(acl[1], ''), 'public') grantee,
		(SELECT string_agg(privilege, ', ' ORDER BY privilege ASC)
					FROM (SELECT
						CASE ch
							WHEN 'r' THEN 'SELECT'
							WHEN 'w' THEN 'UPDATE'
							WHEN 'a' THEN 'INSERT'
							WHEN 'd' THEN 'DELETE'
							WHEN 'D' THEN 'TRUNCATE'
							WHEN 'x' THEN 'REFERENCES'
							WHEN 't' THEN 'TRIGGER'
							WHEN 'U' THEN 'USAGE'
							WHEN 'C' THEN 'CREATE'
						END AS privilege
						FROM regexp_split_to_table(acl[2], '') ch
					) s
				) AS privileges,	
		case acl[2] 
			when 'arwdDxt' then true
			when 'rwU' then true -- seqs
			else false
		end allprivs
		from ps""",		
	"SCHGRANTS": """with ps as (select regexp_split_to_array(unnest(ns.nspacl)::text, '=|/') AS acl
		from pg_namespace ns
		where ns.nspname = %s)
		select coalesce(nullif(acl[1], ''), 'public') grantee,
		(SELECT string_agg(privilege, ', ' ORDER BY privilege ASC)
					FROM (SELECT
						CASE ch
							WHEN 'U' THEN 'USAGE'
							WHEN 'C' THEN 'CREATE'
						END AS privilege
						FROM regexp_split_to_table(acl[2], '') ch
					) s
				) AS privileges,	
		case acl[2] 
			when 'UC' then true
			else false
		end allprivs
		from ps""",
	"SEQNAME_COLNAME": """select seqname, column_name from (
			select substring(column_default from 'nextval\(''([\.a-zA-Z_À-Ýà-ý0-9_\$]+)''::regclass\)') seqname, 
			column_name,
			row_number() over (order by ordinal_position) rn
			from information_schema.columns
			where table_schema = %s
			and table_name = %s
			and column_default like 'nextval%'
		) a
		where rn = 1""",
	"CHANGE_CURR_SEQVAL": """do 
			$$
			declare
				v_maxval integer;
				v_rec record;
				v_schema text;
				v_tname text;
				v_sql text;
			begin
				v_schema := '{0}';
				v_tname := '{1}';
				for v_rec in (
					select seqname, column_name from (
						select substring(column_default from 'nextval\(''([\.a-zA-Z_À-Ýà-ý0-9_\$]+)''::regclass\)') seqname, 
						column_name,
						row_number() over (order by ordinal_position) rn
						from information_schema.columns
						where table_schema = v_schema
						and table_name = v_tname
						and column_default like 'nextval%'
					) a
					where rn = 1) 
				loop
					execute format('select max(%s) from %I.%I', v_rec.column_name, v_schema, v_tname) into v_maxval;
					v_sql := format('select setval(''%s'', %s, true)', v_rec.seqname, v_maxval);
					execute v_sql;
					exit;
				end loop;
			end$$;""",
	"ROW_COUNT": "SELECT count(*) from {0}.{1}"
}

# if __name__ == "__main__":
	# import re
	# patt = re.compile(r"(from|join)[\s]+(?P<table>[^ \(\t\n\r\f\v]+)", flags= re.I)
	
	# for k in SQL.keys():
		# src = SQL[k]
		# mo = re.search(patt, src)
		# if mo:
			# print(k, "--", mo.group("table"), "--")
		# else:
			# print("NAO DETECTADO:", k)
			# print("-------------------------------------------")
			# print(src)
			# print("-------------------------------------------")
		
		
