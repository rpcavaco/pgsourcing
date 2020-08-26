
from src.common import PROC_SRC_BODY_FNAME

SQL = {
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
			data_type, numeric_precision, numeric_precision_radix, numeric_scale,
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
	"TABLES": """SELECT schemaname, tablename, tableowner, tablespace
		FROM pg_tables
		WHERE schemaname NOT LIKE 'pg\_%'""",
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
		WHERE contype IN ('p')
		AND n2.nspname = %s
		AND t.relname = %s) a
		JOIN pg_attribute atr
			ON a.oid = atr.attrelid 
			and a.attnum = atr.attnum
		GROUP BY idxtblspc, conname""",
	"FKEYS": r"""select conname, cdef
			from
			(
				select b.conname, b.cdef,
				(regexp_split_to_array(b.table_from, E'\\.'))[2] as table_name
				from 
				( 
					SELECT 
					  conrelid::regclass::text AS table_from,
					  conname,
					  pg_get_constraintdef(c.oid) AS cdef 
					FROM pg_constraint c 
					JOIN pg_namespace n 
					  ON n.oid = c.connamespace 
					WHERE contype IN ('f') 
					AND n.nspname = %s 
				) b
			) a
			where a.table_name = %s""",
	"CHECKS": r"""select c.conname, pg_get_constraintdef(c.oid) AS cdef
		FROM pg_constraint c 
		JOIN pg_class t
			ON c.conrelid = t.oid
		JOIN pg_namespace n2
		  ON n2.oid = t.relnamespace
		WHERE contype IN ('c')
		AND n2.nspname = %s
		AND t.relname = %s""",
	"UNIQUE": r"""select conname, cdef
			from
			(
				select b.conname, b.cdef,
				(regexp_split_to_array(b.table_from, E'\\.'))[2] as table_name
				from 
				( 
					SELECT 
					  conrelid::regclass::text AS table_from,
					  conname,
					  pg_get_constraintdef(c.oid) AS cdef 
					FROM pg_constraint c 
					JOIN pg_namespace n 
					  ON n.oid = c.connamespace 
					WHERE contype IN ('u') 
					AND n.nspname = %s 
				) b
			) a
			where a.table_name = %s""",
	"INDEXES": """SELECT indexname, indexdef
			FROM
				pg_indexes
			WHERE
				schemaname = %s and tablename = %s""",
	"PROCS_OLD": """SELECT procedure_schema, procedure_name, args, return_type,
				procedure_owner, language_type, %s, provolatile
			FROM
			  (SELECT ns1.nspname as procedure_schema, 
				p.proname AS procedure_name,
				pg_get_function_identity_arguments(p.oid) args, 
				t1.typname AS return_type,
				proowner::regrole::text AS procedure_owner,
				l.lanname AS language_type,
				prosrc AS %s,
				p.provolatile
			FROM pg_proc p
			LEFT JOIN pg_type t1 ON p.prorettype=t1.oid   
			LEFT JOIN pg_language l ON p.prolang=l.oid
			join pg_namespace ns1 on p.pronamespace = ns1.oid
			WHERE l.lanname not in ('internal', 'c')
				-- and p.prokind in ('f', 'p')
				AND NOT p.proisagg) a
				where not procedure_schema in ('pg_catalog', 'information_schema')""" % (PROC_SRC_BODY_FNAME,PROC_SRC_BODY_FNAME),
	"PROCS_NEW": """SELECT procedure_schema, procedure_name, args, return_type,
				procedure_owner, language_type, %s, provolatile
			FROM
			  (SELECT ns1.nspname as procedure_schema, 
				p.proname AS procedure_name,
				pg_get_function_identity_arguments(p.oid) args, 
				t1.typname AS return_type,
				proowner::regrole::text AS procedure_owner,
				l.lanname AS language_type,
				prosrc AS %s,
				p.provolatile
			FROM pg_proc p
			LEFT JOIN pg_type t1 ON p.prorettype=t1.oid   
			LEFT JOIN pg_language l ON p.prolang=l.oid
			join pg_namespace ns1 on p.pronamespace = ns1.oid
			WHERE l.lanname not in ('internal', 'c')
				and p.prokind in ('f', 'p')) a
				where not procedure_schema in ('pg_catalog', 'information_schema')""" % (PROC_SRC_BODY_FNAME,PROC_SRC_BODY_FNAME),
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
										when 20 then 'INSERT, UPDATE'
										when 28 then 'INSERT, UPDATE, DELETE'
										when 24 then 'UPDATE, DELETE'
										when 12 then 'INSERT, DELETE'
						end as trigger_event,
						case t.tgtype::integer & 1
										when 1 then 'ROW'::text
										else 'STATEMENT'::text
						end as trigger_level,
				 		ns1.nspname as function_schema,
						p.proname as function_name
				 from pg_trigger t
				 left join pg_class c on t.tgrelid = c.oid
				 left join pg_namespace ns1 on c.relnamespace = ns1.oid
				 left join pg_proc p on t.tgfoid = p.oid
				 left join pg_namespace ns2 on p.pronamespace = ns2.oid
				 where not tgisinternal"""
}
