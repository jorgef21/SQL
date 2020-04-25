/*
Utility to create CSV files pasing a query
 -need an oracle directory

This utilize UTL_FILE oracle functionality. Need proper permissions.

Package Spec
*/
CREATE OR REPLACE PACKAGE ODSMGR."CSV" AS

PROCEDURE generate (p_dir        IN  VARCHAR2,
                    p_file       IN  VARCHAR2,
                    p_query      IN  VARCHAR2);

PROCEDURE generate_rc (p_dir        IN  VARCHAR2,
                       p_file       IN  VARCHAR2,
                       p_refcursor  IN OUT SYS_REFCURSOR);

PROCEDURE set_separator (p_sep  IN  VARCHAR2);

END csv;
/*
Package Body
*/
CREATE OR REPLACE PACKAGE BODY ODSMGR."CSV" AS

g_sep         VARCHAR2(5)  := ',';
QUOTES CHAR(2) := CHR(34)||CHR(34);
PROCEDURE generate_all (p_dir        IN  VARCHAR2,
                        p_file       IN  VARCHAR2,
                        p_query      IN  VARCHAR2,
                        p_refcursor  IN OUT SYS_REFCURSOR);



PROCEDURE generate (p_dir        IN  VARCHAR2,
                    p_file       IN  VARCHAR2,
                    p_query      IN  VARCHAR2) AS
  l_cursor  SYS_REFCURSOR;
BEGIN
  EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_DATE_FORMAT = ''YYYY-MM-DD HH24:MI:SS''';
  EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_TIMESTAMP_FORMAT = ''YYYY-MM-DD HH24:MI:SS''';
  generate_all (p_dir        => p_dir,
                p_file       => p_file,
                p_query      => p_query,
                p_refcursor  => l_cursor);
END generate;



PROCEDURE generate_rc (p_dir        IN  VARCHAR2,
                       p_file       IN  VARCHAR2,
                       p_refcursor  IN OUT SYS_REFCURSOR) AS
BEGIN
  generate_all (p_dir        => p_dir,
                p_file       => p_file,
                p_query      => NULL,
                p_refcursor  => p_refcursor);
END generate_rc;



PROCEDURE generate_all (p_dir        IN  VARCHAR2,
                        p_file       IN  VARCHAR2,
                        p_query      IN  VARCHAR2,
                        p_refcursor  IN OUT  SYS_REFCURSOR) AS
  l_cursor    PLS_INTEGER;
  l_rows      PLS_INTEGER;
  l_col_cnt   PLS_INTEGER;
  l_desc_tab  DBMS_SQL.desc_tab;
  l_buffer    VARCHAR2(32767);

  l_file      UTL_FILE.file_type;
BEGIN
  
  IF p_query IS NOT NULL THEN
    l_cursor := DBMS_SQL.open_cursor;
    DBMS_SQL.parse(l_cursor, p_query, DBMS_SQL.native);
  ELSIF p_refcursor%ISOPEN THEN
     l_cursor := DBMS_SQL.to_cursor_number(p_refcursor);
  ELSE
    RAISE_APPLICATION_ERROR(-20000, 'You must specify a query or a REF CURSOR.');
  END IF;

  DBMS_SQL.describe_columns (l_cursor, l_col_cnt, l_desc_tab);

  FOR i IN 1 .. l_col_cnt LOOP
    DBMS_SQL.define_column(l_cursor, i, l_buffer, 32767 );
  END LOOP;

  IF p_query IS NOT NULL THEN
    l_rows := DBMS_SQL.execute(l_cursor);
  END IF;

  l_file := UTL_FILE.fopen(p_dir, p_file, 'w', 32767);

--ADDING COLUMN HEADER


  LOOP
    EXIT WHEN DBMS_SQL.fetch_rows(l_cursor) = 0;

    FOR i IN 1 .. l_col_cnt LOOP
      IF i > 1 THEN
        UTL_FILE.put(l_file, g_sep );
      END IF;

      DBMS_SQL.COLUMN_VALUE(l_cursor,i,l_buffer);
      --IF INSTR(l_buffer,',') > 0 THEN 
        --UTL_FILE.put(l_file,'"'||l_buffer||'"');
      
      --ELSE 
      	--	UTL_FILE.put(l_file,l_buffer);
      --END IF;
      CASE
      		WHEN INSTR(l_buffer,',') > 0 THEN
      		 UTL_FILE.put(l_file,'"'||REPLACE(l_buffer,'"','''')||'"');
      		WHEN INSTR(l_buffer,CHR(10)) > 0 OR INSTR(l_buffer,CHR(13)) > 0 OR INSTR(l_buffer,u'\000A')>0  THEN
      		 UTL_FILE.put(l_file,'"'||REPLACE(l_buffer,'"','''')||'"');
      		WHEN REGEXP_LIKE(l_buffer,'[^\x80-\xFF]') THEN 
      		 UTL_FILE.put(l_file,'"'||REPLACE(l_buffer,'"','''')||'"');
      		ELSE 
      		  UTL_FILE.put(l_file,REPLACE(l_buffer,CHR(13),QUOTES));
	  END CASE;
     
    END LOOP;
    UTL_FILE.new_line(l_file);
  END LOOP;

  UTL_FILE.fclose(l_file);
  DBMS_SQL.close_cursor(l_cursor);
EXCEPTION
  WHEN OTHERS THEN
    IF UTL_FILE.is_open(l_file) THEN
      UTL_FILE.fclose(l_file);
    END IF;
    IF DBMS_SQL.is_open(l_cursor) THEN
      DBMS_SQL.close_cursor(l_cursor);
    END IF;
    DBMS_OUTPUT.put_line('ERROR: ' || DBMS_UTILITY.format_error_backtrace);
    RAISE;
END generate_all;



PROCEDURE set_separator (p_sep  IN  VARCHAR2) AS
BEGIN
  g_sep := p_sep;
END set_separator;

END csv;