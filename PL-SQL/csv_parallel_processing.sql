/*
Implement parallel procesing using the CSV package. This procedure will help to partition a table in chuncks and then
create CSV files out of the table. 

Requirements:
Partition your table based on a query, this works if the table has a primary key or not since it is using
rownumber metadata provided by oracle.
Call the procedure which will do the actual CSV export

Using dbms_jobs utility.
*/
CREATE OR REPLACE PROCEDURE ODSMGR.EXECUTE_CSV_PARALLEL (TABLE_NAME IN VARCHAR,TABLE_CATEGORY IN VARCHAR) AS
  TASK VARCHAR(300) := TABLE_NAME||' PARALLEL';
  PLSQL VARCHAR(1000);
  WS_QUOTE VARCHAR2(1) := CHR(39);
BEGIN
    --CREATE TASK
    DBMS_OUTPUT.PUT_LINE('START TIME: '||TO_CHAR(SYSDATE,'YYYY-MM-DD HH24:MI:SS'));
    DBMS_PARALLEL_EXECUTE.CREATE_TASK(TASK_NAME => TASK);

    --CREATE DIFFERENT CHUNKS BASED ON SQL QUERY, THIS CAN BE ALSO BASED ON ROWID
    DBMS_PARALLEL_EXECUTE.CREATE_CHUNKS_BY_SQL(
        TASK_NAME => TASK,
        SQL_STMT  =>'WITH GPRS AS (
                     SELECT /*+ parallel(16) */ NTILE(12) OVER (ORDER BY ROWNUM) GRP
                     FROM ' ||TABLE_NAME||
                    ' T )
                    SELECT MIN(ROWNUM) START_ID, MAX(ROWNUM) END_ID FROM GPRS
                    GROUP BY GRP',
        BY_ROWID  =>FALSE
    );

    --FORMING PLSQL TO CALL
    PLSQL := 'BEGIN GENERATE_CSV_PARALLEL(:START_ID,:END_ID,'||WS_QUOTE||TABLE_NAME||WS_QUOTE||','||WS_QUOTE||TABLE_CATEGORY||WS_QUOTE||'); END;';
    --RUNNING THE TASK AND ASSIGNING PARALLELISIM, THIS DEPENDS ON CPU AND DB PARAMETERS, EXAMPLE WITH 12 CHUNCKS
    DBMS_PARALLEL_EXECUTE.RUN_TASK(
        TASK_NAME     => TASK,
        SQL_STMT      => PLSQL,
        LANGUAGE_FLAG => DBMS_SQL.NATIVE,
        PARALLEL_LEVEL=> 12
    );

    --DROP THE TASK AFTER COMPLETED
    DBMS_PARALLEL_EXECUTE.DROP_TASK(TASK_NAME => TASK);
DBMS_OUTPUT.PUT_LINE('END TIME: '||TO_CHAR(SYSDATE,'YYYY-MM-DD HH24:MI:SS'));   
END;