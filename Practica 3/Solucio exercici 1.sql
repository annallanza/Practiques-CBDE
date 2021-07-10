CREATE TABLE  poll_answers(
  ref INTEGER,
  pobl INTEGER NOT NULL,
  edat INTEGER NOT NULL,
  cand INTEGER NOT NULL,
  val INTEGER NOT NULL,
  resposta_1 VARCHAR2(10),
  resposta_2 VARCHAR2(10),
  resposta_3 VARCHAR2(10),
  resposta_4 VARCHAR2(10),
  resposta_5 VARCHAR2(10)
)PCTFREE 0 ENABLE ROW MOVEMENT;

----------------------------- INSERTS
DECLARE
  j INTEGER;
  maxTuples CONSTANT INTEGER := 20000;
  maxPobl CONSTANT INTEGER := 120;
  maxEdat CONSTANT INTEGER := 100;  
  maxCand CONSTANT INTEGER := 10;
  maxVal CONSTANT INTEGER := 10;  
BEGIN
DBMS_RANDOM.seed(0);
-- Insertions 
FOR j IN 1..(maxTuples) LOOP
    INSERT INTO poll_answers(ref,pobl,edat,cand,val,resposta_1,resposta_2,resposta_3,resposta_4,resposta_5)--,resposta_6,resposta_7,resposta_8,resposta_9,resposta_10) 
    VALUES (
    j, 
    dbms_random.value(1, maxPobl), 
    dbms_random.value(1,maxEdat), 
    dbms_random.value(1, maxCand),     
    dbms_random.value(1, maxVal),    
    LPAD(dbms_random.string('U',50),10,'-'),
    LPAD(dbms_random.string('U',50),10,'-'),
    LPAD(dbms_random.string('U',50),10,'-'),
    LPAD(dbms_random.string('U',50),10,'-'),
    LPAD(dbms_random.string('U',50),10,'-')
  );
END LOOP;
END;

COMMIT;

ALTER TABLE poll_answers SHRINK SPACE;

ALTER TABLE poll_answers MINIMIZE RECORDS_PER_BLOCK;


---------------------
-- INSERT HERE YOUR SOLUTION
---------------------
/*
(20%) SELECT pobl, MIN(edat), MAX(edat), COUNT(*) FROM poll_answers GROUP BY pobl;
(30%) SELECT pobl, edat, cand, MAX(val), MIN(val), AVG(val) FROM poll_answers GROUP BY pobl, edat, cand;
(50%) SELECT cand, AVG(val) FROM poll_answers GROUP BY cand;

Take into account that you can only use 275 disck blocks in overall.

SELECT * FROM USER_TS_QUOTAS;

--Vistes materialitzades
CREATE MATERIALIZED VIEW VM1 ORGANIZATION HEAP PCTFREE 0 BUILD IMMEDIATE REFRESH COMPLETE ON DEMAND ENABLE QUERY REWRITE AS ( SELECT pobl, MIN(edat), MAX(edat), COUNT(*) FROM poll_answers GROUP BY pobl );
CREATE MATERIALIZED VIEW VM2 ORGANIZATION HEAP PCTFREE 0 BUILD IMMEDIATE REFRESH COMPLETE ON DEMAND ENABLE QUERY REWRITE AS ( SELECT pobl, edat, cand, MAX(val), MIN(val), AVG(val) FROM poll_answers GROUP BY pobl, edat, cand );
CREATE MATERIALIZED VIEW VM3 ORGANIZATION HEAP PCTFREE 0 BUILD IMMEDIATE REFRESH COMPLETE ON DEMAND ENABLE QUERY REWRITE AS ( SELECT cand, AVG(val) FROM poll_answers GROUP BY cand );

DROP MATERIALIZED VIEW VM3

--BMES A POBL
CREATE INDEX BMES ON poll_answers (pobl) PCTFREE 33;

--BMES A POBL,edat,cand
CREATE INDEX BMES ON poll_answers (pobl,edat,cand) PCTFREE 33;

--BMES A cand
CREATE INDEX BMES ON poll_answers (cand) PCTFREE 33;

--BMES A pobl, edat
CREATE INDEX BMES ON poll_answers (pobl,edat) PCTFREE 33;

--BMES A pobl, cand
CREATE INDEX BMES ON poll_answers (pobl,cand) PCTFREE 33;

--BMES A edat, cand
CREATE INDEX BMES ON poll_answers (edat,cand) PCTFREE 33;

--BMES A edat
CREATE INDEX BMES ON poll_answers (edat) PCTFREE 33;

--Bitmap A POBL         
 CREATE BITMAP INDEX bitmap ON poll_answers(pobl) PCTFREE 0;

--Bitmap A POBL,edat,cand         
 CREATE BITMAP INDEX bitmap ON poll_answers(pobl,edat,cand) PCTFREE 0;

--Bitmap A cand         
 CREATE BITMAP INDEX bitmap2 ON poll_answers(cand) PCTFREE 0;

--Bitmap A pobl, edat AQUEEEEEESTTTTTTT         
 CREATE BITMAP INDEX bitmap ON poll_answers(pobl, edat) PCTFREE 0;

--Bitmap A pobl, cand         
 CREATE BITMAP INDEX bitmap ON poll_answers(pobl, cand) PCTFREE 0;

--Bitmap A edat, cand         
 CREATE BITMAP INDEX bitmap ON poll_answers(EDAT, cand) PCTFREE 0;

--Bitmap A edat         
 CREATE BITMAP INDEX bitmap3 ON poll_answers(EDAT) PCTFREE 0;

--Bitmap A val         
 CREATE BITMAP INDEX bitmap4 ON poll_answers(val) PCTFREE 0;
*/

--SOLUCIO FINAL--
CREATE BITMAP INDEX bitmap_pobl ON poll_answers(pobl) PCTFREE 0;
CREATE BITMAP INDEX bitmap_edat ON poll_answers(edat) PCTFREE 0;
CREATE BITMAP INDEX bitmap_cand ON poll_answers(cand) PCTFREE 0;
CREATE BITMAP INDEX bitmap_val ON poll_answers(val) PCTFREE 0;
CREATE MATERIALIZED VIEW VM3 ORGANIZATION HEAP PCTFREE 0 BUILD IMMEDIATE REFRESH COMPLETE ON DEMAND ENABLE QUERY REWRITE AS ( SELECT cand, AVG(val) FROM poll_answers GROUP BY cand );


 ------------------------------------------- Update Statistics ---------------------------
DECLARE
esquema VARCHAR2(100);
CURSOR c IS SELECT TABLE_NAME FROM USER_TABLES UNION SELECT TABLE_NAME FROM USER_OBJECT_TABLES;
BEGIN
SELECT '"'||sys_context('USERENV', 'CURRENT_SCHEMA')||'"' INTO esquema FROM dual;
FOR taula IN c LOOP
  DBMS_STATS.GATHER_TABLE_STATS( 
    ownname => esquema, 
    tabname => taula.table_name, 
    estimate_percent => NULL,
    method_opt =>'FOR ALL COLUMNS SIZE REPEAT',
    granularity => 'GLOBAL',
    cascade => TRUE
    );
  END LOOP;
END;

---------------------------- To check the real costs -------------------------

--DELETE FROM measure;
CREATE TABLE measure (id INTEGER, weight FLOAT, i FLOAT, f FLOAT);

DECLARE 
i0 INTEGER;
i1 INTEGER;
i2 INTEGER;
i3 INTEGER;
r INTEGER;
BEGIN
select value INTO i0 from v$statname c, v$sesstat a where a.statistic# = c.statistic# and sys_context('USERENV','SID') = a.sid and c.name in ('consistent gets');

SELECT MAX(LENGTH(a||b||c||d)) INTO r FROM (SELECT pobl AS a, MIN(edat) AS b, MAX(edat) AS c, COUNT(*) AS d 
FROM poll_answers
GROUP BY pobl
);

select value INTO i1 from v$statname c, v$sesstat a where a.statistic# = c.statistic# and sys_context('USERENV','SID') = a.sid and c.name in ('consistent gets');

SELECT MAX(LENGTH(a||b||c||d||e||f)) INTO r FROM (SELECT pobl AS a, edat AS b, cand AS c, MAX(val) AS d, MIN(val) AS e, AVG(val) AS f 
FROM poll_answers 
GROUP BY pobl, edat, cand
);

select value INTO i2 from v$statname c, v$sesstat a where a.statistic# = c.statistic# and sys_context('USERENV','SID') = a.sid and c.name in ('consistent gets');

SELECT MAX(LENGTH(a||b)) INTO r FROM (SELECT cand AS a, AVG(val) AS b 
FROM poll_answers 
GROUP BY cand
);

select value INTO i3 from v$statname c, v$sesstat a where a.statistic# = c.statistic# and sys_context('USERENV','SID') = a.sid and c.name in ('consistent gets');

INSERT INTO measure (id,weight,i,f) VALUES (1,0.2,i0,i1);
INSERT INTO measure (id,weight,i,f) VALUES (2,0.3,i1,i2);
INSERT INTO measure (id,weight,i,f) VALUES (3,0.5,i2,i3);
END;

--SELECT * FROM MEASURE;
SELECT SUM((f-i)*weight) FROM measure;
SELECT SUM(BLOCKS) FROM USER_TS_QUOTAS;
DROP TABLE measure PURGE;