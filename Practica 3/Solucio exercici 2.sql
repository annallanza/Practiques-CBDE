--esquema
-- DROP TABLE poll_answers
/*
 CREATE TABLE poll_answers(
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
*/

---------- DELETE THE ORIGINAL TABLE AND CREATE A TABLE CONTAINING 
---------- NESTED TABLES (AND THE NEEDED COLLECTION TYPES)

CREATE TYPE pobl_edat_cand_val_t AS OBJECT (
   pobl  INTEGER,
   edat  INTEGER,
   cand  INTEGER,
   val  INTEGER
);

CREATE TYPE pobl_edat_cand_val_tab IS TABLE OF pobl_edat_cand_val_t;

CREATE TYPE respostes_t AS OBJECT (
   resposta_1 VARCHAR2(10),
   resposta_2 VARCHAR2(10),
   resposta_3 VARCHAR2(10),
   resposta_4 VARCHAR2(10),
   resposta_5 VARCHAR2(10)
);

CREATE TYPE respostes_tab IS TABLE OF respostes_t;


CREATE TABLE poll_answers (
   ref  INTEGER,
   pobl_edat_cand_val pobl_edat_cand_val_tab,
   respostes respostes_tab
) NESTED TABLE pobl_edat_cand_val STORE AS poll_answers_pecv
NESTED TABLE respostes STORE AS poll_answers_respostes
PCTFREE 0 ENABLE ROW MOVEMENT;

/*
INSERT INTO poll_answers(ref,pobl_edat_cand_val,respostes) 
	VALUES (
    1, 
    pobl_edat_cand_val_tab(pobl_edat_cand_val_t(1, 2, 3, 4)), 
    respostes_tab(respostes_t(1,2,3,4,5))     
  );
 
 SELECT a.REF, pecv.pobl, pecv.edat, pecv.cand, pecv.val from poll_answers a, table (a.pobl_edat_cand_val) pecv, table (a.respostes) re;
*/

-------- MODIFY THE INSERTS SCRIPT TO ADAPT IT TO YOUR OBJECT-RELATIONAL SCHEMA
----------------------------- INSERTS SCRIPT
DECLARE
  j INTEGER;
  maxTuples CONSTANT INTEGER := 20000;
  maxPobl CONSTANT INTEGER := 200;
  maxEdat CONSTANT INTEGER := 100;  
  maxCand CONSTANT INTEGER := 10;
  maxVal CONSTANT INTEGER := 10;  
BEGIN
DBMS_RANDOM.seed(0);
-- Insertions 
FOR j IN 1..(maxTuples) LOOP
    INSERT INTO poll_answers(ref,pobl_edat_cand_val,respostes)
    VALUES (
    j, 
    pobl_edat_cand_val_tab(pobl_edat_cand_val_t(dbms_random.value(1, maxPobl), dbms_random.value(1,maxEdat), dbms_random.value(1, maxCand), dbms_random.value(1, maxVal))), 
    respostes_tab(respostes_t(LPAD(dbms_random.string('U',50),10,'-'), LPAD(dbms_random.string('U',50),10,'-'), LPAD(dbms_random.string('U',50),10,'-'), LPAD(dbms_random.string('U',50),10,'-'), LPAD(dbms_random.string('U',50),10,'-')))
  );
END LOOP;
END;

COMMIT;

ALTER TABLE poll_answers SHRINK SPACE;
ALTER TABLE poll_answers MINIMIZE RECORDS_PER_BLOCK;

------ IF YOU WANT TO CREATE ANY OTHER INDEX DO IT HERE
/*
(20%) SELECT pobl, MIN(edat), MAX(edat), COUNT(*) FROM poll_answers GROUP BY pobl;
(30%) SELECT pobl, edat, cand, MAX(val), MIN(val), AVG(val) FROM poll_answers GROUP BY pobl, edat, cand;
(50%) SELECT cand, AVG(val) FROM poll_answers GROUP BY cand;
 */

CREATE BITMAP INDEX bitmap_pobl ON poll_answers_pecv(pobl) PCTFREE 0;
CREATE BITMAP INDEX bitmap_edat ON poll_answers_pecv(edat) PCTFREE 0;
CREATE BITMAP INDEX bitmap_cand ON poll_answers_pecv(cand,val) PCTFREE 0;


------ NOW, CREATE THE THREE NEEDED VIEWS ADAPTING THE THREE QUERIES IN THE STATEMENT
------ TO YOUR OBJECT-RELATIONAL SCHEMA

CREATE VIEW view1 AS 
SELECT pecv.pobl AS a, MIN(pecv.edat) AS b, MAX(pecv.edat) AS c, COUNT(*) AS d 
FROM poll_answers pa, TABLE(pa.pobl_edat_cand_val) pecv
GROUP BY pecv.pobl;

CREATE VIEW view2 AS 
SELECT pecv.pobl AS a, pecv.edat AS b, pecv.cand AS c, MAX(pecv.val) AS d, MIN(pecv.val) AS e, AVG(pecv.val) AS f 
FROM poll_answers pa, TABLE(pa.pobl_edat_cand_val) pecv
GROUP BY pecv.pobl, pecv.edat, pecv.cand;

CREATE VIEW view3 AS 
SELECT pecv.cand AS a, AVG(pecv.val) AS b
FROM poll_answers pa, TABLE(pa.pobl_edat_cand_val) pecv
GROUP BY pecv.cand;


------------------------------------------- Update Statistics 
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



-- NO NEED TO MODIFY THE REST OF THIS CODE. IT RELIES ON THE VIEWS ABOVE CREATED
-- BE CAREFUL AND DO NOT REUSE THIS BIT OF CODE FROM PREVIOUS EXERCISES AS IT IS DIFFERENT
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

SELECT MAX(LENGTH(a||b||c||d)) INTO r FROM (SELECT a, b, c, d
FROM view1
);
 
select value INTO i1 from v$statname c, v$sesstat a where a.statistic# = c.statistic# and sys_context('USERENV','SID') = a.sid and c.name in ('consistent gets');

SELECT MAX(LENGTH(a||b||c||d||e||f)) INTO r FROM (SELECT a, b, c, d, e, f
FROM view2
);

select value INTO i2 from v$statname c, v$sesstat a where a.statistic# = c.statistic# and sys_context('USERENV','SID') = a.sid and c.name in ('consistent gets');

SELECT MAX(LENGTH(a||b)) INTO r FROM (SELECT a,b
FROM view3
);

select value INTO i3 from v$statname c, v$sesstat a where a.statistic# = c.statistic# and sys_context('USERENV','SID') = a.sid and c.name in ('consistent gets');

INSERT INTO measure (id,weight,i,f) VALUES (1,0.2,i0,i1);
INSERT INTO measure (id,weight,i,f) VALUES (2,0.3,i1,i2);
INSERT INTO measure (id,weight,i,f) VALUES (3,0.5,i2,i3);
END;

--SELECT * FROM MEASURE;
SELECT SUM((f-i)*weight) FROM measure;
SELECT SUM(BLOCKS) FROM USER_TS_QUOTAS;
--DROP TABLE measure PURGE;
