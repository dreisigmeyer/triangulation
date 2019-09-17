.mode csv
.headers on
pragma temp_store = MEMORY;

.import ../outData/a1_models.csv a1_models
.import ../outData/a2_models.csv a2_models
.import ../outData/a3_models.csv a3_models
.import ../outData/b1_models.csv b1_models  
.import ../outData/b2_models.csv b2_models
.import ../outData/c1_models.csv c1_models
.import ../outData/c2_models.csv c2_models
.import ../outData/c3_models.csv c3_models
.import ../outData/e1_models.csv e1_models
.import ../outData/e2_models.csv e2_models
.import ../inData/assignee_76_16.csv assignee_name_data


CREATE TABLE name_match (
    xml_pat_num TEXT NOT NULL,
    uspto_pat_num TEXT NOT NULL,
    assg_num INTEGER NOT NULL,
    grant_yr INTEGER NOT NULL,
    zip3_flag INTEGER NOT NULL,
    ein TEXT NOT NULL,
    firmid TEXT NOT NULL,
    pass_no INTEGER NOT NULL,
    br_year INTEGER NOT NULL
);
CREATE TABLE prdn_metadata (
    prdn TEXT NOT NULL,                                                                                                                                                          
    grant_yr INTEGER NOT NULL,                                                                                                                                                     
    app_yr INTEGER,
    num_assg INTEGER,
    us_inv_flag INTEGER
);
CREATE TABLE assignee_info (
    prdn TEXT NOT NULL,
    assg_seq INTEGER NOT NULL,
    assg_type TEXT,
    assg_st TEXT,
    assg_ctry TEXT
);

-- from the hand-corrected D2 models
CREATE TABLE assg_name_firmid (
    name TEXT NOT NULL,
    yr INT,
    firmid TEXT NOT NULL,
    UNIQUE(name, yr, firmid)
);

.headers off
.import ../inData/prdn_metadata.csv prdn_metadata
.import ../inData/assignee_info.csv assignee_info
.import ../inData/name_match.csv name_match
.import ../inData/assg_yr_firmid.csv assg_name_firmid
.headers on

CREATE INDEX a1_models_indx
ON a1_models (prdn, firmid, assg_seq);

CREATE INDEX a2_models_indx
ON a2_models (prdn, firmid, assg_seq);

CREATE INDEX a3_models_indx
ON a3_models (prdn, firmid, assg_seq);

CREATE INDEX b1_models_indx
ON b1_models (prdn, assg_seq);

CREATE INDEX b2_models_indx
ON b2_models (prdn, assg_seq);

CREATE INDEX c1_models_indx
ON c1_models (prdn);

CREATE INDEX c2_models_indx
ON c2_models (prdn);

CREATE INDEX c3_models_indx
ON c3_models (prdn);

CREATE INDEX e1_models_indx
ON e1_models (prdn, assg_seq);

CREATE INDEX e2_models_indx
ON e2_models (prdn);

CREATE INDEX assignee_name_data_indx
ON assignee_name_data (xml_pat_num, assg_num);

CREATE INDEX name_match_indx
ON name_match (xml_pat_num, assg_num);

CREATE INDEX
    prdn_metadata_main_idx
ON
    prdn_metadata(prdn);
    
CREATE INDEX
    assg_info_prdn_as
ON
    assignee_info(prdn, assg_seq);

CREATE TABLE name_match_prdn_assg_num (
    prdn TEXT NOT NULL,
    assg_num INTEGER NOT NULL,
    UNIQUE (prdn, assg_num)
);

INSERT INTO name_match_prdn_assg_num
SELECT DISTINCT xml_pat_num, assg_num 
FROM name_match;

CREATE TABLE firmid_seq_data (
    prdn TEXT NOT NULL,
    assg_seq INTEGER NOT NULL,
    firmid TEXT NOT NULL,
    UNIQUE (prdn, assg_seq, firmid)
);

INSERT INTO firmid_seq_data
SELECT DISTINCT prdn, assg_seq, firmid
FROM a1_models;
    
INSERT INTO firmid_seq_data
SELECT DISTINCT prdn, assg_seq, firmid
FROM a2_models;

INSERT INTO firmid_seq_data
SELECT DISTINCT prdn, assg_seq, firmid
FROM a3_models;

CREATE TABLE firmid_name_data (
    firm_name TEXT NOT NULL,
    firmid TEXT NOT NULL,
    UNIQUE (firm_name, firmid)
);

INSERT INTO firmid_name_data
SELECT DISTINCT assignee_name_data.name, firmid_seq_data.firmid
FROM assignee_name_data, firmid_seq_data
WHERE 
    assignee_name_data.xml_pat_num = firmid_seq_data.prdn AND
    assignee_name_data.assg_num = firmid_seq_data.assg_seq;
    
DELETE FROM firmid_name_data
WHERE firm_name IN (
    SELECT firm_name
    FROM firmid_name_data
    GROUP BY firm_name
    HAVING COUNT(*) > 1
);

DELETE FROM firmid_name_data
WHERE firm_name IN (
    SELECT firm_name
    FROM firmid_name_data
    WHERE firmid = ''
);

CREATE TABLE possible_d_models (
    prdn TEXT NOT NULL,
    count TEXT DEFAULT '',
    assg_seq INTEGER NOT NULL,
    crosswalk_yr TEXT DEFAULT '',
    emp_yr TEXT DEFAULT '',
    firmid TEXT,
    grant_yr INTEGER NOT NULL,
    app_yr INTEGER,
    assg_ctry TEXT,
    assg_st TEXT,
    assg_type TEXT,     
    us_inventor_flag INTEGER,
    multiple_assignee_flag INTEGER,
    model TEXT DEFAULT 'D1',
    unique_firm_id TEXT DEFAULT '',
    name TEXT NOT NULL
);

INSERT INTO possible_d_models (
    prdn, assg_seq, grant_yr, name,
    assg_type, assg_st, assg_ctry,
    app_yr, multiple_assignee_flag, us_inventor_flag
)
SELECT 
    assignee_name_data.xml_pat_num, 
    assignee_name_data.assg_num, 
    assignee_name_data.grant_yr, 
    assignee_name_data.name,
    assignee_info.assg_type,
    assignee_info.assg_st,
    assignee_info.assg_ctry,
    prdn_metadata.app_yr,
    prdn_metadata.num_assg,
    prdn_metadata.us_inv_flag
FROM 
    assignee_name_data,
    assignee_info,
    prdn_metadata
WHERE assignee_name_data.name IN (
    SELECT firm_name
    FROM firmid_name_data) AND
    assignee_name_data.xml_pat_num = assignee_info.prdn AND
    assignee_name_data.assg_num = assignee_info.assg_seq AND
    assignee_name_data.xml_pat_num = prdn_metadata.prdn;

UPDATE possible_d_models
SET firmid = (
    SELECT firmid_name_data.firmid
    FROM firmid_name_data
    WHERE possible_d_models.name = firmid_name_data.firm_name
);

CREATE INDEX possible_d_models_indx
ON possible_d_models (prdn, assg_seq, name);

DELETE FROM possible_d_models
WHERE NOT EXISTS (
    SELECT *
    FROM name_match_prdn_assg_num
    WHERE
        name_match_prdn_assg_num.prdn = possible_d_models.prdn
        AND name_match_prdn_assg_num.assg_num = possible_d_models.assg_seq
);

DELETE FROM possible_d_models
WHERE EXISTS (
    SELECT *
    FROM a1_models
    WHERE
        a1_models.prdn = possible_d_models.prdn
);

DELETE FROM possible_d_models
WHERE EXISTS (
    SELECT *
    FROM a2_models
    WHERE
        a2_models.prdn = possible_d_models.prdn
);

DELETE FROM possible_d_models
WHERE EXISTS (
    SELECT *
    FROM a3_models
    WHERE
        a3_models.prdn = possible_d_models.prdn
);

DELETE FROM possible_d_models
WHERE EXISTS (
    SELECT *
    FROM b1_models
    WHERE
        b1_models.prdn = possible_d_models.prdn
);

DELETE FROM possible_d_models
WHERE EXISTS (
    SELECT *
    FROM b2_models
    WHERE
        b2_models.prdn = possible_d_models.prdn
);

DELETE FROM possible_d_models
WHERE EXISTS (
    SELECT *
    FROM c1_models
    WHERE
        c1_models.prdn = possible_d_models.prdn
);

DELETE FROM possible_d_models
WHERE EXISTS (
    SELECT *
    FROM c2_models
    WHERE
        c2_models.prdn = possible_d_models.prdn
);

DELETE FROM possible_d_models
WHERE EXISTS (
    SELECT *
    FROM c3_models
    WHERE
        c3_models.prdn = possible_d_models.prdn
);

DELETE FROM possible_d_models
WHERE EXISTS (
    SELECT *
    FROM e1_models
    WHERE
        e1_models.prdn = possible_d_models.prdn
);

DELETE FROM possible_d_models
WHERE EXISTS (
    SELECT *
    FROM e2_models
    WHERE
        e2_models.prdn = possible_d_models.prdn
);

.output d1_models.csv
SELECT DISTINCT * 
FROM possible_d_models 
WHERE firmid !='';
.output stdout

--/*
-- D2 models
.import ./d1_models.csv d1_models

CREATE INDEX d1_final_indx
ON d1_models (prdn, firmid, assg_seq);

DROP TABLE possible_d_models;

CREATE TABLE possible_d_models (
    prdn TEXT NOT NULL,
    count TEXT DEFAULT '',
    assg_seq INTEGER NOT NULL,
    crosswalk_yr TEXT DEFAULT '',
    emp_yr TEXT DEFAULT '',
    firmid TEXT DEFAULT '',
    grant_yr INTEGER NOT NULL,
    app_yr INTEGER,
    assg_ctry TEXT,
    assg_st TEXT,
    assg_type TEXT,     
    us_inventor_flag INTEGER,
    multiple_assignee_flag INTEGER,
    model TEXT DEFAULT 'D2',
    unique_firm_id TEXT DEFAULT '',
    name TEXT NOT NULL
);

INSERT INTO possible_d_models (
    prdn, assg_seq, grant_yr, name,
    assg_type, assg_st, assg_ctry,
    app_yr, multiple_assignee_flag, us_inventor_flag
)
SELECT 
    assignee_name_data.xml_pat_num, 
    assignee_name_data.assg_num, 
    assignee_name_data.grant_yr, 
    assignee_name_data.name,
    assignee_info.assg_type,
    assignee_info.assg_st,
    assignee_info.assg_ctry,
    prdn_metadata.app_yr,
    prdn_metadata.num_assg,
    prdn_metadata.us_inv_flag
FROM 
    assignee_name_data,
    assignee_info,
    prdn_metadata
WHERE
    assignee_name_data.xml_pat_num = assignee_info.prdn AND
    assignee_name_data.assg_num = assignee_info.assg_seq AND
    assignee_name_data.xml_pat_num = prdn_metadata.prdn;

CREATE INDEX possible_d_models_indx
ON possible_d_models (prdn, assg_seq, name);

DELETE FROM possible_d_models
WHERE NOT EXISTS (
    SELECT *
    FROM name_match_prdn_assg_num
    WHERE
        name_match_prdn_assg_num.prdn = possible_d_models.prdn
        AND name_match_prdn_assg_num.assg_num = possible_d_models.assg_seq
);

DELETE FROM possible_d_models
WHERE EXISTS (
    SELECT *
    FROM a1_models
    WHERE
        a1_models.prdn = possible_d_models.prdn
);

DELETE FROM possible_d_models
WHERE EXISTS (
    SELECT *
    FROM a2_models
    WHERE
        a2_models.prdn = possible_d_models.prdn
);

DELETE FROM possible_d_models
WHERE EXISTS (
    SELECT *
    FROM a3_models
    WHERE
        a3_models.prdn = possible_d_models.prdn
);

DELETE FROM possible_d_models
WHERE EXISTS (
    SELECT *
    FROM b1_models
    WHERE
        b1_models.prdn = possible_d_models.prdn
);

DELETE FROM possible_d_models
WHERE EXISTS (
    SELECT *
    FROM b2_models
    WHERE
        b2_models.prdn = possible_d_models.prdn
);

DELETE FROM possible_d_models
WHERE EXISTS (
    SELECT *
    FROM c1_models
    WHERE
        c1_models.prdn = possible_d_models.prdn
);

DELETE FROM possible_d_models
WHERE EXISTS (
    SELECT *
    FROM c2_models
    WHERE
        c2_models.prdn = possible_d_models.prdn
);

DELETE FROM possible_d_models
WHERE EXISTS (
    SELECT *
    FROM c3_models
    WHERE
        c3_models.prdn = possible_d_models.prdn
);

DELETE FROM possible_d_models
WHERE EXISTS (
    SELECT *
    FROM e1_models
    WHERE
        e1_models.prdn = possible_d_models.prdn
);

DELETE FROM possible_d_models
WHERE EXISTS (
    SELECT *
    FROM e2_models
    WHERE
        e2_models.prdn = possible_d_models.prdn
);

CREATE TABLE big_firm_names
AS
    SELECT name
    FROM (
        SELECT name, COUNT(DISTINCT prdn) AS name_count 
        FROM possible_d_models 
        GROUP BY name
    ) subquery_1
    WHERE subquery_1.name_count > 499;

DELETE FROM possible_d_models
WHERE EXISTS (
    SELECT *
    FROM d1_models
    WHERE
        d1_models.prdn = possible_d_models.prdn
);

.output d2_models.csv
SELECT DISTINCT
    possible_d_models.prdn,
    possible_d_models.assg_seq,
    assg_name_firmid.firmid,
    possible_d_models.app_yr,
    possible_d_models.grant_yr,
    possible_d_models.assg_type,     
    possible_d_models.assg_st,
    possible_d_models.assg_ctry,
    possible_d_models.us_inventor_flag,
    possible_d_models.multiple_assignee_flag,
    possible_d_models.crosswalk_yr,
    possible_d_models.emp_yr,
    'D2',
    possible_d_models.unique_firm_id,
    possible_d_models.count,
    possible_d_models.name
FROM 
    possible_d_models,
    assg_name_firmid
WHERE 
    assg_name_firmid.firmid != '' AND
    possible_d_models.name IN (SELECT name FROM big_firm_names) AND
    possible_d_models.grant_yr = assg_name_firmid.yr AND
    possible_d_models.name = assg_name_firmid.name; 
.output stdout
--*/
