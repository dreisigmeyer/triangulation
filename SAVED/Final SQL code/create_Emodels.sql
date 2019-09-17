.mode csv
--.headers off
-- A model prdn information
CREATE TABLE a_models_prdns (
    prdn TEXT NOT NULL,
    UNIQUE (prdn)
);
-- /tmp was filling up - the PRAGMA seems to take care of that
pragma temp_store = MEMORY;
-- Bring in the data
.import Amodels_prdns a_models_prdns
---- We need to redo this fresh so as not to have PRDNs filtered
---- out of ein_data
CREATE TABLE pik_data (                                                                                                                                                  
    prdn TEXT NOT NULL,                                                                                                                                                          
    grant_yr INTEGER NOT NULL,                                                                                                                                                     
    app_yr INTEGER NOT NULL,                                                                                                                                                     
    inv_seq INTEGER NOT NULL,                                                                                                                                                    
    pik TEXT NOT NULL,                                                                                                                                                           
    ein TEXT NOT NULL,                                                                                                                                                           
    firmid TEXT NOT NULL,                                                                                                                                                        
    emp_yr INTEGER NOT NULL
);
CREATE TABLE ein_data (
    prdn TEXT NOT NULL,
    grant_yr INTEGER NOT NULL,
    ass_seq INTEGER NOT NULL,
    ein TEXT,
    firmid TEXT NOT NULL,
    crosswalk_yr INTEGER NOT NULL,
    pass_no INTEGER NOT NULL
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
    ass_seq INTEGER NOT NULL,
    ass_type TEXT,
    ass_st TEXT,
    ass_ctry TEXT
);

-- Bring the data in
.import prdn_eins.csv ein_data
.import prdn_piks.csv pik_data
.import ../inData/prdn_metadata.csv prdn_metadata
.import ../inData/assignee_info.csv assignee_info

-- make our indexes
CREATE INDEX
    ein_data_main_idx
ON
    ein_data(prdn, ass_seq, firmid, crosswalk_yr);

CREATE INDEX
    pik_data_main_idx
ON
    pik_data(prdn, firmid, emp_yr);
    
CREATE INDEX
    prdn_metadata_main_idx
ON
    prdn_metadata(prdn);
    
CREATE INDEX
    ass_info_prdn_as
ON
    assignee_info(prdn, ass_seq);
    
---- Create filtered table to find E1 and E2 models
CREATE TABLE e_models_prdns AS
SELECT DISTINCT prdn
FROM ein_data 
INTERSECT
SELECT DISTINCT prdn 
FROM pik_data;

CREATE INDEX
    e_models_prdn_idx
ON
    e_models_prdns(prdn);
    
-- Only want PRDNs with any assignee and inventor information that
-- are not A models
DELETE FROM e_models_prdns
WHERE prdn IN ( 
    SELECT DISTINCT prdn 
    FROM a_models_prdns 
);

DELETE FROM ein_data
WHERE prdn NOT IN ( 
    SELECT prdn
    FROM e_models_prdns
);

VACUUM;

---- E1 Models
CREATE TABLE potential_e1_models AS
SELECT DISTINCT
    prdn,
    ass_seq,
    firmid,
    grant_yr,
    crosswalk_yr,
    abs(ein_data.crosswalk_yr - ein_data.grant_yr) AS abs_diff_yr,
    (ein_data.crosswalk_yr - ein_data.grant_yr) AS diff_yr
FROM
    ein_data
WHERE
    firmid != ''
ORDER BY
    prdn, 
    ass_seq, 
    abs_diff_yr ASC,
    diff_yr ASC;
    
CREATE INDEX
    e1_models_idx
ON
    potential_e1_models(prdn, ass_seq, diff_yr);
-- need a unique firmid for a prdn-assignee pair
CREATE TABLE e1_models_counter AS
SELECT DISTINCT
    prdn,
    ass_seq,
    COUNT(DISTINCT firmid) AS counter
FROM
    potential_e1_models
GROUP BY prdn, ass_seq;

CREATE INDEX
    e1_models_counter_idx
ON
    e1_models_counter(prdn, ass_seq);

DELETE FROM potential_e1_models
WHERE EXISTS (
    SELECT *
    FROM e1_models_counter
    WHERE
        e1_models_counter.counter > 1 AND
        potential_e1_models.prdn = e1_models_counter.prdn AND
        potential_e1_models.ass_seq = e1_models_counter.ass_seq
);

CREATE TABLE e1_models_first_row (
    prdn TEXT NOT NULL,
    ass_seq INTEGER NOT NULL,
    abs_diff_yr INTEGER NOT NULL,
    diff_yr INTEGER NOT NULL,
    UNIQUE (prdn,ass_seq)
);
---- this works because of the way we ordered the data in the potential_e1_models 
---- when it was created
INSERT OR IGNORE INTO e1_models_first_row
    (prdn,ass_seq,abs_diff_yr,diff_yr)
SELECT 
    prdn,ass_seq,abs_diff_yr,diff_yr
FROM 
    potential_e1_models;
    
CREATE TABLE e1_models AS
SELECT
    potential_e1_models.prdn,
    '' AS count,
    potential_e1_models.ass_seq,
    potential_e1_models.crosswalk_yr,
    '' AS emp_yr,
    potential_e1_models.firmid,
    potential_e1_models.grant_yr,
    prdn_metadata.app_yr,
    assignee_info.ass_ctry,
    assignee_info.ass_st,
    assignee_info.ass_type,
    prdn_metadata.us_inv_flag AS us_inventor_flag,
    prdn_metadata.num_assg AS multiple_assignee_flag,
    'E1' AS model,
    '' AS unique_firm_id
FROM
    potential_e1_models, e1_models_first_row, prdn_metadata, assignee_info
WHERE
    potential_e1_models.prdn = e1_models_first_row.prdn AND
    potential_e1_models.ass_seq = e1_models_first_row.ass_seq AND
    potential_e1_models.abs_diff_yr = e1_models_first_row.abs_diff_yr AND
    potential_e1_models.diff_yr = e1_models_first_row.diff_yr AND
    e1_models_first_row.prdn = prdn_metadata.prdn AND
    e1_models_first_row.prdn = assignee_info.prdn AND
    e1_models_first_row.ass_seq = assignee_info.ass_seq;
    

    
.output e1_models.csv
SELECT DISTINCT * FROM e1_models;
.output stdout

--/*
---- E2 Models
DELETE FROM pik_data
WHERE prdn NOT IN ( 
    SELECT DISTINCT prdn
    FROM e_models_prdns
);

DELETE FROM pik_data
WHERE prdn IN ( 
    SELECT DISTINCT prdn
    FROM e1_models
);

VACUUM;

CREATE TABLE potential_e2_models AS
SELECT DISTINCT
    prdn,
    firmid,
    app_yr,
    grant_yr,
    emp_yr,
    abs(pik_data.emp_yr - pik_data.app_yr) AS abs_diff_yr,
    (pik_data.emp_yr - pik_data.app_yr) AS diff_yr
FROM
    pik_data
WHERE
    firmid != ''
ORDER BY
    prdn, 
    abs_diff_yr ASC,
    diff_yr ASC;
    
CREATE INDEX
    e2_models_idx
ON
    potential_e2_models(prdn, diff_yr);
    
CREATE TABLE e2_models_counter AS
SELECT DISTINCT
    prdn,
    COUNT(DISTINCT firmid) AS counter
FROM
    potential_e2_models
GROUP BY prdn;

CREATE INDEX
    e2_models_counter_idx
ON
    e2_models_counter(prdn);

DELETE FROM potential_e2_models
WHERE EXISTS (
    SELECT *
    FROM e2_models_counter
    WHERE
        e2_models_counter.counter > 1 AND
        potential_e2_models.prdn = e2_models_counter.prdn
);

CREATE TABLE e2_models_first_row (
    prdn TEXT NOT NULL,
    abs_diff_yr INTEGER NOT NULL,
    diff_yr INTEGER NOT NULL,
    UNIQUE (prdn)
);

INSERT OR IGNORE INTO e2_models_first_row
    (prdn,abs_diff_yr,diff_yr)
SELECT 
    prdn,abs_diff_yr,diff_yr
FROM 
    potential_e2_models;

CREATE TABLE e2_models AS
SELECT
    potential_e2_models.prdn,
    '' AS count,
    assignee_info.ass_seq AS ass_seq,
    '' AS crosswalk_yr,
    potential_e2_models.emp_yr,
    potential_e2_models.firmid,
    potential_e2_models.grant_yr,
    prdn_metadata.app_yr,
    assignee_info.ass_ctry AS ass_ctry,
    assignee_info.ass_st AS ass_st,
    assignee_info.ass_type AS ass_type,
    prdn_metadata.us_inv_flag AS us_inventor_flag,
    prdn_metadata.num_assg AS multiple_assignee_flag,
    'E2' AS model,
    '' AS unique_firm_id
FROM
    potential_e2_models, e2_models_first_row, prdn_metadata, assignee_info
WHERE
    potential_e2_models.prdn = e2_models_first_row.prdn AND
    e2_models_first_row.prdn = assignee_info.prdn AND
    potential_e2_models.abs_diff_yr = e2_models_first_row.abs_diff_yr AND
    potential_e2_models.diff_yr = e2_models_first_row.diff_yr AND
    e2_models_first_row.prdn = prdn_metadata.prdn;
    
.output e2_models.csv
SELECT DISTINCT * FROM e2_models;
.output stdout

VACUUM;
--*/
