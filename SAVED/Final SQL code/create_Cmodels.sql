.mode csv
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
CREATE TABLE assignee_info (
    prdn TEXT NOT NULL,
    ass_seq INTEGER NOT NULL,
    ass_type TEXT,
    ass_st TEXT,
    ass_ctry TEXT
);
CREATE TABLE prdn_metadata (
    prdn TEXT NOT NULL,                                                                                                                                                          
    grant_yr INTEGER NOT NULL,                                                                                                                                                     
    app_yr INTEGER,
    num_assg INTEGER,
    us_inventor_flag INTEGER
);
pragma temp_store = MEMORY;
.import prdn_eins.csv ein_data
.import prdn_piks.csv pik_data
.import ../inData/assignee_info.csv assignee_info
.import ../inData/prdn_metadata.csv prdn_metadata
-- make our indexes
CREATE INDEX
    ein_data_main_idx
ON
    ein_data(prdn);

CREATE INDEX
    pik_data_main_idx
ON
    pik_data(prdn, app_yr, inv_seq, pik, firmid, emp_yr);

CREATE INDEX
    ass_info_prdn
ON
    assignee_info(prdn);
    
CREATE INDEX
    prdn_metadata_main_idx
ON
    prdn_metadata(prdn);


    
---- information about A models
CREATE TABLE a_models_info_for_c_models (
    pik INTEGER NOT NULL,
    employment_yr INTEGER NOT NULL,
    firmid TEXT NOT NULL,
    UNIQUE (pik,employment_yr,firmid)
);
-- Bring in the data
.import Amodel_pik_year_firmid.csv a_models_info_for_c_models
--.headers off

---- potential C models
CREATE TABLE c_models AS
SELECT DISTINCT
    prdn, grant_yr, app_yr, inv_seq, pik, firmid, emp_yr
FROM 
    pik_data
WHERE
    firmid != '';
    
CREATE INDEX
    idx_c_models
ON
    c_models(prdn,pik,firmid,emp_yr);
    
-- Only want PRDNs without any assignee information.
DELETE FROM c_models
WHERE prdn IN ( 
    SELECT DISTINCT prdn 
    FROM ein_data 
);


---------------------------------------------------------------------------
-- The C1 models    
---- find the piks that were on a prior granted patent
CREATE TABLE c1_models_holder AS
SELECT DISTINCT
    c_models.prdn,
    c_models.firmid,
    c_models.emp_yr,
    abs(c_models.emp_yr - c_models.app_yr) AS abs_diff_yr,
    (c_models.emp_yr - c_models.app_yr) AS diff_yr
FROM 
    c_models, 
    a_models_info_for_c_models
WHERE
    c_models.pik = a_models_info_for_c_models.pik AND
    c_models.emp_yr = a_models_info_for_c_models.employment_yr AND
    c_models.firmid = a_models_info_for_c_models.firmid
ORDER BY
    c_models.prdn,
    abs_diff_yr ASC, 
    diff_yr ASC;
    
-- Closest to application year in window ( 0 , -1, 1, -2, 2)
CREATE TABLE c1_models_holder_first_row (
    prdn TEXT NOT NULL,
    abs_diff_yr INTEGER NOT NULL,
    diff_yr INTEGER NOT NULL,
    counter INTEGER NOT NULL DEFAULT 0,
    UNIQUE (prdn)
);
---- this works because of the way we ordered the data in the c1_models_holder 
---- when it was created
INSERT OR IGNORE INTO c1_models_holder_first_row
    (prdn,abs_diff_yr,diff_yr)
SELECT 
    prdn,abs_diff_yr,diff_yr
FROM 
    c1_models_holder;
    
CREATE INDEX
    temp_idx_c1_first_row
ON
    c1_models_holder_first_row(prdn,abs_diff_yr,diff_yr);
    
CREATE INDEX
    temp_idx_c1_holder
ON
    c1_models_holder(prdn,abs_diff_yr,diff_yr);
    
UPDATE c1_models_holder_first_row
SET counter = (
    SELECT COUNT(*)
    FROM c1_models_holder
    WHERE
        c1_models_holder.prdn = c1_models_holder_first_row.prdn AND
        c1_models_holder.abs_diff_yr = c1_models_holder_first_row.abs_diff_yr AND
        c1_models_holder.diff_yr = c1_models_holder_first_row.diff_yr
)
WHERE EXISTS (
    SELECT *
    FROM c1_models_holder
    WHERE
        c1_models_holder.prdn = c1_models_holder_first_row.prdn AND
        c1_models_holder.abs_diff_yr = c1_models_holder_first_row.abs_diff_yr AND
        c1_models_holder.diff_yr = c1_models_holder_first_row.diff_yr
);
    
CREATE TABLE c1_models AS
SELECT 
    c_models.prdn,
    '' AS count,
    assignee_info.ass_seq AS ass_seq,
    '' AS crosswalk_yr,
    c_models.emp_yr,
    c_models.firmid,
    prdn_metadata.grant_yr,
    prdn_metadata.app_yr,
    assignee_info.ass_ctry AS ass_ctry,
    assignee_info.ass_st AS ass_st,
    assignee_info.ass_type AS ass_type,
    prdn_metadata.us_inventor_flag,
    prdn_metadata.num_assg AS multiple_assignee_flag,
    'C1' AS model,
    '' AS unique_firm_id
FROM 
    c_models,
    prdn_metadata,
    c1_models_holder, 
    c1_models_holder_first_row,
    assignee_info
WHERE
    c1_models_holder_first_row.counter = 1 AND
    c1_models_holder.prdn = c1_models_holder_first_row.prdn AND
    c1_models_holder.abs_diff_yr = c1_models_holder_first_row.abs_diff_yr AND
    c1_models_holder.diff_yr = c1_models_holder_first_row.diff_yr AND
    c_models.prdn = c1_models_holder_first_row.prdn AND
    c_models.prdn = assignee_info.prdn AND
    c_models.emp_yr = c1_models_holder.emp_yr AND    
    c_models.firmid = c1_models_holder.firmid AND
    c1_models_holder_first_row.prdn = prdn_metadata.prdn
ORDER BY
    c_models.prdn;

---- this is for the C3 models
CREATE INDEX
    idx_c1_models_prdn
ON
    c1_models(prdn);
    
.output c1_models.csv
SELECT DISTINCT * FROM c1_models;
.output stdout
VACUUM;


-- The C2 models    
---- Don't consider for a C2 model anything that was considered for a C1 model
DELETE FROM c_models
WHERE EXISTS (
    SELECT *
    FROM c1_models_holder
    WHERE
        c1_models_holder.prdn = c_models.prdn
);

CREATE TABLE c2_models_holder AS
SELECT DISTINCT
    c_models.prdn,
    c_models.firmid
FROM 
    c_models, 
    a_models_info_for_c_models
WHERE
    c_models.emp_yr = a_models_info_for_c_models.employment_yr AND
    c_models.firmid = a_models_info_for_c_models.firmid;

CREATE INDEX
    temp_idx_c2_models_holder
ON
    c2_models_holder(prdn,firmid);
    
DELETE FROM c2_models_holder
WHERE c2_models_holder.prdn IN (
    SELECT subquery_1.prdn
    FROM (
        SELECT prdn, count(*) AS counter
        FROM c2_models_holder
        GROUP BY prdn
    ) subquery_1 
    WHERE subquery_1.counter > 1
);
    
CREATE TABLE c2_models_pik_data AS
SELECT 
    pik_data.prdn,                                                                                                                                                          
    pik_data.app_yr,                                                                                                                                                     
    pik_data.inv_seq,                                                                                                                                                    
    pik_data.pik,                                                                                                                                                           
    pik_data.firmid,
    pik_data.grant_yr
FROM 
    pik_data,
    c2_models_holder
WHERE
    pik_data.prdn = c2_models_holder.prdn AND
    pik_data.firmid = c2_models_holder.firmid AND
    pik_data.emp_yr = pik_data.app_yr;
    
ALTER TABLE c2_models_pik_data
ADD COLUMN ui_firm_id TEXT;

CREATE INDEX
    temp_idx_c2_models_pik_data
ON
    c2_models_pik_data(prdn,inv_seq,pik,firmid,ui_firm_id);
    
INSERT INTO c2_models_pik_data 
    (prdn, app_yr, inv_seq, pik, ui_firm_id, grant_yr)
SELECT 
    pik_data.prdn, 
    pik_data.app_yr,
    pik_data.inv_seq,
    pik_data.pik,
    pik_data.firmid,
    pik_data.grant_yr
FROM 
    pik_data,
    c2_models_pik_data
WHERE
    pik_data.prdn = c2_models_pik_data.prdn AND
    pik_data.pik = c2_models_pik_data.pik AND
    pik_data.emp_yr = c2_models_pik_data.app_yr AND
    pik_data.firmid != c2_models_pik_data.firmid;
    
CREATE TABLE c2_models (                                                                                                                                                  
    prdn TEXT NOT NULL,
    firmid TEXT NOT NULL,
    pik TEXT NOT NULL,
    PRIMARY KEY (prdn, firmid, pik)
);

INSERT OR IGNORE INTO c2_models
    (prdn, firmid, pik)
SELECT DISTINCT
    prdn, firmid, pik
FROM 
    c2_models_pik_data;
    
INSERT OR IGNORE INTO c2_models
    (prdn, firmid, pik)
SELECT DISTINCT
    prdn, ui_firm_id, pik
FROM 
    c2_models_pik_data;
    
CREATE TABLE c2_models_out AS
SELECT *, count(*) AS count
FROM c2_models
GROUP BY prdn, pik
HAVING count = 1;

DROP TABLE c2_models;
    
---- this is for the C3 models
CREATE INDEX
    idx_c2_models_out_prdn
ON
    c2_models_out(prdn);
    
CREATE TABLE c2_models AS
SELECT
    c2_models_out.prdn,
    '' AS count,
    assignee_info.ass_seq AS ass_seq,
    '' AS crosswalk_yr,
    prdn_metadata.app_yr AS emp_yr,
    c2_models_out.firmid,
    prdn_metadata.grant_yr,
    prdn_metadata.app_yr,
    assignee_info.ass_ctry AS ass_ctry,
    assignee_info.ass_st AS ass_st,
    assignee_info.ass_type AS ass_type,
    prdn_metadata.us_inventor_flag,
    prdn_metadata.num_assg AS multiple_assignee_flag,
    'C2' AS model,
    '' AS unique_firm_id
FROM 
    c2_models_out,
    prdn_metadata,
    assignee_info
WHERE
    c2_models_out.prdn = prdn_metadata.prdn AND
    c2_models_out.prdn = assignee_info.prdn
ORDER BY
    c2_models_out.prdn;

.output c2_models.csv
SELECT DISTINCT * FROM c2_models;
.output stdout
VACUUM;
   
   
-- The C3 models    
---- Need to recreate c1_models and remove appropriate C1 and C2 cases
DROP TABLE c_models;
CREATE TABLE c_models AS
SELECT DISTINCT
    prdn, grant_yr, app_yr, inv_seq, pik, firmid, emp_yr
FROM 
    pik_data;
    
CREATE INDEX
    idx_c_models
ON
    c_models(prdn,inv_seq,pik,firmid,emp_yr);
    
DELETE FROM c_models
WHERE prdn IN ( 
    SELECT DISTINCT prdn 
    FROM ein_data 
);

---- Only delete if a C1 model
DELETE FROM c_models
WHERE prdn IN (
    SELECT DISTINCT prdn
    FROM c1_models
);

---- Only delete if a C2 model
DELETE FROM c_models
WHERE prdn IN (
    SELECT DISTINCT prdn
    FROM c2_models_out
);

---- Remove prdn-inv_seq pairs if PIK is not unique
CREATE TABLE to_delete_from_c_models AS
SELECT prdn, inv_seq
FROM
(
    SELECT DISTINCT prdn, inv_seq, pik
    FROM c_models
) subquery
GROUP BY prdn, inv_seq
HAVING count(*) > 1;

CREATE INDEX
    idx_to_delete_from_c_models
ON
    to_delete_from_c_models(prdn,inv_seq);
    
DELETE FROM c_models
WHERE EXISTS
(
    SELECT *
    FROM to_delete_from_c_models
    WHERE
        c_models.prdn = to_delete_from_c_models.prdn AND
        c_models.inv_seq = to_delete_from_c_models.inv_seq
);

DROP TABLE to_delete_from_c_models;

---- order by distance from app_yr in (0, -1, 1, -2, 2) order
CREATE TABLE c3_models_holder AS
SELECT
    *,
    abs(emp_yr - app_yr) AS abs_diff_yr,
    (emp_yr - app_yr) AS diff_yr
FROM 
    c_models
ORDER BY
    c_models.prdn,
    abs_diff_yr ASC, 
    diff_yr ASC;
    
-- Closest to grant year in window ( 0 , -1, 1, -2, 2)
CREATE TABLE c3_models_holder_first_row (
    prdn TEXT NOT NULL,
    abs_diff_yr INTEGER NOT NULL,
    diff_yr INTEGER NOT NULL,
    counter INTEGER NOT NULL DEFAULT 0,
    UNIQUE (prdn)
);

INSERT OR IGNORE INTO c3_models_holder_first_row
    (prdn,abs_diff_yr,diff_yr)
SELECT 
    prdn,abs_diff_yr,diff_yr
FROM 
    c3_models_holder;
    
CREATE INDEX
    temp_idx_c3_first_row
ON
    c3_models_holder_first_row(prdn,abs_diff_yr,diff_yr);
    
CREATE INDEX
    temp_idx_c3_holder
ON
    c3_models_holder(prdn,abs_diff_yr,diff_yr);
    
UPDATE c3_models_holder_first_row
SET counter = (
    SELECT COUNT(DISTINCT firmid)
    FROM c3_models_holder
    WHERE
        c3_models_holder.prdn = c3_models_holder_first_row.prdn AND
        c3_models_holder.abs_diff_yr = c3_models_holder_first_row.abs_diff_yr AND
        c3_models_holder.diff_yr = c3_models_holder_first_row.diff_yr
)
WHERE EXISTS (
    SELECT *
    FROM c3_models_holder
    WHERE
        c3_models_holder.prdn = c3_models_holder_first_row.prdn AND
        c3_models_holder.abs_diff_yr = c3_models_holder_first_row.abs_diff_yr AND
        c3_models_holder.diff_yr = c3_models_holder_first_row.diff_yr
);
    
CREATE TABLE c3_models AS
SELECT
    c3_models_holder.prdn,
    '' AS count,
    assignee_info.ass_seq AS ass_seq,
    '' AS crosswalk_yr,
    c3_models_holder.emp_yr,
    c3_models_holder.firmid,
    prdn_metadata.grant_yr,
    prdn_metadata.app_yr,
    assignee_info.ass_ctry AS ass_ctry,
    assignee_info.ass_st AS ass_st,
    assignee_info.ass_type AS ass_type,
    prdn_metadata.us_inventor_flag,
    prdn_metadata.num_assg AS multiple_assignee_flag,
    'C3' AS model,
    '' AS unique_firm_id
FROM 
    c3_models_holder, 
    c3_models_holder_first_row, 
    prdn_metadata,
    assignee_info
WHERE
    c3_models_holder_first_row.counter = 1 AND
    c3_models_holder.prdn = c3_models_holder_first_row.prdn AND
    c3_models_holder.prdn = assignee_info.prdn AND
    c3_models_holder.abs_diff_yr = c3_models_holder_first_row.abs_diff_yr AND
    c3_models_holder.diff_yr = c3_models_holder_first_row.diff_yr AND
    c3_models_holder_first_row.prdn = prdn_metadata.prdn
ORDER BY
    c3_models_holder.prdn;

.output c3_models.csv
SELECT DISTINCT * FROM c3_models;
.output stdout
VACUUM;

---- END finding patterns -----------------------------------------------------
/*
-- Clean up
DROP TABLE c_models;
DROP TABLE c1_models;
DROP TABLE c1_models_holder;
DROP TABLE c1_models_holder_first_row;
DROP TABLE c2_models_holder;
DROP TABLE c2_models_pik_data;
VACUUM;
*/
