.mode csv
.headers on
-- A model FirmID-Year information
CREATE TABLE a_models_info_for_b_models (
    firmid TEXT NOT NULL,
    crosswalk_yr INTEGER NOT NULL,
    UNIQUE (firmid,crosswalk_yr)
);

/*
CREATE TABLE prdn_metadata (
    prdn TEXT NOT NULL,                                                                                                                                                          
    grant_yr INTEGER NOT NULL,                                                                                                                                                     
    app_yr INTEGER,
    num_assg INTEGER,
    us_inventor_flag INTEGER
);
.import ../inData/prdn_metadata.csv prdn_metadata
CREATE INDEX
    prdn_metadata_main_idx
ON
    prdn_metadata(prdn);
*/

-- /tmp was filling up - the PRAGMA seems to take care of that
pragma temp_store = MEMORY;
-- Bring in the data
.import Amodel_firmid_year.csv a_models_info_for_b_models
.headers off
    
---- Create filtered table to find B1 and B2 models
CREATE TABLE b_models AS
SELECT DISTINCT
    ein_data.prdn,
    ein_data.ass_seq,
    ein_data.firmid,
    ein_data.crosswalk_yr,
    ein_data.grant_yr
FROM 
    ein_data,
    (
        SELECT prdn AS prdn_sq1, ass_seq AS ass_seq_sq1, min(pass_no) AS pass_no_sq1 
        FROM ein_data 
        GROUP BY prdn, ass_seq
    ) AS subquery1    
WHERE
    ein_data.firmid != '' AND
    ein_data.prdn = subquery1.prdn_sq1 AND
    ein_data.ass_seq = subquery1.ass_seq_sq1 AND
    ein_data.pass_no = subquery1.pass_no_sq1;
    
    
-- Only want PRDNs without any inventor information: These are the ones that never
-- had a chance to be A models when they grew up.
DELETE FROM b_models
WHERE prdn IN ( 
    SELECT DISTINCT prdn 
    FROM pik_data 
);

---------------------------------------------------------------------------
-- The B1 models    
---- find the firmids that were on a prior granted patent
CREATE TABLE b1_models_holder AS
SELECT 
    b_models.prdn,
    b_models.ass_seq,
    b_models.firmid,
    b_models.grant_yr,
    abs(b_models.crosswalk_yr - b_models.grant_yr) AS abs_diff_yr,
    (b_models.crosswalk_yr - b_models.grant_yr) AS diff_yr,
    b_models.crosswalk_yr
FROM 
    b_models, 
    a_models_info_for_b_models
WHERE
    b_models.firmid = a_models_info_for_b_models.firmid AND
    b_models.crosswalk_yr = a_models_info_for_b_models.crosswalk_yr
ORDER BY
    b_models.prdn,
    b_models.ass_seq,
    abs_diff_yr ASC, 
    diff_yr ASC;

-- Closest to grant year in window ( 0 , -1, 1, -2, 2)
CREATE TABLE b1_models_holder_first_row (
    prdn TEXT NOT NULL,
    ass_seq INTEGER NOT NULL,
    abs_diff_yr INTEGER NOT NULL,
    diff_yr INTEGER NOT NULL,
    counter INTEGER NOT NULL DEFAULT 0,
    UNIQUE (prdn,ass_seq)
);
---- this works because of the way we ordered the data in the b1_models_holder 
---- when it was created
INSERT OR IGNORE INTO b1_models_holder_first_row
    (prdn,ass_seq,abs_diff_yr,diff_yr)
SELECT 
    prdn,ass_seq,abs_diff_yr,diff_yr
FROM 
    b1_models_holder;
    
CREATE INDEX
    temp_idx_b1_first_row
ON
    b1_models_holder_first_row(prdn,ass_seq,abs_diff_yr,diff_yr);
    
CREATE INDEX
    temp_idx_b1_holder
ON
    b1_models_holder(prdn,ass_seq,abs_diff_yr,diff_yr);
    
UPDATE b1_models_holder_first_row
SET counter = (
    SELECT COUNT(*)
    FROM b1_models_holder
    WHERE
        b1_models_holder.prdn = b1_models_holder_first_row.prdn AND
        b1_models_holder.ass_seq = b1_models_holder_first_row.ass_seq AND
        b1_models_holder.abs_diff_yr = b1_models_holder_first_row.abs_diff_yr AND
        b1_models_holder.diff_yr = b1_models_holder_first_row.diff_yr
)
WHERE EXISTS (
    SELECT *
    FROM b1_models_holder
    WHERE
        b1_models_holder.prdn = b1_models_holder_first_row.prdn AND
        b1_models_holder.ass_seq = b1_models_holder_first_row.ass_seq AND
        b1_models_holder.abs_diff_yr = b1_models_holder_first_row.abs_diff_yr AND
        b1_models_holder.diff_yr = b1_models_holder_first_row.diff_yr
);

CREATE TABLE b1_models AS
SELECT 
    b1_models_holder.prdn,
    '' AS count,
    b1_models_holder.ass_seq,
    b1_models_holder.crosswalk_yr,
    '' AS emp_yr,
    b1_models_holder.firmid,
    b1_models_holder.grant_yr,
    prdn_metadata.app_yr AS app_yr,
    assignee_info.ass_ctry,
    assignee_info.ass_st,
    assignee_info.ass_type,
    prdn_metadata.us_inventor_flag AS us_inventor_flag,
    prdn_metadata.num_assg AS multiple_assignee_flag,
    'B1' AS model,
    '' AS unique_firm_id
FROM 
    b1_models_holder, 
    b1_models_holder_first_row,
    assignee_info,
    prdn_metadata
WHERE
    b1_models_holder_first_row.counter = 1 AND
    b1_models_holder.prdn = b1_models_holder_first_row.prdn AND
    b1_models_holder.ass_seq = b1_models_holder_first_row.ass_seq AND
    b1_models_holder.abs_diff_yr = b1_models_holder_first_row.abs_diff_yr AND
    b1_models_holder.diff_yr = b1_models_holder_first_row.diff_yr AND
    b1_models_holder.prdn = assignee_info.prdn AND
    b1_models_holder.ass_seq = assignee_info.ass_seq AND
    b1_models_holder.prdn = prdn_metadata.prdn
ORDER BY
    b1_models_holder.prdn,
    b1_models_holder.ass_seq;
    
.output b1_models.csv
SELECT DISTINCT * FROM b1_models;
.output stdout
VACUUM;

-- The B2 models    
-- Don't consider for a B2 model anything that was considered for a B1 model
DELETE FROM b_models
WHERE EXISTS (
    SELECT *
    FROM b1_models_holder
    WHERE
        b1_models_holder.prdn = b_models.prdn AND
        b1_models_holder.ass_seq = b_models.ass_seq
);

-- This is now all the same as the B1 models execpt that we don't use
-- any a_models_info_for_b_models based constraints in the next query
CREATE TABLE b2_models_holder AS
SELECT 
    b_models.prdn,
    b_models.ass_seq,
    b_models.firmid,
    b_models.grant_yr,
    abs(b_models.crosswalk_yr - b_models.grant_yr) AS abs_diff_yr,
    (b_models.crosswalk_yr - b_models.grant_yr) AS diff_yr,
    b_models.crosswalk_yr
FROM 
    b_models
ORDER BY
    b_models.prdn,
    b_models.ass_seq,
    abs_diff_yr ASC, 
    diff_yr ASC;

-- Closest to grant year in window ( 0 , -1, 1, -2, 2)
CREATE TABLE b2_models_holder_first_row (
    prdn TEXT NOT NULL,
    ass_seq INTEGER NOT NULL,
    abs_diff_yr INTEGER NOT NULL,
    diff_yr INTEGER NOT NULL,
    counter INTEGER NOT NULL DEFAULT 0,
    UNIQUE (prdn,ass_seq)
);
---- this works because of the way we ordered the data in the b2_models_holder 
---- when it was created
INSERT OR IGNORE INTO b2_models_holder_first_row
    (prdn,ass_seq,abs_diff_yr,diff_yr)
SELECT 
    prdn,ass_seq,abs_diff_yr,diff_yr
FROM 
    b2_models_holder;
    
CREATE INDEX
    temp_idx_b2_first_row
ON
    b2_models_holder_first_row(prdn,ass_seq,abs_diff_yr,diff_yr);
    
CREATE INDEX
    temp_idx_b2_holder
ON
    b2_models_holder(prdn,ass_seq,abs_diff_yr,diff_yr);
    
UPDATE b2_models_holder_first_row
SET counter = (
    SELECT COUNT(*)
    FROM b2_models_holder
    WHERE
        b2_models_holder.prdn = b2_models_holder_first_row.prdn AND
        b2_models_holder.ass_seq = b2_models_holder_first_row.ass_seq AND
        b2_models_holder.abs_diff_yr = b2_models_holder_first_row.abs_diff_yr AND
        b2_models_holder.diff_yr = b2_models_holder_first_row.diff_yr
)
WHERE EXISTS (
    SELECT *
    FROM b2_models_holder
    WHERE
        b2_models_holder.prdn = b2_models_holder_first_row.prdn AND
        b2_models_holder.ass_seq = b2_models_holder_first_row.ass_seq AND
        b2_models_holder.abs_diff_yr = b2_models_holder_first_row.abs_diff_yr AND
        b2_models_holder.diff_yr = b2_models_holder_first_row.diff_yr
);
    
CREATE TABLE b2_models AS
SELECT 
    b2_models_holder.prdn,
    '' AS count,
    b2_models_holder.ass_seq,
    b2_models_holder.crosswalk_yr,
    '' AS emp_yr,
    b2_models_holder.firmid,
    b2_models_holder.grant_yr,
    prdn_metadata.app_yr AS app_yr,
    assignee_info.ass_ctry,
    assignee_info.ass_st,
    assignee_info.ass_type,
    prdn_metadata.us_inventor_flag AS us_inventor_flag,
    prdn_metadata.num_assg AS multiple_assignee_flag,
    'B2' AS model,
    '' AS unique_firm_id
FROM 
    b2_models_holder, 
    b2_models_holder_first_row,
    assignee_info,
    prdn_metadata
WHERE
    b2_models_holder_first_row.counter = 1 AND
    b2_models_holder.prdn = b2_models_holder_first_row.prdn AND
    b2_models_holder.ass_seq = b2_models_holder_first_row.ass_seq AND
    b2_models_holder.abs_diff_yr = b2_models_holder_first_row.abs_diff_yr AND
    b2_models_holder.diff_yr = b2_models_holder_first_row.diff_yr AND
    b2_models_holder.prdn = assignee_info.prdn AND
    b2_models_holder.ass_seq = assignee_info.ass_seq AND
    b2_models_holder.prdn = prdn_metadata.prdn
ORDER BY
    b2_models_holder.prdn,
    b2_models_holder.ass_seq;
    
.output b2_models.csv
SELECT DISTINCT * FROM b2_models;
.output stdout

---- END finding patterns -----------------------------------------------------

--  Clean up
/*
DROP TABLE a_models_info_for_b_models;
DROP TABLE b_models;
DROP TABLE b1_models_holder;
DROP TABLE b1_models_holder_first_row;
DROP TABLE b1_models;
DROP TABLE b2_models_holder;
DROP TABLE b2_models_holder_first_row;
DROP TABLE b2_models;
VACUUM;
*/

