-- The first few things should be clear what they're doing
-- We're going to be importing and exporting csv files
--.headers on
.mode csv
-- Make the tables
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

-- /tmp was filling up - the PRAGMA seems to take care of that
pragma temp_store = MEMORY;
-- Bring the data in
.import prdn_eins.csv ein_data
.import prdn_piks.csv pik_data
.import ../inData/assignee_info.csv assignee_info
.import ../inData/prdn_metadata.csv prdn_metadata

-- make our indexes
CREATE INDEX
    ein_idx_prdn_ein_firmid
ON
    ein_data(prdn, ein, firmid);

CREATE INDEX
    pik_idx_prdn_ein_firmid
ON
    pik_data(prdn, ein, firmid);

CREATE INDEX
    ein_idx_prdn_as
ON
    ein_data(prdn, ass_seq);

CREATE INDEX
    ass_info_prdn_as
ON
    assignee_info(prdn, ass_seq);
    
CREATE INDEX
    prdn_metadata_main_idx
ON
    prdn_metadata(prdn);


---- BEGIN finding patterns ---------------------------------------------------

-- Closed loops (A1, A2, A3)
---- A1: paths close on a shared ein and firmid
CREATE TABLE closed_paths_A1_TB AS
SELECT 
    pik_data.prdn AS pik_prdn, 
    ein_data.prdn AS ass_prdn, 
    pik_data.app_yr AS app_yr,
    ein_data.grant_yr AS grant_yr,
    ein_data.ass_seq AS ass_seq,
    pik_data.inv_seq AS inv_seq,
    pik_data.pik AS pik,
    ein_data.crosswalk_yr AS crosswalk_yr,
    pik_data.emp_yr AS emp_yr,
    pik_data.ein AS pik_ein,
    ein_data.ein AS ass_ein,
    pik_data.firmid AS pik_firmid,
    ein_data.firmid AS ass_firmid,
    ein_data.pass_no AS pass_no,
    pik_data.app_yr AS app_yr
FROM pik_data
INNER JOIN ein_data
USING (prdn, ein, firmid)
WHERE ein_data.firmid != '';

CREATE INDEX
    closed_paths_A1_TB_idx
ON
    closed_paths_A1_TB(ass_prdn, ass_seq);
    
ALTER TABLE 
    closed_paths_A1_TB
ADD COLUMN
    assignee_country;
UPDATE
    closed_paths_A1_TB
SET
    assignee_country = 
    (
        SELECT ass_ctry
        FROM assignee_info
        WHERE
            assignee_info.prdn = closed_paths_A1_TB.ass_prdn AND 
            assignee_info.ass_seq = closed_paths_A1_TB.ass_seq
    );
ALTER TABLE 
    closed_paths_A1_TB
ADD COLUMN
    assignee_state;
UPDATE
    closed_paths_A1_TB
SET
    assignee_state = 
    (
        SELECT ass_st
        FROM assignee_info
        WHERE
            assignee_info.prdn = closed_paths_A1_TB.ass_prdn AND 
            assignee_info.ass_seq = closed_paths_A1_TB.ass_seq
    );
ALTER TABLE 
    closed_paths_A1_TB
ADD COLUMN
    assignee_type;
UPDATE
    closed_paths_A1_TB
SET
    assignee_type = 
    (
        SELECT ass_type
        FROM assignee_info
        WHERE
            assignee_info.prdn = closed_paths_A1_TB.ass_prdn AND 
            assignee_info.ass_seq = closed_paths_A1_TB.ass_seq
    );
ALTER TABLE 
    closed_paths_A1_TB
ADD COLUMN
    us_inventor_flag;
UPDATE
    closed_paths_A1_TB
SET
    us_inventor_flag =
    (
        SELECT us_inventor_flag
        FROM prdn_metadata
        WHERE
            prdn_metadata.prdn = closed_paths_A1_TB.ass_prdn
    );
ALTER TABLE 
    closed_paths_A1_TB
ADD COLUMN
    multiple_assignee_flag;
UPDATE
    closed_paths_A1_TB
SET
    multiple_assignee_flag =
    (
        SELECT num_assg
        FROM prdn_metadata
        WHERE
            prdn_metadata.prdn = closed_paths_A1_TB.ass_prdn
    );

.output closed_paths_A1.csv
SELECT * FROM closed_paths_A1_TB;
.output stdout

-- Get the lists we're going to delete on
CREATE TABLE prdn_as_A1 AS
SELECT DISTINCT ass_prdn AS prdn, ass_seq
FROM closed_paths_A1_TB;

CREATE INDEX
    indx_2
ON
    prdn_as_A1(prdn, ass_seq);

DELETE FROM ein_data
WHERE EXISTS (
    SELECT * 
    FROM prdn_as_A1
    WHERE prdn_as_A1.prdn = ein_data.prdn
    AND prdn_as_A1.ass_seq = ein_data.ass_seq
);
DROP TABLE prdn_as_A1;
-- clean things up
VACUUM;


---- A2: paths close on a shared firmid
CREATE TABLE closed_paths_A2_TB AS
SELECT
    pik_data.prdn AS pik_prdn, 
    ein_data.prdn AS ass_prdn, 
    pik_data.app_yr AS app_yr,
    ein_data.grant_yr AS grant_yr,
    ein_data.ass_seq AS ass_seq,
    pik_data.inv_seq AS inv_seq,
    pik_data.pik AS pik,
    ein_data.crosswalk_yr AS crosswalk_yr,
    pik_data.emp_yr AS emp_yr,
    pik_data.ein AS pik_ein,
    ein_data.ein AS ass_ein,
    pik_data.firmid AS pik_firmid,
    ein_data.firmid AS ass_firmid,
    ein_data.pass_no AS pass_no,
    pik_data.app_yr AS app_yr
FROM pik_data
INNER JOIN ein_data
USING (prdn, firmid)
WHERE ein_data.firmid != '';

CREATE INDEX
    closed_paths_A2_TB_idx
ON
    closed_paths_A2_TB(ass_prdn, ass_seq);
    
ALTER TABLE 
    closed_paths_A2_TB
ADD COLUMN
    assignee_country;
UPDATE
    closed_paths_A2_TB
SET
    assignee_country = 
    (
        SELECT ass_ctry
        FROM assignee_info
        WHERE
            assignee_info.prdn = closed_paths_A2_TB.ass_prdn AND 
            assignee_info.ass_seq = closed_paths_A2_TB.ass_seq
    );
ALTER TABLE 
    closed_paths_A2_TB
ADD COLUMN
    assignee_state;
UPDATE
    closed_paths_A2_TB
SET
    assignee_state = 
    (
        SELECT ass_st
        FROM assignee_info
        WHERE
            assignee_info.prdn = closed_paths_A2_TB.ass_prdn AND 
            assignee_info.ass_seq = closed_paths_A2_TB.ass_seq
    );
ALTER TABLE 
    closed_paths_A2_TB
ADD COLUMN
    assignee_type;
UPDATE
    closed_paths_A2_TB
SET
    assignee_type = 
    (
        SELECT ass_type
        FROM assignee_info
        WHERE
            assignee_info.prdn = closed_paths_A2_TB.ass_prdn AND 
            assignee_info.ass_seq = closed_paths_A2_TB.ass_seq
    );
ALTER TABLE 
    closed_paths_A2_TB
ADD COLUMN
    us_inventor_flag;
UPDATE
    closed_paths_A2_TB
SET
    us_inventor_flag =
    (
        SELECT us_inventor_flag
        FROM prdn_metadata
        WHERE
            prdn_metadata.prdn = closed_paths_A2_TB.ass_prdn
    );
ALTER TABLE 
    closed_paths_A2_TB
ADD COLUMN
    multiple_assignee_flag;
UPDATE
    closed_paths_A2_TB
SET
    multiple_assignee_flag =
    (
        SELECT num_assg
        FROM prdn_metadata
        WHERE
            prdn_metadata.prdn = closed_paths_A2_TB.ass_prdn
    );

    
.output closed_paths_A2.csv
SELECT * FROM closed_paths_A2_TB;
.output stdout

-- Get the lists we're going to delete on
CREATE TABLE prdn_as_A2 AS
SELECT DISTINCT ass_prdn AS prdn, ass_seq
FROM closed_paths_A2_TB;

CREATE INDEX
    indx_2
ON
    prdn_as_A2(prdn, ass_seq);

DELETE FROM ein_data
WHERE EXISTS (
    SELECT * 
    FROM prdn_as_A2
    WHERE prdn_as_A2.prdn = ein_data.prdn
    AND prdn_as_A2.ass_seq = ein_data.ass_seq
);
DROP TABLE prdn_as_A2;
-- clean things up
VACUUM;


---- A3: paths close on a shared ein
CREATE TABLE closed_paths_A3_TB AS
SELECT 
    pik_data.prdn AS pik_prdn, 
    ein_data.prdn AS ass_prdn, 
    pik_data.app_yr AS app_yr,
    ein_data.grant_yr AS grant_yr,
    ein_data.ass_seq AS ass_seq,
    pik_data.inv_seq AS inv_seq,
    pik_data.pik AS pik,
    ein_data.crosswalk_yr AS crosswalk_yr,
    pik_data.emp_yr AS emp_yr,
    pik_data.ein AS pik_ein,
    ein_data.ein AS ass_ein,
    pik_data.firmid AS pik_firmid,
    ein_data.firmid AS ass_firmid,
    ein_data.pass_no AS pass_no,
    pik_data.app_yr AS app_yr
FROM pik_data
INNER JOIN ein_data
USING (prdn, ein)
WHERE ein_data.firmid != '';

CREATE INDEX
    closed_paths_A3_TB_idx
ON
    closed_paths_A3_TB(ass_prdn, ass_seq);
    
ALTER TABLE 
    closed_paths_A3_TB
ADD COLUMN
    assignee_country;
UPDATE
    closed_paths_A3_TB
SET
    assignee_country = 
    (
        SELECT ass_ctry
        FROM assignee_info
        WHERE
            assignee_info.prdn = closed_paths_A3_TB.ass_prdn AND 
            assignee_info.ass_seq = closed_paths_A3_TB.ass_seq
    );
ALTER TABLE 
    closed_paths_A3_TB
ADD COLUMN
    assignee_state;
UPDATE
    closed_paths_A3_TB
SET
    assignee_state = 
    (
        SELECT ass_st
        FROM assignee_info
        WHERE
            assignee_info.prdn = closed_paths_A3_TB.ass_prdn AND 
            assignee_info.ass_seq = closed_paths_A3_TB.ass_seq
    );
ALTER TABLE 
    closed_paths_A3_TB
ADD COLUMN
    assignee_type;
UPDATE
    closed_paths_A3_TB
SET
    assignee_type = 
    (
        SELECT ass_type
        FROM assignee_info
        WHERE
            assignee_info.prdn = closed_paths_A3_TB.ass_prdn AND 
            assignee_info.ass_seq = closed_paths_A3_TB.ass_seq
    );
ALTER TABLE 
    closed_paths_A3_TB
ADD COLUMN
    us_inventor_flag;
UPDATE
    closed_paths_A3_TB
SET
    us_inventor_flag =
    (
        SELECT us_inventor_flag
        FROM prdn_metadata
        WHERE
            prdn_metadata.prdn = closed_paths_A3_TB.ass_prdn
    );
ALTER TABLE 
    closed_paths_A3_TB
ADD COLUMN
    multiple_assignee_flag;
UPDATE
    closed_paths_A3_TB
SET
    multiple_assignee_flag =
    (
        SELECT num_assg
        FROM prdn_metadata
        WHERE
            prdn_metadata.prdn = closed_paths_A3_TB.ass_prdn
    );
    
.output closed_paths_A3.csv
SELECT * FROM closed_paths_A3_TB;
.output stdout

-- Get the lists we're going to delete on
CREATE TABLE prdn_as_A3 AS
SELECT DISTINCT ass_prdn AS prdn, ass_seq
FROM closed_paths_A3_TB;
CREATE INDEX
    indx_2
ON
    prdn_as_A3(prdn, ass_seq);

DELETE FROM ein_data
WHERE EXISTS (
    SELECT * 
    FROM prdn_as_A3
    WHERE prdn_as_A3.prdn = ein_data.prdn
    AND prdn_as_A3.ass_seq = ein_data.ass_seq
);
DROP TABLE prdn_as_A3;
---- END finding patterns -----------------------------------------------------

-- clean things up
DROP TABLE closed_paths_A1_TB;
DROP TABLE closed_paths_A2_TB;
DROP TABLE closed_paths_A3_TB;
DROP INDEX ein_idx_prdn_as;
VACUUM;
