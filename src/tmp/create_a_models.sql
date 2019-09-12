
.mode csv
pragma temp_store = MEMORY;
    
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
    assg_seq INTEGER NOT NULL,
    ein TEXT NOT NULL,
    firmid TEXT NOT NULL,
    cw_yr INTEGER NOT NULL,
    pass_num INTEGER NOT NULL
);
CREATE TABLE assignee_info (
    prdn TEXT NOT NULL,
    assg_seq INTEGER NOT NULL,
    assg_type TEXT,
    assg_st TEXT,
    assg_ctry TEXT
);
CREATE TABLE prdn_metadata (
    prdn TEXT NOT NULL,
    grant_yr INTEGER NOT NULL,
    app_yr INTEGER NOT NULL,
    num_assg INTEGER,
    us_inv_flag INTEGER
);
    
.import ./triangulation/in_data/prdn_piks.csv pik_data
    
.import ./triangulation/in_data/prdn_eins.csv ein_data
    
.import ./triangulation/in_data/assignee_info.csv assignee_info
    
.import ./triangulation/in_data/prdn_metadata.csv prdn_metadata
    
CREATE INDEX
    ein_prdn_ein_firmid_idx
ON
    ein_data(prdn, ein, firmid);
CREATE INDEX
    pik_prdn_ein_firmid_idx
ON
    pik_data(prdn, ein, firmid);
CREATE INDEX
    ein_prdn_as_idx
ON
    ein_data(prdn, assg_seq);
CREATE INDEX
    assg_info_prdn_as_idx
ON
    assignee_info(prdn, assg_seq);
CREATE INDEX
    prdn_metadata_main_idx
ON
    prdn_metadata(prdn);
    
CREATE TABLE closed_paths AS
SELECT
    pik_data.prdn AS pik_prdn,
    ein_data.prdn AS assg_prdn,
    pik_data.app_yr,
    ein_data.grant_yr,
    ein_data.assg_seq,
    pik_data.inv_seq,
    pik_data.pik,
    ein_data.cw_yr,
    pik_data.emp_yr,
    pik_data.ein AS pik_ein,
    ein_data.ein AS assg_ein,
    pik_data.firmid AS pik_firmid,
    ein_data.firmid AS assg_firmid,
    ein_data.pass_num
FROM pik_data
INNER JOIN ein_data
USING (prdn,ein,firmid)
WHERE ein_data.firmid != '';

CREATE INDEX
    closed_paths_idx
ON
    closed_paths(assg_prdn, assg_seq);
    
ALTER TABLE
    closed_paths
ADD COLUMN
    assg_ctry;
UPDATE
    closed_paths
SET
    assg_ctry =
    (
        SELECT assg_ctry
        FROM assignee_info
        WHERE
            assignee_info.prdn = closed_paths.assg_prdn AND
            assignee_info.assg_seq = closed_paths.assg_seq
    );
ALTER TABLE
    closed_paths
ADD COLUMN
    assg_st;
UPDATE
    closed_paths
SET
    assg_st =
    (
        SELECT assg_st
        FROM assignee_info
        WHERE
            assignee_info.prdn = closed_paths.assg_prdn AND
            assignee_info.assg_seq = closed_paths.assg_seq
    );
ALTER TABLE
    closed_paths
ADD COLUMN
    assg_type;
UPDATE
    closed_paths
SET
    assg_type =
    (
        SELECT assg_type
        FROM assignee_info
        WHERE
            assignee_info.prdn = closed_paths.assg_prdn AND
            assignee_info.assg_seq = closed_paths.assg_seq
    );
ALTER TABLE
    closed_paths
ADD COLUMN
    us_inv_flag;
UPDATE
    closed_paths
SET
    us_inv_flag =
    (
        SELECT us_inv_flag
        FROM prdn_metadata
        WHERE
            prdn_metadata.prdn = closed_paths.assg_prdn
    );
ALTER TABLE
    closed_paths
ADD COLUMN
    multiple_assignee_flag;
UPDATE
    closed_paths
SET
    multiple_assignee_flag =
    (
        SELECT num_assg
        FROM prdn_metadata
        WHERE
            prdn_metadata.prdn = closed_paths.assg_prdn
    );
    
CREATE TABLE inv_counts AS
SELECT
    COUNT(DISTINCT inv_seq) AS num_inv,
    assg_prdn,
    assg_seq,
    ABS(cw_yr - grant_yr) AS abs_cw_yr,
    cw_yr,
    ABS(emp_yr - app_yr)  AS abs_emp_yr,
    emp_yr,
    assg_firmid,
    grant_yr,
    app_yr,
    assg_ctry,
    assg_st,
    assg_type,
    us_inv_flag,
    multiple_assignee_flag
FROM closed_paths
-- grouping to find the number of inventors at the firmid for a
-- given |cw_yr - grant_yr|, cw_yr, |emp_yr - app_yr| and emp_yr
-- for each prdn+assg_seq pair.
GROUP BY
    assg_prdn,
    assg_seq,
    ABS(cw_yr - grant_yr),
    cw_yr,
    ABS(emp_yr - app_yr),
    emp_yr,
    assg_firmid;

CREATE TABLE closed_loops AS
SELECT
    assg_prdn,
    assg_seq,
    assg_firmid,
    app_yr,
    grant_yr,
    assg_type,
    assg_st,
    assg_ctry,
    0 AS us_assg_flag,
    0 AS foreign_assg_flag,
    us_inv_flag,
    multiple_assignee_flag,
    cw_yr,
    emp_yr,
    A1 AS model,
    0 AS uniq_firmid,
    num_inv
FROM (
    SELECT
        num_inv,
        assg_prdn,
        assg_seq,
        abs_cw_yr,
        cw_yr,
        abs_emp_yr,
        emp_yr,
        assg_firmid,
        grant_yr,
        app_yr,
        assg_ctry,
        assg_st,
        assg_type,
        us_inv_flag,
        multiple_assignee_flag
        -- for each prdn+assg_seq pair sort by |cw_yr - grant_yr|,
        -- cw_yr, |emp_yr - app_yr|, emp_yr and num_inv and take the
        -- first row(s)
        RANK() OVER (
            PARTITION BY
                assg_prdn,
                assg_seq
            ORDER BY
                abs_cw_yr,
                cw_yr,
                abs_emp_yr,
                emp_yr,
                num_inv DESC
        ) AS rnk
    FROM inv_counts
)
WHERE rnk = 1;

DROP TABLE inv_counts;

-- a state => US assignee
UPDATE closed_loops
SET us_assg_flag = 1
WHERE assg_st != "";
-- no state + country => foreign assignee
UPDATE closed_loops
SET foreign_assg_flag = 1
WHERE
    us_assg_flag != 1 AND
    assg_ctry != "";

UPDATE closed_loops AS outer_tbl
SET uniq_firmid = 1
WHERE
    (
        SELECT COUNT(*)
        FROM closed_loops AS inner_tbl
        WHERE
            outer_tbl.assg_prdn = inner_tbl.assg_prdn AND
            outer_tbl.assg_seq = inner_tbl.assg_seq
    ) > 1;
    
.output ./triangulation/out_data/a1_models.csv
SELECT * FROM closed_loops;
.output stdout
    
DROP TABLE closed_loops;
    
CREATE TABLE prdn_as_Amodel AS
SELECT DISTINCT pik_prdn, assg_seq
FROM closed_paths;

CREATE INDEX
    indx_2
ON
    prdn_as_Amodel(pik_prdn, assg_seq);

DELETE FROM ein_data
WHERE EXISTS (
    SELECT *
    FROM prdn_as_Amodel
    WHERE prdn_as_Amodel.pik_prdn = ein_data.prdn
    AND prdn_as_Amodel.assg_seq = ein_data.assg_seq
);

DROP TABLE prdn_as_Amodel;
DROP TABLE closed_paths;
VACUUM;
    
CREATE TABLE closed_paths AS
SELECT
    pik_data.prdn AS pik_prdn,
    ein_data.prdn AS assg_prdn,
    pik_data.app_yr,
    ein_data.grant_yr,
    ein_data.assg_seq,
    pik_data.inv_seq,
    pik_data.pik,
    ein_data.cw_yr,
    pik_data.emp_yr,
    pik_data.ein AS pik_ein,
    ein_data.ein AS assg_ein,
    pik_data.firmid AS pik_firmid,
    ein_data.firmid AS assg_firmid,
    ein_data.pass_num
FROM pik_data
INNER JOIN ein_data
USING (prdn,ein,firmid)
WHERE ein_data.firmid != '';

CREATE INDEX
    closed_paths_idx
ON
    closed_paths(assg_prdn, assg_seq);
    
ALTER TABLE
    closed_paths
ADD COLUMN
    assg_ctry;
UPDATE
    closed_paths
SET
    assg_ctry =
    (
        SELECT assg_ctry
        FROM assignee_info
        WHERE
            assignee_info.prdn = closed_paths.assg_prdn AND
            assignee_info.assg_seq = closed_paths.assg_seq
    );
ALTER TABLE
    closed_paths
ADD COLUMN
    assg_st;
UPDATE
    closed_paths
SET
    assg_st =
    (
        SELECT assg_st
        FROM assignee_info
        WHERE
            assignee_info.prdn = closed_paths.assg_prdn AND
            assignee_info.assg_seq = closed_paths.assg_seq
    );
ALTER TABLE
    closed_paths
ADD COLUMN
    assg_type;
UPDATE
    closed_paths
SET
    assg_type =
    (
        SELECT assg_type
        FROM assignee_info
        WHERE
            assignee_info.prdn = closed_paths.assg_prdn AND
            assignee_info.assg_seq = closed_paths.assg_seq
    );
ALTER TABLE
    closed_paths
ADD COLUMN
    us_inv_flag;
UPDATE
    closed_paths
SET
    us_inv_flag =
    (
        SELECT us_inv_flag
        FROM prdn_metadata
        WHERE
            prdn_metadata.prdn = closed_paths.assg_prdn
    );
ALTER TABLE
    closed_paths
ADD COLUMN
    multiple_assignee_flag;
UPDATE
    closed_paths
SET
    multiple_assignee_flag =
    (
        SELECT num_assg
        FROM prdn_metadata
        WHERE
            prdn_metadata.prdn = closed_paths.assg_prdn
    );
    
CREATE TABLE inv_counts AS
SELECT
    COUNT(DISTINCT inv_seq) AS num_inv,
    assg_prdn,
    assg_seq,
    ABS(cw_yr - grant_yr) AS abs_cw_yr,
    cw_yr,
    ABS(emp_yr - app_yr)  AS abs_emp_yr,
    emp_yr,
    assg_firmid,
    grant_yr,
    app_yr,
    assg_ctry,
    assg_st,
    assg_type,
    us_inv_flag,
    multiple_assignee_flag
FROM closed_paths
-- grouping to find the number of inventors at the firmid for a
-- given |cw_yr - grant_yr|, cw_yr, |emp_yr - app_yr| and emp_yr
-- for each prdn+assg_seq pair.
GROUP BY
    assg_prdn,
    assg_seq,
    ABS(cw_yr - grant_yr),
    cw_yr,
    ABS(emp_yr - app_yr),
    emp_yr,
    assg_firmid;

CREATE TABLE closed_loops AS
SELECT
    assg_prdn,
    assg_seq,
    assg_firmid,
    app_yr,
    grant_yr,
    assg_type,
    assg_st,
    assg_ctry,
    0 AS us_assg_flag,
    0 AS foreign_assg_flag,
    us_inv_flag,
    multiple_assignee_flag,
    cw_yr,
    emp_yr,
    A2 AS model,
    0 AS uniq_firmid,
    num_inv
FROM (
    SELECT
        num_inv,
        assg_prdn,
        assg_seq,
        abs_cw_yr,
        cw_yr,
        abs_emp_yr,
        emp_yr,
        assg_firmid,
        grant_yr,
        app_yr,
        assg_ctry,
        assg_st,
        assg_type,
        us_inv_flag,
        multiple_assignee_flag
        -- for each prdn+assg_seq pair sort by |cw_yr - grant_yr|,
        -- cw_yr, |emp_yr - app_yr|, emp_yr and num_inv and take the
        -- first row(s)
        RANK() OVER (
            PARTITION BY
                assg_prdn,
                assg_seq
            ORDER BY
                abs_cw_yr,
                cw_yr,
                abs_emp_yr,
                emp_yr,
                num_inv DESC
        ) AS rnk
    FROM inv_counts
)
WHERE rnk = 1;

DROP TABLE inv_counts;

-- a state => US assignee
UPDATE closed_loops
SET us_assg_flag = 1
WHERE assg_st != "";
-- no state + country => foreign assignee
UPDATE closed_loops
SET foreign_assg_flag = 1
WHERE
    us_assg_flag != 1 AND
    assg_ctry != "";

UPDATE closed_loops AS outer_tbl
SET uniq_firmid = 1
WHERE
    (
        SELECT COUNT(*)
        FROM closed_loops AS inner_tbl
        WHERE
            outer_tbl.assg_prdn = inner_tbl.assg_prdn AND
            outer_tbl.assg_seq = inner_tbl.assg_seq
    ) > 1;
    
.output ./triangulation/out_data/a2_models.csv
SELECT * FROM closed_loops;
.output stdout
    
DROP TABLE closed_loops;
    
CREATE TABLE prdn_as_Amodel AS
SELECT DISTINCT pik_prdn, assg_seq
FROM closed_paths;

CREATE INDEX
    indx_2
ON
    prdn_as_Amodel(pik_prdn, assg_seq);

DELETE FROM ein_data
WHERE EXISTS (
    SELECT *
    FROM prdn_as_Amodel
    WHERE prdn_as_Amodel.pik_prdn = ein_data.prdn
    AND prdn_as_Amodel.assg_seq = ein_data.assg_seq
);

DROP TABLE prdn_as_Amodel;
DROP TABLE closed_paths;
VACUUM;
    
CREATE TABLE closed_paths AS
SELECT
    pik_data.prdn AS pik_prdn,
    ein_data.prdn AS assg_prdn,
    pik_data.app_yr,
    ein_data.grant_yr,
    ein_data.assg_seq,
    pik_data.inv_seq,
    pik_data.pik,
    ein_data.cw_yr,
    pik_data.emp_yr,
    pik_data.ein AS pik_ein,
    ein_data.ein AS assg_ein,
    pik_data.firmid AS pik_firmid,
    ein_data.firmid AS assg_firmid,
    ein_data.pass_num
FROM pik_data
INNER JOIN ein_data
USING (prdn,ein,firmid)
WHERE ein_data.firmid != '';

CREATE INDEX
    closed_paths_idx
ON
    closed_paths(assg_prdn, assg_seq);
    
ALTER TABLE
    closed_paths
ADD COLUMN
    assg_ctry;
UPDATE
    closed_paths
SET
    assg_ctry =
    (
        SELECT assg_ctry
        FROM assignee_info
        WHERE
            assignee_info.prdn = closed_paths.assg_prdn AND
            assignee_info.assg_seq = closed_paths.assg_seq
    );
ALTER TABLE
    closed_paths
ADD COLUMN
    assg_st;
UPDATE
    closed_paths
SET
    assg_st =
    (
        SELECT assg_st
        FROM assignee_info
        WHERE
            assignee_info.prdn = closed_paths.assg_prdn AND
            assignee_info.assg_seq = closed_paths.assg_seq
    );
ALTER TABLE
    closed_paths
ADD COLUMN
    assg_type;
UPDATE
    closed_paths
SET
    assg_type =
    (
        SELECT assg_type
        FROM assignee_info
        WHERE
            assignee_info.prdn = closed_paths.assg_prdn AND
            assignee_info.assg_seq = closed_paths.assg_seq
    );
ALTER TABLE
    closed_paths
ADD COLUMN
    us_inv_flag;
UPDATE
    closed_paths
SET
    us_inv_flag =
    (
        SELECT us_inv_flag
        FROM prdn_metadata
        WHERE
            prdn_metadata.prdn = closed_paths.assg_prdn
    );
ALTER TABLE
    closed_paths
ADD COLUMN
    multiple_assignee_flag;
UPDATE
    closed_paths
SET
    multiple_assignee_flag =
    (
        SELECT num_assg
        FROM prdn_metadata
        WHERE
            prdn_metadata.prdn = closed_paths.assg_prdn
    );
    
CREATE TABLE inv_counts AS
SELECT
    COUNT(DISTINCT inv_seq) AS num_inv,
    assg_prdn,
    assg_seq,
    ABS(cw_yr - grant_yr) AS abs_cw_yr,
    cw_yr,
    ABS(emp_yr - app_yr)  AS abs_emp_yr,
    emp_yr,
    assg_firmid,
    grant_yr,
    app_yr,
    assg_ctry,
    assg_st,
    assg_type,
    us_inv_flag,
    multiple_assignee_flag
FROM closed_paths
-- grouping to find the number of inventors at the firmid for a
-- given |cw_yr - grant_yr|, cw_yr, |emp_yr - app_yr| and emp_yr
-- for each prdn+assg_seq pair.
GROUP BY
    assg_prdn,
    assg_seq,
    ABS(cw_yr - grant_yr),
    cw_yr,
    ABS(emp_yr - app_yr),
    emp_yr,
    assg_firmid;

CREATE TABLE closed_loops AS
SELECT
    assg_prdn,
    assg_seq,
    assg_firmid,
    app_yr,
    grant_yr,
    assg_type,
    assg_st,
    assg_ctry,
    0 AS us_assg_flag,
    0 AS foreign_assg_flag,
    us_inv_flag,
    multiple_assignee_flag,
    cw_yr,
    emp_yr,
    A3 AS model,
    0 AS uniq_firmid,
    num_inv
FROM (
    SELECT
        num_inv,
        assg_prdn,
        assg_seq,
        abs_cw_yr,
        cw_yr,
        abs_emp_yr,
        emp_yr,
        assg_firmid,
        grant_yr,
        app_yr,
        assg_ctry,
        assg_st,
        assg_type,
        us_inv_flag,
        multiple_assignee_flag
        -- for each prdn+assg_seq pair sort by |cw_yr - grant_yr|,
        -- cw_yr, |emp_yr - app_yr|, emp_yr and num_inv and take the
        -- first row(s)
        RANK() OVER (
            PARTITION BY
                assg_prdn,
                assg_seq
            ORDER BY
                abs_cw_yr,
                cw_yr,
                abs_emp_yr,
                emp_yr,
                num_inv DESC
        ) AS rnk
    FROM inv_counts
)
WHERE rnk = 1;

DROP TABLE inv_counts;

-- a state => US assignee
UPDATE closed_loops
SET us_assg_flag = 1
WHERE assg_st != "";
-- no state + country => foreign assignee
UPDATE closed_loops
SET foreign_assg_flag = 1
WHERE
    us_assg_flag != 1 AND
    assg_ctry != "";

UPDATE closed_loops AS outer_tbl
SET uniq_firmid = 1
WHERE
    (
        SELECT COUNT(*)
        FROM closed_loops AS inner_tbl
        WHERE
            outer_tbl.assg_prdn = inner_tbl.assg_prdn AND
            outer_tbl.assg_seq = inner_tbl.assg_seq
    ) > 1;
    
.output ./triangulation/out_data/a3_models.csv
SELECT * FROM closed_loops;
.output stdout
    
DROP TABLE closed_loops;
    
CREATE TABLE prdn_as_Amodel AS
SELECT DISTINCT pik_prdn, assg_seq
FROM closed_paths;

CREATE INDEX
    indx_2
ON
    prdn_as_Amodel(pik_prdn, assg_seq);

DELETE FROM ein_data
WHERE EXISTS (
    SELECT *
    FROM prdn_as_Amodel
    WHERE prdn_as_Amodel.pik_prdn = ein_data.prdn
    AND prdn_as_Amodel.assg_seq = ein_data.assg_seq
);

DROP TABLE prdn_as_Amodel;
DROP TABLE closed_paths;
VACUUM;
    
        