.mode csv
pragma temp_store = MEMORY;


-- Bring in the data
CREATE TABLE name_match 
(
    xml_pat_num TEXT,
    uspto_pat_num TEXT,
    assg_num INTEGER,
    grant_yr INTEGER,
    zip3_flag INTEGER,
    ein TEXT,
    firmid TEXT,
    pass_no INTEGER,
    br_yr INTEGER
);
.import ../inData/name_match.csv name_match

-- Preprocess the raw data
DELETE FROM name_match
WHERE
    (br_yr < grant_yr - 2 OR br_yr > grant_yr + 2) OR
    (ein = "" AND firmid = "") OR
    (ein = "000000000" AND firmid = "");

-- prdn assignee information
CREATE TABLE prdns_assgs
(
    prdn TEXT NOT NULL,
    ein TEXT,
    firmid TEXT NOT NULL,
    assg_seq INTEGER NOT NULL,
    br_yr INTEGER NOT NULL,
    pass_no INTEGER,
    grant_yr INTEGER
);
INSERT INTO prdns_assgs
SELECT xml_pat_num, ein, firmid, assg_num, br_yr, pass_no, grant_yr
FROM name_match;


---- Get rid of what we don't need
DROP TABLE name_match;


---- Output table
CREATE TABLE prdn_eins
(
    prdn TEXT,
    grant_yr INTEGER,
    assg_seq INTEGER,
    ein TEXT,
    firmid TEXT,
    cw_yr INTEGER,
    pass_no INTEGER
);
INSERT INTO prdn_eins
SELECT
    prdns_assgs.prdn,
    prdns_assgs.grant_yr,
    prdns_assgs.assg_seq,
    '',
    prdns_assgs.firmid,
    prdns_assgs.br_yr,
    prdns_assgs.pass_no
FROM
    prdns_assgs
WHERE
    prdns_assgs.ein = "" OR 
    prdns_assgs.ein = "000000000";
--     prdn_app_grant_yr.prdn = prdns_assgs.prdn AND
--     (prdns_assgs.ein IS NULL OR prdns_assgs.ein = "000000000");
INSERT INTO prdn_eins
SELECT
    prdns_assgs.prdn,
    prdns_assgs.grant_yr,
    prdns_assgs.assg_seq,
    prdns_assgs.ein,
    prdns_assgs.firmid,
    prdns_assgs.br_yr,
    prdns_assgs.pass_no
FROM
    prdns_assgs
WHERE
    prdns_assgs.ein != "" AND
    prdns_assgs.ein != "000000000";
---- output
.output prdn_eins.csv
SELECT DISTINCT * FROM prdn_eins;
.output stdout

