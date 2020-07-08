/*

*/

-- pragma temp_store = MEMORY;
-- .mode csv

-- -- this has the name match standardized names on it
-- .headers ON
-- .import ../inData/assignee_76_16.csv assignee_name_data
-- .headers OFF
-- CREATE INDEX assignee_name_data_indx
-- ON assignee_name_data
-- (xml_pat_num, assg_num);

-- -- this is what was sent into the name match
-- CREATE TABLE holder (
--     prdn TEXT NOT NULL,
--     assg_seq INT NOT NULL,
--     grant_yr INT NOT NULL,
--     assg_st TEXT,
--     assg_ctry TEXT,
--     xml_name TEXT NOT NULL,
--     uspto_name TEXT,
--     corrected_name TEXT,
--     assg_type TEXT NOT NULL,
--     UNIQUE(prdn, assg_seq)
-- );
-- .import prdn_seq_stand_name.csv holder

-- CREATE TABLE prdn_assg_names AS
-- SELECT
--     holder.*,
--     assignee_name_data.name AS name_match_name
-- FROM
--     holder
-- JOIN
--     assignee_name_data
-- ON
--     holder.prdn = assignee_name_data.xml_pat_num AND
--     holder.assg_seq = assignee_name_data.assg_num;

-- CREATE INDEX an_xml_indx
-- ON prdn_assg_names
-- (prdn, assg_seq, xml_name);
-- CREATE INDEX an_uspto_indx
-- ON prdn_assg_names
-- (prdn, assg_seq, uspto_name);
-- CREATE INDEX an_corrected_indx
-- ON prdn_assg_names
-- (prdn, assg_seq, corrected_name);
-- CREATE INDEX an_name_match_indx
-- ON prdn_assg_names
-- (prdn, assg_seq, name_match_name);

-- DROP TABLE assignee_name_data;
-- DROP TABLE holder;

-- -- from the hand-corrected D2 models
-- CREATE TABLE assg_name_firmid (
--     name TEXT NOT NULL,
--     yr INT,
--     firmid TEXT NOT NULL,
--     UNIQUE(name, yr, firmid)
-- );
-- .import ../inData/assg_yr_firmid.csv assg_name_firmid

-- .headers ON
-- -- from the A1 models
-- CREATE TABLE a1_information (
--     prdn TEXT NOT NULL,
--     assg_seq INT NOT NULL,
--     firmid TEXT NOT NULL,
--     app_yr INT NOT NULL,
--     grant_yr INT NOT NULL,
--     assg_type INT,
--     assg_st TEXT,
--     assg_ctry TEXT,
--     us_assignee_flag INT,
--     foreign_assignee_flag INT,
--     us_inventor_flag INT,
--     multiple_assignee_flag INT,
--     br_yr INT NOT NULL,
--     lehd_yr INT,
--     model_type TEXT,
--     unique_firm_id INT,        
--     count INT,
--     UNIQUE (prdn, assg_seq, firmid)
-- );
-- .import ../outData/a1_models.csv a1_information

-- CREATE TABLE d2_information (
--     prdn TEXT NOT NULL,
--     assg_seq INT NOT NULL,
--     firmid TEXT NOT NULL,
--     app_yr INT NOT NULL,
--     grant_yr INT NOT NULL,
--     assg_type INT,
--     assg_st TEXT,
--     assg_ctry TEXT,
--     us_assignee_flag INT,
--     foreign_assignee_flag INT,
--     us_inventor_flag INT,
--     multiple_assignee_flag INT,
--     br_yr INT NOT NULL,
--     lehd_yr INT,
--     model_type TEXT,
--     unique_firm_id INT,        
--     count INT,
--     UNIQUE (prdn, assg_seq, firmid)
-- );
-- .import ../outData/d2_models.csv d2_information
-- .headers OFF

-- -- metadata for the patents: 
-- CREATE TABLE patent_metadata (
--     prdn TEXT NOT NULL,
--     grant_yr INT NOT NULL,
--     app_yr INT NOT NULL,
--     num_assigs INT NOT NULL,
--     us_inventor_flag INT NOT NULL,
--     UNIQUE (prdn, grant_yr) -- for index on grant_yr
-- );
-- .import ../inData/prdn_metadata.csv patent_metadata

-- this will be the final output
-- CREATE TABLE f_models (
--     prdn TEXT NOT NULL,
--     count INT,
--     assg_seq INT NOT NULL,
--     br_yr INT NOT NULL,
--     lehd_yr INT,
--     firmid TEXT NOT NULL,
--     grant_yr INT NOT NULL,
--     app_yr INT NOT NULL,
--     assg_st TEXT,
--     assg_ctry TEXT,
--     assg_type INT,
--     us_inventor_flag INT,
--     multiple_assignee_flag INT,
--     model_type TEXT,
--     unique_firm_id INT,
--     standardized_name TEXT,
--     a1_prdns_with_standardized_name INT,
--     alias_name TEXT,
--     a1_prdns_with_alias_name INT,
--     UNIQUE (prdn, assg_seq, firmid, model_type, standardized_name, alias_name)
-- );

-- -- collect together all of the name spellings, firmids and valid years
-- CREATE TABLE name_information AS
-- SELECT DISTINCT
--     prdn_assg_names.xml_name AS xml_name,
--     prdn_assg_names.uspto_name AS uspto_name,
--     prdn_assg_names.corrected_name AS corrected_name,
--     prdn_assg_names.name_match_name AS name_match_name,
--     prdn_assg_names.assg_st AS assg_st,
--     prdn_assg_names.assg_ctry AS assg_ctry,
--     a1_information.br_yr AS br_yr,
--     a1_information.firmid AS a1_model_firmid
-- FROM 
--     prdn_assg_names,
--     a1_information
-- WHERE
--     prdn_assg_names.prdn = a1_information.prdn AND
--     prdn_assg_names.assg_seq = a1_information.assg_seq;
    
-- we'll map all of the (mis)spellings onto a standardized (USPTO) name
-- CREATE TABLE standard_name_to_firmid (
--     standard_name TEXT NOT NULL,
--     alias_name TEXT,
--     valid_yr INT NOT NULL,
--     firmid TEXT NOT NULL,
--     state TEXT,
--     country TEXT,
--     model_origin TEXT NOT NULL,
--     sn_on_prdn_count INT,
--     alias_on_prdn_count INT
-- );
-- CREATE UNIQUE INDEX sn_indx ON standard_name_to_firmid
-- (standard_name, valid_yr, firmid, alias_name, model_origin);

-- and now get the by year map from the A1 model information: 
--      standard name -> mis-/alternate spelling -> firmid
-- INSERT OR IGNORE INTO standard_name_to_firmid
--     (
--         standard_name,
--         alias_name,
--         valid_yr,
--         firmid,
--         state,
--         country,
--         model_origin
--     )
-- SELECT
--     corrected_name,
--     corrected_name,
--     br_yr,
--     a1_model_firmid,
--     assg_st,
--     assg_ctry,
--     "A1"
-- FROM
--     name_information
-- WHERE
--     corrected_name != "";
-- INSERT OR IGNORE INTO standard_name_to_firmid
--     (
--         standard_name,
--         alias_name,
--         valid_yr,
--         firmid,
--         state,
--         country,
--         model_origin
--     )
-- SELECT
--     corrected_name,
--     xml_name,
--     br_yr,
--     a1_model_firmid,
--     assg_st,
--     assg_ctry,
--     "A1"
-- FROM
--     name_information
-- WHERE
--     xml_name != "";
-- INSERT OR IGNORE INTO standard_name_to_firmid
--     (
--         standard_name,
--         alias_name,
--         valid_yr,
--         firmid,
--         state,
--         country,
--         model_origin
--     )
-- SELECT
--     corrected_name,
--     uspto_name,
--     br_yr,
--     a1_model_firmid,
--     assg_st,
--     assg_ctry,
--     "A1"
-- FROM
--     name_information
-- WHERE
--     uspto_name != "";
-- this one includes the names from the name match code
-- INSERT OR IGNORE INTO standard_name_to_firmid
--     (
--         standard_name,
--         alias_name,
--         valid_yr,
--         firmid,
--         state,
--         country,
--         model_origin
--     )
-- SELECT
--     corrected_name,
--     name_match_name,
--     br_yr,
--     a1_model_firmid,
--     assg_st,
--     assg_ctry,
--     "A1"
-- FROM
--     name_information
-- WHERE
--     name_match_name != "";
    
-- DROP TABLE name_information;
-- CREATE TABLE name_information AS
-- SELECT DISTINCT
--     prdn_assg_names.xml_name AS xml_name,
--     prdn_assg_names.uspto_name AS uspto_name,
--     prdn_assg_names.corrected_name AS corrected_name,
--     prdn_assg_names.name_match_name AS name_match_name,
--     --assignee_name_data.name AS name_match_name,
--     prdn_assg_names.assg_st AS assg_st,
--     prdn_assg_names.assg_ctry AS assg_ctry,
--     d2_information.grant_yr AS br_yr,
--     d2_information.firmid AS d2_model_firmid
-- FROM 
--     prdn_assg_names,
--     d2_information
--     --,assignee_name_data
-- WHERE
--     prdn_assg_names.prdn = d2_information.prdn AND
--     prdn_assg_names.assg_seq = d2_information.assg_seq;

-- trying to patch things up for the D2 models by mapping the hand matched,
-- name matched and XML/USPTO firm name strings
-- INSERT OR REPLACE INTO standard_name_to_firmid
--     (
--         standard_name,
--         alias_name,
--         valid_yr,
--         firmid,
--         state,
--         country,
--         model_origin
--     )
-- SELECT
--     corrected_name,
--     corrected_name,
--     br_yr,
--     d2_model_firmid,
--     "",
--     "",
--     "D2"
-- FROM
--     name_information
-- WHERE
--     corrected_name != "";
-- INSERT OR REPLACE INTO standard_name_to_firmid
--     (
--         standard_name,
--         alias_name,
--         valid_yr,
--         firmid,
--         state,
--         country,
--         model_origin
--     )
-- SELECT
--     corrected_name,
--     uspto_name,
--     br_yr,
--     d2_model_firmid,
--     "",
--     "",
--     "D2"
-- FROM
--     name_information
-- WHERE
--     uspto_name != "";
-- INSERT OR REPLACE INTO standard_name_to_firmid
--     (
--         standard_name,
--         alias_name,
--         valid_yr,
--         firmid,
--         state,
--         country,
--         model_origin
--     )
-- SELECT
--     corrected_name,
--     xml_name,
--     br_yr,
--     d2_model_firmid,
--     "",
--     "",
--     "D2"
-- FROM
--     name_information
-- WHERE
--     xml_name != "";
-- INSERT OR REPLACE INTO standard_name_to_firmid
--     (
--         standard_name,
--         alias_name,
--         valid_yr,
--         firmid,
--         state,
--         country,
--         model_origin
--     )
-- SELECT
--     corrected_name,
--     name_match_name,
--     br_yr,
--     d2_model_firmid,
--     "",
--     "",
--     "D2"
-- FROM
--     name_information
-- WHERE
--     name_match_name != "";
    
-- remove trash from non-first assignees
-- DELETE FROM standard_name_to_firmid
-- WHERE
--     (
--         standard_name == "" AND 
--         alias_name == ""
--     ) 
--     OR
--     (
--         standard_name == "INDIVIDUALLY OWNED PATENT" OR 
--         alias_name == "INDIVIDUALLY OWNED PATENT"
--     );
    

    
-- **** need to clean a few things up first ****
---- INSERT NAME CORRECTIONS HERE



-- INSERT OR REPLACE INTO standard_name_to_firmid
--     (
--         standard_name,
--         alias_name,
--         valid_yr,
--         firmid,
--         state,
--         country,
--         model_origin
--     )
-- SELECT
--     name,
--     name,
--     yr,
--     firmid,
--     "",
--     "",
--     "D2"
-- FROM
--     assg_name_firmidINSERT OR REPLACE INTO standard_name_to_firmid
--     (
--         standard_name,
--         alias_name,
--         valid_yr,
--         firmid,
--         state,
--         country,
--         model_origin
--     )
-- SELECT
--     name,
--     name,
--     yr,
--     firmid,
--     "",
--     "",
--     "D2"
-- FROM
--     assg_name_firmid;;
 
-- augment the D2 model information with A1 model information
-- CREATE TABLE expanded_d2_names (
--     standard_name TEXT NOT NULL,
--     alias_name TEXT,
--     valid_yr INT NOT NULL,
--     firmid TEXT NOT NULL,
--     state TEXT,
--     country TEXT,
--     model_origin TEXT NOT NULL,
--     sn_on_prdn_count INT,
--     alias_on_prdn_count INT,
--     UNIQUE (standard_name, valid_yr, alias_name)
-- );
-- -- put the original data in
-- INSERT INTO expanded_d2_names
-- SELECT *
-- FROM standard_name_to_firmid
-- WHERE model_origin = "D2";

-- INSERT OR IGNORE INTO expanded_d2_names
--     (
--         standard_name,
--         alias_name,
--         valid_yr,
--         firmid,
--         state,
--         country,
--         model_origin
--     )
-- SELECT 
--     standard_name_to_firmid.standard_name,
--     standard_name_to_firmid.alias_name,
--     standard_name_to_firmid.valid_yr,
--     expanded_d2_names.firmid,
--     "","","STANDARD"
-- FROM 
--     expanded_d2_names,
--     standard_name_to_firmid
-- WHERE
--     expanded_d2_names.alias_name = standard_name_to_firmid.alias_name AND
--     standard_name_to_firmid.standard_name != "" AND -- avoid non-1st assg problems
--     expanded_d2_names.valid_yr = standard_name_to_firmid.valid_yr AND
--     expanded_d2_names.model_origin = "D2" AND
--     standard_name_to_firmid.model_origin = "A1";
-- -- find all of the other aliases for the standardized D2 names
-- INSERT OR IGNORE INTO expanded_d2_names
--     (
--         standard_name,
--         alias_name,
--         valid_yr,
--         firmid,
--         state,
--         country,
--         model_origin
--     )
-- SELECT 
--     standard_name_to_firmid.standard_name,
--     standard_name_to_firmid.alias_name,
--     standard_name_to_firmid.valid_yr,
--     expanded_d2_names.firmid,
--     "","","ALIAS"
-- FROM 
--     expanded_d2_names,
--     standard_name_to_firmid
-- WHERE
--     expanded_d2_names.standard_name = standard_name_to_firmid.standard_name AND
--     standard_name_to_firmid.standard_name != "" AND -- avoid non-1st assg problems
--     expanded_d2_names.valid_yr = standard_name_to_firmid.valid_yr AND
--     expanded_d2_names.model_origin = "STANDARD" AND
--     standard_name_to_firmid.model_origin = "A1";
-- and now put it all back in overwriting any results from the A1 with the D2
-- DELETE FROM standard_name_to_firmid
-- WHERE EXISTS (
--     SELECT 1
--     FROM
--         expanded_d2_names
--     WHERE
--         standard_name_to_firmid.standard_name = expanded_d2_names.standard_name AND
--         standard_name_to_firmid.alias_name = expanded_d2_names.alias_name AND
--         standard_name_to_firmid.valid_yr = expanded_d2_names.valid_yr
-- );

-- DROP INDEX sn_indx;
-- CREATE UNIQUE INDEX sn_indx ON standard_name_to_firmid
-- (valid_yr, alias_name, standard_name, firmid);

-- -- just need to know it was a D2 model initially
-- UPDATE expanded_d2_names SET model_origin = "D2";
-- INSERT OR REPLACE INTO standard_name_to_firmid
-- SELECT *
-- FROM expanded_d2_names;

-- For non-1st assignees see if the alias is mapped to a standard name
-- UPDATE OR IGNORE standard_name_to_firmid
-- SET
--     standard_name = (
--         SELECT sntf.standard_name
--         FROM standard_name_to_firmid AS sntf
--         WHERE
--             standard_name_to_firmid.alias_name = sntf.alias_name AND
--             standard_name_to_firmid.valid_yr = sntf.valid_yr AND
--             sntf.standard_name != ""
--     )
-- WHERE EXISTS (
--         SELECT sntf.standard_name
--         FROM standard_name_to_firmid AS sntf
--         WHERE
--             standard_name_to_firmid.alias_name = sntf.alias_name AND
--             standard_name_to_firmid.valid_yr = sntf.valid_yr AND
--             sntf.standard_name != ""
--     ) 
--     AND
--     standard_name = "";
    
-- UPDATE OR IGNORE standard_name_to_firmid
-- SET
--     standard_name = (
--         SELECT DISTINCT sntf.standard_name
--         FROM standard_name_to_firmid AS sntf
--         WHERE
--             standard_name_to_firmid.alias_name = sntf.alias_name AND
--             sntf.standard_name != ""
--     )
-- WHERE 
--     standard_name = ""
--     AND
--     (
--         SELECT count( DISTINCT sntf.standard_name)
--         FROM standard_name_to_firmid AS sntf
--         WHERE
--             standard_name_to_firmid.alias_name = sntf.alias_name AND
--             sntf.standard_name != ""
--     ) = 1;
-- -- fallback position
-- UPDATE OR IGNORE standard_name_to_firmid
-- SET
--     standard_name = alias_name
-- WHERE
--     standard_name = "";
-- -- and clean things up
-- DELETE FROM standard_name_to_firmid WHERE standard_name = "";

-- -- put the counts in to determine which names are most common
-- -- only A1 1st assignee information is used
-- CREATE TABLE name_counts AS
-- SELECT
--     COUNT( DISTINCT prdn_assg_names.prdn ) AS count,
--     prdn_assg_names.corrected_name AS standard_name,
--     prdn_assg_names.uspto_name AS uspto_name,
--     prdn_assg_names.xml_name AS xml_name,
--     prdn_assg_names.name_match_name AS name_match_name,
--     a1_information.br_yr AS br_yr,
--     a1_information.firmid AS firmid
-- FROM 
--     prdn_assg_names,
--     a1_information
-- WHERE
--     prdn_assg_names.prdn = a1_information.prdn AND
--     prdn_assg_names.assg_seq = a1_information.assg_seq AND
--     prdn_assg_names.corrected_name != "" AND
--     prdn_assg_names.uspto_name != "" AND
--     prdn_assg_names.xml_name != ""
-- GROUP BY
--     prdn_assg_names.corrected_name,
--     prdn_assg_names.uspto_name,
--     prdn_assg_names.xml_name,
--     prdn_assg_names.name_match_name,
--     a1_information.br_yr,
--     a1_information.firmid;
    
-- CREATE INDEX sn_name_counts_indx ON name_counts
-- (br_yr, firmid, standard_name);
-- CREATE INDEX un_name_counts_indx ON name_counts
-- (br_yr, firmid, uspto_name);
-- CREATE INDEX xn_name_counts_indx ON name_counts
-- (br_yr, firmid, xml_name);
-- CREATE INDEX nm_name_counts_indx ON name_counts
-- (br_yr, firmid, name_match_name);

-- UPDATE standard_name_to_firmid
-- SET
--     sn_on_prdn_count = (
--         SELECT SUM(count)
--         FROM name_counts
--         WHERE
--             standard_name_to_firmid.standard_name = name_counts.standard_name AND
--             standard_name_to_firmid.valid_yr = name_counts.br_yr AND
--             standard_name_to_firmid.firmid = name_counts.firmid
--     );

-- UPDATE standard_name_to_firmid
-- SET
--     alias_on_prdn_count = (
--         SELECT SUM(count)
--         FROM name_counts
--         WHERE
--             (
--                 standard_name_to_firmid.alias_name = name_counts.uspto_name
--                 OR
--                 standard_name_to_firmid.alias_name = name_counts.xml_name
--                 OR
--                 standard_name_to_firmid.alias_name = name_counts.name_match_name
--             ) AND
--             standard_name_to_firmid.valid_yr = name_counts.br_yr AND
--             standard_name_to_firmid.firmid = name_counts.firmid
--     );

-- .headers ON
-- .output standard_name_to_firmid.csv
-- SELECT * FROM standard_name_to_firmid;
-- .output stdout
-- .headers OFF

-- DROP INDEX sn_indx;
-- CREATE INDEX sn_indx ON standard_name_to_firmid
-- (valid_yr, alias_name);

-- --------------------------------------------------------------------------------
-- -- F models with br_yr = grant_yr
-- INSERT OR IGNORE INTO f_models
-- SELECT
--     prdn_assg_names.prdn,
--     "",
--     prdn_assg_names.assg_seq,
--     standard_name_to_firmid.valid_yr,
--     "",
--     standard_name_to_firmid.firmid,
--     patent_metadata.grant_yr,
--     patent_metadata.app_yr,
--     prdn_assg_names.assg_st,
--     prdn_assg_names.assg_ctry,
--     prdn_assg_names.assg_type,
--     patent_metadata.us_inventor_flag,
--     patent_metadata.num_assigs,
--     standard_name_to_firmid.model_origin,
--     "",
--     standard_name_to_firmid.standard_name,
--     standard_name_to_firmid.sn_on_prdn_count,
--     standard_name_to_firmid.alias_name,
--     standard_name_to_firmid.alias_on_prdn_count
-- FROM
--     patent_metadata,
--     prdn_assg_names,
--     standard_name_to_firmid
-- WHERE
--     patent_metadata.grant_yr >= 2000 AND
--     prdn_assg_names.prdn = patent_metadata.prdn AND
--     standard_name_to_firmid.valid_yr = patent_metadata.grant_yr AND
--     standard_name_to_firmid.alias_name = prdn_assg_names.name_match_name;

-- INSERT OR IGNORE INTO f_models
-- SELECT
--     prdn_assg_names.prdn,
--     "",
--     prdn_assg_names.assg_seq,
--     standard_name_to_firmid.valid_yr,
--     "",
--     standard_name_to_firmid.firmid,
--     patent_metadata.grant_yr,
--     patent_metadata.app_yr,
--     prdn_assg_names.assg_st,
--     prdn_assg_names.assg_ctry,
--     prdn_assg_names.assg_type,
--     patent_metadata.us_inventor_flag,
--     patent_metadata.num_assigs,
--     standard_name_to_firmid.model_origin,
--     "",
--     standard_name_to_firmid.standard_name,
--     standard_name_to_firmid.sn_on_prdn_count,
--     standard_name_to_firmid.alias_name,
--     standard_name_to_firmid.alias_on_prdn_count
-- FROM
--     patent_metadata,
--     prdn_assg_names,
--     standard_name_to_firmid
-- WHERE
--     patent_metadata.grant_yr >= 2000 AND
--     prdn_assg_names.prdn = patent_metadata.prdn AND
--     standard_name_to_firmid.valid_yr = patent_metadata.grant_yr AND
--     standard_name_to_firmid.alias_name = prdn_assg_names.corrected_name;

-- INSERT OR IGNORE INTO f_models
-- SELECT
--     prdn_assg_names.prdn,
--     "",
--     prdn_assg_names.assg_seq,
--     standard_name_to_firmid.valid_yr,
--     "",
--     standard_name_to_firmid.firmid,
--     patent_metadata.grant_yr,
--     patent_metadata.app_yr,
--     prdn_assg_names.assg_st,
--     prdn_assg_names.assg_ctry,
--     prdn_assg_names.assg_type,
--     patent_metadata.us_inventor_flag,
--     patent_metadata.num_assigs,
--     standard_name_to_firmid.model_origin,
--     "",
--     standard_name_to_firmid.standard_name,
--     standard_name_to_firmid.sn_on_prdn_count,
--     standard_name_to_firmid.alias_name,
--     standard_name_to_firmid.alias_on_prdn_count
-- FROM
--     patent_metadata,
--     prdn_assg_names,
--     standard_name_to_firmid
-- WHERE
--     patent_metadata.grant_yr >= 2000 AND
--     prdn_assg_names.prdn = patent_metadata.prdn AND
--     standard_name_to_firmid.valid_yr = patent_metadata.grant_yr AND
--     standard_name_to_firmid.alias_name = prdn_assg_names.uspto_name;

-- INSERT OR IGNORE INTO f_models
-- SELECT
--     prdn_assg_names.prdn,
--     "",
--     prdn_assg_names.assg_seq,
--     standard_name_to_firmid.valid_yr,
--     "",
--     standard_name_to_firmid.firmid,
--     patent_metadata.grant_yr,
--     patent_metadata.app_yr,
--     prdn_assg_names.assg_st,
--     prdn_assg_names.assg_ctry,
--     prdn_assg_names.assg_type,
--     patent_metadata.us_inventor_flag,
--     patent_metadata.num_assigs,
--     standard_name_to_firmid.model_origin,
--     "",
--     standard_name_to_firmid.standard_name,
--     standard_name_to_firmid.sn_on_prdn_count,
--     standard_name_to_firmid.alias_name,
--     standard_name_to_firmid.alias_on_prdn_count
-- FROM
--     patent_metadata,
--     prdn_assg_names,
--     standard_name_to_firmid
-- WHERE
--     patent_metadata.grant_yr >= 2000 AND
--     prdn_assg_names.prdn = patent_metadata.prdn AND
--     standard_name_to_firmid.valid_yr = patent_metadata.grant_yr AND
--     standard_name_to_firmid.alias_name = prdn_assg_names.xml_name;

-- DELETE FROM prdn_assg_names
-- WHERE EXISTS
--     (
--         SELECT *
--         FROM f_models
--         WHERE
--             f_models.prdn = prdn_assg_names.prdn AND
--             f_models.assg_seq = prdn_assg_names.assg_seq
--     );

-- --------------------------------------------------------------------------------
-- -- F models with br_yr = grant_yr - 1
-- INSERT OR IGNORE INTO f_models
-- SELECT
--     prdn_assg_names.prdn,
--     "",
--     prdn_assg_names.assg_seq,
--     standard_name_to_firmid.valid_yr,
--     "",
--     standard_name_to_firmid.firmid,
--     patent_metadata.grant_yr,
--     patent_metadata.app_yr,
--     prdn_assg_names.assg_st,
--     prdn_assg_names.assg_ctry,
--     prdn_assg_names.assg_type,
--     patent_metadata.us_inventor_flag,
--     patent_metadata.num_assigs,
--     standard_name_to_firmid.model_origin,
--     "",
--     standard_name_to_firmid.standard_name,
--     standard_name_to_firmid.sn_on_prdn_count,
--     standard_name_to_firmid.alias_name,
--     standard_name_to_firmid.alias_on_prdn_count
-- FROM
--     patent_metadata,
--     prdn_assg_names,
--     standard_name_to_firmid
-- WHERE
--     patent_metadata.grant_yr >= 2000 AND
--     prdn_assg_names.prdn = patent_metadata.prdn AND
--     standard_name_to_firmid.valid_yr = patent_metadata.grant_yr - 1 AND
--     standard_name_to_firmid.alias_name = prdn_assg_names.name_match_name;

-- INSERT OR IGNORE INTO f_models
-- SELECT
--     prdn_assg_names.prdn,
--     "",
--     prdn_assg_names.assg_seq,
--     standard_name_to_firmid.valid_yr,
--     "",
--     standard_name_to_firmid.firmid,
--     patent_metadata.grant_yr,
--     patent_metadata.app_yr,
--     prdn_assg_names.assg_st,
--     prdn_assg_names.assg_ctry,
--     prdn_assg_names.assg_type,
--     patent_metadata.us_inventor_flag,
--     patent_metadata.num_assigs,
--     standard_name_to_firmid.model_origin,
--     "",
--     standard_name_to_firmid.standard_name,
--     standard_name_to_firmid.sn_on_prdn_count,
--     standard_name_to_firmid.alias_name,
--     standard_name_to_firmid.alias_on_prdn_count
-- FROM
--     patent_metadata,
--     prdn_assg_names,
--     standard_name_to_firmid
-- WHERE
--     patent_metadata.grant_yr >= 2000 AND
--     prdn_assg_names.prdn = patent_metadata.prdn AND
--     standard_name_to_firmid.valid_yr = patent_metadata.grant_yr - 1 AND
--     standard_name_to_firmid.alias_name = prdn_assg_names.corrected_name;

-- INSERT OR IGNORE INTO f_models
-- SELECT
--     prdn_assg_names.prdn,
--     "",
--     prdn_assg_names.assg_seq,
--     standard_name_to_firmid.valid_yr,
--     "",
--     standard_name_to_firmid.firmid,
--     patent_metadata.grant_yr,
--     patent_metadata.app_yr,
--     prdn_assg_names.assg_st,
--     prdn_assg_names.assg_ctry,
--     prdn_assg_names.assg_type,
--     patent_metadata.us_inventor_flag,
--     patent_metadata.num_assigs,
--     standard_name_to_firmid.model_origin,
--     "",
--     standard_name_to_firmid.standard_name,
--     standard_name_to_firmid.sn_on_prdn_count,
--     standard_name_to_firmid.alias_name,
--     standard_name_to_firmid.alias_on_prdn_count
-- FROM
--     patent_metadata,
--     prdn_assg_names,
--     standard_name_to_firmid
-- WHERE
--     patent_metadata.grant_yr >= 2000 AND
--     prdn_assg_names.prdn = patent_metadata.prdn AND
--     standard_name_to_firmid.valid_yr = patent_metadata.grant_yr - 1 AND
--     standard_name_to_firmid.alias_name = prdn_assg_names.uspto_name;

-- INSERT OR IGNORE INTO f_models
-- SELECT
--     prdn_assg_names.prdn,
--     "",
--     prdn_assg_names.assg_seq,
--     standard_name_to_firmid.valid_yr,
--     "",
--     standard_name_to_firmid.firmid,
--     patent_metadata.grant_yr,
--     patent_metadata.app_yr,
--     prdn_assg_names.assg_st,
--     prdn_assg_names.assg_ctry,
--     prdn_assg_names.assg_type,
--     patent_metadata.us_inventor_flag,
--     patent_metadata.num_assigs,
--     standard_name_to_firmid.model_origin,
--     "",
--     standard_name_to_firmid.standard_name,
--     standard_name_to_firmid.sn_on_prdn_count,
--     standard_name_to_firmid.alias_name,
--     standard_name_to_firmid.alias_on_prdn_count
-- FROM
--     patent_metadata,
--     prdn_assg_names,
--     standard_name_to_firmid
-- WHERE
--     patent_metadata.grant_yr >= 2000 AND
--     prdn_assg_names.prdn = patent_metadata.prdn AND
--     standard_name_to_firmid.valid_yr = patent_metadata.grant_yr - 1 AND
--     standard_name_to_firmid.alias_name = prdn_assg_names.xml_name;

-- DELETE FROM prdn_assg_names
-- WHERE EXISTS
--     (
--         SELECT *
--         FROM f_models
--         WHERE
--             f_models.prdn = prdn_assg_names.prdn AND
--             f_models.assg_seq = prdn_assg_names.assg_seq
--     );

-- --------------------------------------------------------------------------------
-- -- F models with br_yr = grant_yr + 1
-- INSERT OR IGNORE INTO f_models
-- SELECT
--     prdn_assg_names.prdn,
--     "",
--     prdn_assg_names.assg_seq,
--     standard_name_to_firmid.valid_yr,
--     "",
--     standard_name_to_firmid.firmid,
--     patent_metadata.grant_yr,
--     patent_metadata.app_yr,
--     prdn_assg_names.assg_st,
--     prdn_assg_names.assg_ctry,
--     prdn_assg_names.assg_type,
--     patent_metadata.us_inventor_flag,
--     patent_metadata.num_assigs,
--     standard_name_to_firmid.model_origin,
--     "",
--     standard_name_to_firmid.standard_name,
--     standard_name_to_firmid.sn_on_prdn_count,
--     standard_name_to_firmid.alias_name,
--     standard_name_to_firmid.alias_on_prdn_count
-- FROM
--     patent_metadata,
--     prdn_assg_names,
--     standard_name_to_firmid
-- WHERE
--     patent_metadata.grant_yr >= 2000 AND
--     prdn_assg_names.prdn = patent_metadata.prdn AND
--     standard_name_to_firmid.valid_yr = patent_metadata.grant_yr + 1 AND
--     standard_name_to_firmid.alias_name = prdn_assg_names.name_match_name;

-- INSERT OR IGNORE INTO f_models
-- SELECT
--     prdn_assg_names.prdn,
--     "",
--     prdn_assg_names.assg_seq,
--     standard_name_to_firmid.valid_yr,
--     "",
--     standard_name_to_firmid.firmid,
--     patent_metadata.grant_yr,
--     patent_metadata.app_yr,
--     prdn_assg_names.assg_st,
--     prdn_assg_names.assg_ctry,
--     prdn_assg_names.assg_type,
--     patent_metadata.us_inventor_flag,
--     patent_metadata.num_assigs,
--     standard_name_to_firmid.model_origin,
--     "",
--     standard_name_to_firmid.standard_name,
--     standard_name_to_firmid.sn_on_prdn_count,
--     standard_name_to_firmid.alias_name,
--     standard_name_to_firmid.alias_on_prdn_count
-- FROM
--     patent_metadata,
--     prdn_assg_names,
--     standard_name_to_firmid
-- WHERE
--     patent_metadata.grant_yr >= 2000 AND
--     prdn_assg_names.prdn = patent_metadata.prdn AND
--     standard_name_to_firmid.valid_yr = patent_metadata.grant_yr + 1 AND
--     standard_name_to_firmid.alias_name = prdn_assg_names.corrected_name;

-- INSERT OR IGNORE INTO f_models
-- SELECT
--     prdn_assg_names.prdn,
--     "",
--     prdn_assg_names.assg_seq,
--     standard_name_to_firmid.valid_yr,
--     "",
--     standard_name_to_firmid.firmid,
--     patent_metadata.grant_yr,
--     patent_metadata.app_yr,
--     prdn_assg_names.assg_st,
--     prdn_assg_names.assg_ctry,
--     prdn_assg_names.assg_type,
--     patent_metadata.us_inventor_flag,
--     patent_metadata.num_assigs,
--     standard_name_to_firmid.model_origin,
--     "",
--     standard_name_to_firmid.standard_name,
--     standard_name_to_firmid.sn_on_prdn_count,
--     standard_name_to_firmid.alias_name,
--     standard_name_to_firmid.alias_on_prdn_count
-- FROM
--     patent_metadata,
--     prdn_assg_names,
--     standard_name_to_firmid
-- WHERE
--     patent_metadata.grant_yr >= 2000 AND
--     prdn_assg_names.prdn = patent_metadata.prdn AND
--     standard_name_to_firmid.valid_yr = patent_metadata.grant_yr + 1 AND
--     standard_name_to_firmid.alias_name = prdn_assg_names.uspto_name;

-- INSERT OR IGNORE INTO f_models
-- SELECT
--     prdn_assg_names.prdn,
--     "",
--     prdn_assg_names.assg_seq,
--     standard_name_to_firmid.valid_yr,
--     "",
--     standard_name_to_firmid.firmid,
--     patent_metadata.grant_yr,
--     patent_metadata.app_yr,
--     prdn_assg_names.assg_st,
--     prdn_assg_names.assg_ctry,
--     prdn_assg_names.assg_type,
--     patent_metadata.us_inventor_flag,
--     patent_metadata.num_assigs,
--     standard_name_to_firmid.model_origin,
--     "",
--     standard_name_to_firmid.standard_name,
--     standard_name_to_firmid.sn_on_prdn_count,
--     standard_name_to_firmid.alias_name,
--     standard_name_to_firmid.alias_on_prdn_count
-- FROM
--     patent_metadata,
--     prdn_assg_names,
--     standard_name_to_firmid
-- WHERE
--     patent_metadata.grant_yr >= 2000 AND
--     prdn_assg_names.prdn = patent_metadata.prdn AND
--     standard_name_to_firmid.valid_yr = patent_metadata.grant_yr + 1 AND
--     standard_name_to_firmid.alias_name = prdn_assg_names.xml_name;

-- DELETE FROM prdn_assg_names
-- WHERE EXISTS
--     (
--         SELECT *
--         FROM f_models
--         WHERE
--             f_models.prdn = prdn_assg_names.prdn AND
--             f_models.assg_seq = prdn_assg_names.assg_seq
--     );

-- --------------------------------------------------------------------------------
-- -- F models with br_yr = grant_yr - 2
-- INSERT OR IGNORE INTO f_models
-- SELECT
--     prdn_assg_names.prdn,
--     "",
--     prdn_assg_names.assg_seq,
--     standard_name_to_firmid.valid_yr,
--     "",
--     standard_name_to_firmid.firmid,
--     patent_metadata.grant_yr,
--     patent_metadata.app_yr,
--     prdn_assg_names.assg_st,
--     prdn_assg_names.assg_ctry,
--     prdn_assg_names.assg_type,
--     patent_metadata.us_inventor_flag,
--     patent_metadata.num_assigs,
--     standard_name_to_firmid.model_origin,
--     "",
--     standard_name_to_firmid.standard_name,
--     standard_name_to_firmid.sn_on_prdn_count,
--     standard_name_to_firmid.alias_name,
--     standard_name_to_firmid.alias_on_prdn_count
-- FROM
--     patent_metadata,
--     prdn_assg_names,
--     standard_name_to_firmid
-- WHERE
--     patent_metadata.grant_yr >= 2000 AND
--     prdn_assg_names.prdn = patent_metadata.prdn AND
--     standard_name_to_firmid.valid_yr = patent_metadata.grant_yr - 2 AND
--     standard_name_to_firmid.alias_name = prdn_assg_names.name_match_name;

-- INSERT OR IGNORE INTO f_models
-- SELECT
--     prdn_assg_names.prdn,
--     "",
--     prdn_assg_names.assg_seq,
--     standard_name_to_firmid.valid_yr,
--     "",
--     standard_name_to_firmid.firmid,
--     patent_metadata.grant_yr,
--     patent_metadata.app_yr,
--     prdn_assg_names.assg_st,
--     prdn_assg_names.assg_ctry,
--     prdn_assg_names.assg_type,
--     patent_metadata.us_inventor_flag,
--     patent_metadata.num_assigs,
--     standard_name_to_firmid.model_origin,
--     "",
--     standard_name_to_firmid.standard_name,
--     standard_name_to_firmid.sn_on_prdn_count,
--     standard_name_to_firmid.alias_name,
--     standard_name_to_firmid.alias_on_prdn_count
-- FROM
--     patent_metadata,
--     prdn_assg_names,
--     standard_name_to_firmid
-- WHERE
--     patent_metadata.grant_yr >= 2000 AND
--     prdn_assg_names.prdn = patent_metadata.prdn AND
--     standard_name_to_firmid.valid_yr = patent_metadata.grant_yr - 2 AND
--     standard_name_to_firmid.alias_name = prdn_assg_names.corrected_name;

-- INSERT OR IGNORE INTO f_models
-- SELECT
--     prdn_assg_names.prdn,
--     "",
--     prdn_assg_names.assg_seq,
--     standard_name_to_firmid.valid_yr,
--     "",
--     standard_name_to_firmid.firmid,
--     patent_metadata.grant_yr,
--     patent_metadata.app_yr,
--     prdn_assg_names.assg_st,
--     prdn_assg_names.assg_ctry,
--     prdn_assg_names.assg_type,
--     patent_metadata.us_inventor_flag,
--     patent_metadata.num_assigs,
--     standard_name_to_firmid.model_origin,
--     "",
--     standard_name_to_firmid.standard_name,
--     standard_name_to_firmid.sn_on_prdn_count,
--     standard_name_to_firmid.alias_name,
--     standard_name_to_firmid.alias_on_prdn_count
-- FROM
--     patent_metadata,
--     prdn_assg_names,
--     standard_name_to_firmid
-- WHERE
--     patent_metadata.grant_yr >= 2000 AND
--     prdn_assg_names.prdn = patent_metadata.prdn AND
--     standard_name_to_firmid.valid_yr = patent_metadata.grant_yr - 2 AND
--     standard_name_to_firmid.alias_name = prdn_assg_names.uspto_name;

-- INSERT OR IGNORE INTO f_models
-- SELECT
--     prdn_assg_names.prdn,
--     "",
--     prdn_assg_names.assg_seq,
--     standard_name_to_firmid.valid_yr,
--     "",
--     standard_name_to_firmid.firmid,
--     patent_metadata.grant_yr,
--     patent_metadata.app_yr,
--     prdn_assg_names.assg_st,
--     prdn_assg_names.assg_ctry,
--     prdn_assg_names.assg_type,
--     patent_metadata.us_inventor_flag,
--     patent_metadata.num_assigs,
--     standard_name_to_firmid.model_origin,
--     "",
--     standard_name_to_firmid.standard_name,
--     standard_name_to_firmid.sn_on_prdn_count,
--     standard_name_to_firmid.alias_name,
--     standard_name_to_firmid.alias_on_prdn_count
-- FROM
--     patent_metadata,
--     prdn_assg_names,
--     standard_name_to_firmid
-- WHERE
--     patent_metadata.grant_yr >= 2000 AND
--     prdn_assg_names.prdn = patent_metadata.prdn AND
--     standard_name_to_firmid.valid_yr = patent_metadata.grant_yr - 2 AND
--     standard_name_to_firmid.alias_name = prdn_assg_names.xml_name;

-- DELETE FROM prdn_assg_names
-- WHERE EXISTS
--     (
--         SELECT *
--         FROM f_models
--         WHERE
--             f_models.prdn = prdn_assg_names.prdn AND
--             f_models.assg_seq = prdn_assg_names.assg_seq
--     );

-- --------------------------------------------------------------------------------
-- -- F models with br_yr = grant_yr + 2
-- INSERT OR IGNORE INTO f_models
-- SELECT
--     prdn_assg_names.prdn,
--     "",
--     prdn_assg_names.assg_seq,
--     standard_name_to_firmid.valid_yr,
--     "",
--     standard_name_to_firmid.firmid,
--     patent_metadata.grant_yr,
--     patent_metadata.app_yr,
--     prdn_assg_names.assg_st,
--     prdn_assg_names.assg_ctry,
--     prdn_assg_names.assg_type,
--     patent_metadata.us_inventor_flag,
--     patent_metadata.num_assigs,
--     standard_name_to_firmid.model_origin,
--     "",
--     standard_name_to_firmid.standard_name,
--     standard_name_to_firmid.sn_on_prdn_count,
--     standard_name_to_firmid.alias_name,
--     standard_name_to_firmid.alias_on_prdn_count
-- FROM
--     patent_metadata,
--     prdn_assg_names,
--     standard_name_to_firmid
-- WHERE
--     patent_metadata.grant_yr >= 2000 AND
--     prdn_assg_names.prdn = patent_metadata.prdn AND
--     standard_name_to_firmid.valid_yr = patent_metadata.grant_yr + 2 AND
--     standard_name_to_firmid.alias_name = prdn_assg_names.name_match_name;

-- INSERT OR IGNORE INTO f_models
-- SELECT
--     prdn_assg_names.prdn,
--     "",
--     prdn_assg_names.assg_seq,
--     standard_name_to_firmid.valid_yr,
--     "",
--     standard_name_to_firmid.firmid,
--     patent_metadata.grant_yr,
--     patent_metadata.app_yr,
--     prdn_assg_names.assg_st,
--     prdn_assg_names.assg_ctry,
--     prdn_assg_names.assg_type,
--     patent_metadata.us_inventor_flag,
--     patent_metadata.num_assigs,
--     standard_name_to_firmid.model_origin,
--     "",
--     standard_name_to_firmid.standard_name,
--     standard_name_to_firmid.sn_on_prdn_count,
--     standard_name_to_firmid.alias_name,
--     standard_name_to_firmid.alias_on_prdn_count
-- FROM
--     patent_metadata,
--     prdn_assg_names,
--     standard_name_to_firmid
-- WHERE
--     patent_metadata.grant_yr >= 2000 AND
--     prdn_assg_names.prdn = patent_metadata.prdn AND
--     standard_name_to_firmid.valid_yr = patent_metadata.grant_yr + 2 AND
--     standard_name_to_firmid.alias_name = prdn_assg_names.corrected_name;

-- INSERT OR IGNORE INTO f_models
-- SELECT
--     prdn_assg_names.prdn,
--     "",
--     prdn_assg_names.assg_seq,
--     standard_name_to_firmid.valid_yr,
--     "",
--     standard_name_to_firmid.firmid,
--     patent_metadata.grant_yr,
--     patent_metadata.app_yr,
--     prdn_assg_names.assg_st,
--     prdn_assg_names.assg_ctry,
--     prdn_assg_names.assg_type,
--     patent_metadata.us_inventor_flag,
--     patent_metadata.num_assigs,
--     standard_name_to_firmid.model_origin,
--     "",
--     standard_name_to_firmid.standard_name,
--     standard_name_to_firmid.sn_on_prdn_count,
--     standard_name_to_firmid.alias_name,
--     standard_name_to_firmid.alias_on_prdn_count
-- FROM
--     patent_metadata,
--     prdn_assg_names,
--     standard_name_to_firmid
-- WHERE
--     patent_metadata.grant_yr >= 2000 AND
--     prdn_assg_names.prdn = patent_metadata.prdn AND
--     standard_name_to_firmid.valid_yr = patent_metadata.grant_yr + 2 AND
--     standard_name_to_firmid.alias_name = prdn_assg_names.uspto_name;

-- INSERT OR IGNORE INTO f_models
-- SELECT
--     prdn_assg_names.prdn,
--     "",
--     prdn_assg_names.assg_seq,
--     standard_name_to_firmid.valid_yr,
--     "",
--     standard_name_to_firmid.firmid,
--     patent_metadata.grant_yr,
--     patent_metadata.app_yr,
--     prdn_assg_names.assg_st,
--     prdn_assg_names.assg_ctry,
--     prdn_assg_names.assg_type,
--     patent_metadata.us_inventor_flag,
--     patent_metadata.num_assigs,
--     standard_name_to_firmid.model_origin,
--     "",
--     standard_name_to_firmid.standard_name,
--     standard_name_to_firmid.sn_on_prdn_count,
--     standard_name_to_firmid.alias_name,
--     standard_name_to_firmid.alias_on_prdn_count
-- FROM
--     patent_metadata,
--     prdn_assg_names,
--     standard_name_to_firmid
-- WHERE
--     patent_metadata.grant_yr >= 2000 AND
--     prdn_assg_names.prdn = patent_metadata.prdn AND
--     standard_name_to_firmid.valid_yr = patent_metadata.grant_yr + 2 AND
--     standard_name_to_firmid.alias_name = prdn_assg_names.xml_name;

-- DELETE FROM prdn_assg_names
-- WHERE EXISTS
--     (
--         SELECT *
--         FROM f_models
--         WHERE
--             f_models.prdn = prdn_assg_names.prdn AND
--             f_models.assg_seq = prdn_assg_names.assg_seq
--     );

-- this is the post-processed output
CREATE TABLE final_f_models (
    prdn TEXT NOT NULL,
    count INT,
    assg_seq INT NOT NULL,
    br_yr INT NOT NULL,
    lehd_yr INT,
    firmid TEXT NOT NULL,
    grant_yr INT NOT NULL,
    app_yr INT NOT NULL,
    assg_st TEXT,
    assg_ctry TEXT,
    assg_type INT,
    us_inventor_flag INT,
    multiple_assignee_flag INT,
    model_type TEXT,
    unique_firm_id INT,
    standardized_name_count INT,
    alias_name_count INT,
    UNIQUE (prdn, assg_seq, firmid)
);

INSERT INTO final_f_models
SELECT DISTINCT
    prdn,
    count,
    assg_seq,
    br_yr,
    lehd_yr,
    firmid,
    grant_yr,
    app_yr,
    assg_st,
    assg_ctry,
    assg_type,
    us_inventor_flag,
    multiple_assignee_flag,
    'FA1',
    unique_firm_id,
    SUM(a1_prdns_with_standardized_name),
    SUM(a1_prdns_with_alias_name)
FROM
    f_models
WHERE
    model_type = 'A1'
GROUP BY
    prdn,
    assg_seq,
    firmid,
    br_yr,
    lehd_yr;
    
INSERT OR REPLACE INTO final_f_models
SELECT DISTINCT
    prdn,
    count,
    assg_seq,
    br_yr,
    lehd_yr,
    firmid,
    grant_yr,
    app_yr,
    assg_st,
    assg_ctry,
    assg_type,
    us_inventor_flag,
    multiple_assignee_flag,
    'FD2',
    unique_firm_id,
    -- these are just big dummy numbers so D2 gets picked
    --SUM(a1_prdns_with_standardized_name),
    --SUM(a1_prdns_with_alias_name)
    1000000,
    1000000
FROM
    f_models
WHERE
    model_type = 'D2'
GROUP BY
    prdn,
    assg_seq,
    firmid,
    br_yr,
    lehd_yr;
    
CREATE TABLE total_counts
AS
SELECT
    prdn,
    assg_seq,
    --SUM(a1_prdns_with_standardized_name) AS total_sn_count,
    --SUM(a1_prdns_with_alias_name) AS total_an_count
    SUM(standardized_name_count) AS total_sn_count,
    SUM(alias_name_count) AS total_an_count
FROM
    final_f_models
GROUP BY
    prdn,
    assg_seq;
CREATE UNIQUE INDEX tc_indx ON total_counts
(prdn, assg_seq);

.output f_models.csv
SELECT
    final_f_models.prdn,
    final_f_models.assg_seq,
    firmid,
    app_yr,
    grant_yr,
    assg_type,
    assg_st,
    assg_ctry,
    us_inventor_flag,
    multiple_assignee_flag,
    br_yr,
    lehd_yr,
    model_type,
    unique_firm_id,
    count
FROM 
    final_f_models,
    total_counts
WHERE
    final_f_models.firmid != '' AND
    final_f_models.prdn = total_counts.prdn AND
    final_f_models.assg_seq = total_counts.assg_seq AND
    ( 1.0 * standardized_name_count ) / total_sn_count > 0.7 AND
    ( 1.0 * alias_name_count ) / total_an_count > 0.7
ORDER BY
    final_f_models.prdn,
    final_f_models.assg_seq;
.output stdout

