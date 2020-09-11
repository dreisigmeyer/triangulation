# import os
import triangulation.src.shared_code.column_names as columns
import triangulation.src.shared_code.file_names as file_names
import triangulation.src.shared_code.shared_code as shared_code
import triangulation.src.shared_code.table_names as table_names


# def create_d2_name_maps(fh):
#     """

#     """
#     fh.write(
#         f'''
# CREATE TABLE {table_names.d2_prdn_seq_name} AS
# SELECT
#     {columns.prdn.name},
#     {columns.assg_seq.name},
#     {columns.firmid.name},
#     {columns.assg_name.name}
# FROM
#     {table_names.possible_d_models},
#     {table_names.assg_name_firmid}
# WHERE
#     {table_names.assg_name_firmid}.{columns.firmid.name} IS NOT NULL AND
#     {table_names.possible_d_models}.{columns.assg_name.name} IN (SELECT {columns.assg_name.name} FROM {table_names.big_firm_names}) AND
#     {table_names.possible_d_models}.{columns.grant_yr.name} = {table_names.assg_name_firmid}.{columns.year.name} AND
#     {table_names.possible_d_models}.{columns.assg_name.name} = {table_names.assg_name_firmid}.{columns.assg_name.name};

# CREATE TABLE {table_names.d2_assg_info} (
#     {columns.xml_pat_num.cmd},
#     {columns.uspto_pat_num.cmd},
#     {columns.grant_yr.cmd},
#     {columns.app_yr.cmd},
#     {columns.assg_name_xml.cmd},
#     {columns.assg_seq.cmd},
#     {columns.assg_type.cmd},
#     {columns.assg_city.cmd},
#     {columns.assg_st.cmd},
#     {columns.assg_ctry.cmd},
#     {columns.assg_name_raw.cmd},
#     {columns.assg_name_clean.cmd},
#     {columns.zip3.cmd},
#     {columns.assg_st_inferred.cmd},
#     {columns.assg_name_inferred.cmd}
# );
#     ''')

#     for file in os.listdir(f'{file_names.assignee_out_data}'):
#         if file.endswith(".csv"):
#             file_name = os.path.join(f'{file_names.assignee_out_data}', file)
#             shared_code.import_data(fh, f'{table_names.d2_assg_info}', file_name)

def create_expanded_d2_name(fh):
    """

    """
    fh.write(
        f'''
CREATE TABLE {table_names.expanded_d2_names} (
    {columns.standard_name.cmd},
    {columns.alias_name.cmd},
    {columns.valid_yr.cmd},
    {columns.firmid.cmd},
    {columns.state.cmd},
    {columns.country.cmd},
    {columns.model_origin.cmd},
    {columns.sn_on_prdn_count.cmd},
    {columns.alias_on_prdn_count.cmd},
    UNIQUE (
        {columns.standard_name.name},
        {columns.valid_yr.name},
        {columns.alias_name.name}
    )
);
-- put the original data in
INSERT INTO {table_names.expanded_d2_names}
SELECT *
FROM {table_names.standard_name_to_firmid}
WHERE     {columns.model_origin.name} = "D2";

INSERT OR IGNORE INTO {table_names.expanded_d2_names}
    (
        {columns.standard_name.name},
        {columns.alias_name.name},
        {columns.valid_yr.name},
        {columns.firmid.name},
        {columns.state.name},
        {columns.country.name},
        {columns.model_origin.name}
    )
SELECT
    {table_names.standard_name_to_firmid}.{columns.standard_name.name},
    {table_names.standard_name_to_firmid}.{columns.alias_name.name},
    {table_names.standard_name_to_firmid}.{columns.valid_yr.name},
    {table_names.expanded_d2_names}.{columns.firmid.name},
    "","","STANDARD"
FROM
    {table_names.expanded_d2_names},
    {table_names.standard_name_to_firmid}
WHERE
    {table_names.expanded_d2_names}.{columns.alias_name.name} = {table_names.standard_name_to_firmid}.{columns.alias_name.name} AND
    {table_names.standard_name_to_firmid}.{columns.standard_name.name} != "" AND -- avoid non-1st assg problems
    {table_names.expanded_d2_names}.{columns.valid_yr.name} = {table_names.standard_name_to_firmid}.{columns.valid_yr.name} AND
    {table_names.expanded_d2_names}.{columns.model_origin.name} = "D2" AND
    {table_names.standard_name_to_firmid}.{columns.model_origin.name} = "A1";
-- find all of the other aliases for the standardized D2 names
INSERT OR IGNORE INTO {table_names.expanded_d2_names}
    (
        {columns.standard_name.name},
        {columns.alias_name.name},
        {columns.valid_yr.name},
        {columns.firmid.name},
        {columns.state.name},
        {columns.country.name},
        {columns.model_origin.name}
    )
SELECT
    {table_names.standard_name_to_firmid}.{columns.standard_name.name},
    {table_names.standard_name_to_firmid}.{columns.alias_name.name},
    {table_names.standard_name_to_firmid}.{columns.valid_yr.name},
    {table_names.expanded_d2_names}.{columns.firmid.name},
    "","","ALIAS"
FROM
    {table_names.expanded_d2_names},
    {table_names.standard_name_to_firmid}
WHERE
    {table_names.expanded_d2_names}.{columns.standard_name.name} = {table_names.standard_name_to_firmid}.{columns.standard_name.name} AND
    {table_names.standard_name_to_firmid}.{columns.standard_name.name} != "" AND -- avoid non-1st assg problems
    {table_names.expanded_d2_names}.{columns.valid_yr.name} = {table_names.standard_name_to_firmid}.{columns.valid_yr.name} AND
    {table_names.expanded_d2_names}.{columns.model_origin.name} = "STANDARD" AND
    {table_names.standard_name_to_firmid}.{columns.model_origin.name} = "A1";
-- Use {table_names.expanded_d2_names} to process {table_names.standard_name_to_firmid}
-- and now put it all back in overwriting any results from the A1 with the D2
DELETE FROM {table_names.standard_name_to_firmid}
WHERE EXISTS (
    SELECT 1
    FROM
        {table_names.expanded_d2_names}
    WHERE
        {table_names.standard_name_to_firmid}.{columns.standard_name.name} = {table_names.expanded_d2_names}.{columns.standard_name.name} AND
        {table_names.standard_name_to_firmid}.{columns.alias_name.name} = {table_names.expanded_d2_names}.{columns.alias_name.name} AND
        {table_names.standard_name_to_firmid}.{columns.valid_yr.name} = {table_names.expanded_d2_names}.{columns.valid_yr.name}
);

DROP INDEX {table_names.sn_idx};
CREATE UNIQUE INDEX {table_names.sn_idx} ON {table_names.standard_name_to_firmid}
(
    {columns.valid_yr.name},
    {columns.alias_name.name},
    {columns.standard_name.name},
    {columns.firmid.name}
);

-- just need to know it was a D2 model initially
UPDATE {table_names.expanded_d2_names} SET {columns.model_origin.name} = "D2";
INSERT OR REPLACE INTO {table_names.standard_name_to_firmid}
SELECT *
FROM {table_names.expanded_d2_names};
        ''')


def create_name_information(fh, in_table, in_model_firmid, cw_yr):
    """

    """
    fh.write(
        f'''
DROP TABLE IF EXISTS {table_names.name_information};
CREATE TABLE {table_names.name_information} AS
SELECT DISTINCT
    {table_names.prdn_assg_names}.{columns.xml_name.name} AS {columns.xml_name.name},
    {table_names.prdn_assg_names}.{columns.uspto_name.name} AS {columns.uspto_name.name},
    {table_names.prdn_assg_names}.{columns.corrected_name.name} AS {columns.corrected_name.name},
    {table_names.prdn_assg_names}.{columns.name_match_name.name} AS {columns.name_match_name.name},
    {table_names.prdn_assg_names}.{columns.assg_st.name} AS {columns.assg_st.name},
    {table_names.prdn_assg_names}.{columns.assg_ctry.name} AS {columns.assg_ctry.name},
    {in_table}.{cw_yr} AS {columns.cw_yr.name},
    {in_table}.{columns.firmid.name} AS {in_model_firmid}
FROM
    {table_names.prdn_assg_names},
    {in_table}
WHERE
    {table_names.prdn_assg_names}.{columns.prdn.name} = {in_table}.{columns.prdn.name} AND
    {table_names.prdn_assg_names}.{columns.assg_seq.name} = {in_table}.{columns.assg_seq.name};
        ''')


def create_output_file(fh):
    """

    """
    def insert_into_f_models(fh, f_model_type, model_type):
        """

        """
        if model_type == 'A1':
            fh.write(
                f'''
INSERT INTO {table_names.final_f_models}''')
        elif model_type == 'D2':
            fh.write(
                f'''
INSERT OR REPLACE INTO {table_names.final_f_models}''')

        fh.write(
            f'''
SELECT DISTINCT
    {columns.prdn.name},
    {columns.num_inv.name},
    {columns.assg_seq.name},
    {columns.cw_yr.name},
    {columns.emp_yr.name},
    {columns.firmid.name},
    {columns.grant_yr.name},
    {columns.app_yr.name},
    {columns.assg_st.name},
    {columns.assg_ctry.name},
    {columns.assg_type.name},
    {columns.us_inv_flag.name},
    {columns.num_assg.name},
    '{f_model_type}',
    {columns.uniq_firmid.name},
            ''')
        if model_type == 'A1':
            fh.write(
                f'''
    SUM({columns.a1_prdns_with_standardized_name.name}),
    SUM({columns.a1_prdns_with_alias_name.name})''')
        elif model_type == 'D2':
            fh.write(
                f'''
    {shared_code.D2_dummy_number},
    {shared_code.D2_dummy_number}''')

        fh.write(
            f'''
FROM
    {table_names.f_models}
WHERE
    {columns.model.name} = '{model_type}'
GROUP BY
    {columns.prdn.name},
    {columns.assg_seq.name},
    {columns.firmid.name},
    {columns.cw_yr.name},
    {columns.emp_yr.name};
            ''')

    fh.write(
        f'''
-- this is the post-processed output
CREATE TABLE {table_names.final_f_models} (
    {columns.prdn.cmd},
    {columns.num_inv.cmd},
    {columns.assg_seq.cmd},
    {columns.cw_yr.cmd},
    {columns.emp_yr.cmd},
    {columns.firmid.cmd},
    {columns.grant_yr.cmd},
    {columns.app_yr.cmd},
    {columns.assg_st.cmd},
    {columns.assg_ctry.cmd},
    {columns.assg_type.cmd},
    {columns.us_inv_flag.cmd},
    {columns.num_assg.cmd},
    {columns.model.cmd},
    {columns.uniq_firmid.cmd},
    {columns.a1_prdns_with_standardized_name.cmd},
    {columns.a1_prdns_with_alias_name.cmd},
    UNIQUE (
        {columns.prdn.name},
        {columns.assg_seq.name},
        {columns.firmid.name}
    )
);
        ''')

    insert_into_f_models(fh, 'FA1', 'A1')
    insert_into_f_models(fh, 'FD2', 'D2')

    fh.write(
        f'''
CREATE TABLE {table_names.total_counts}
AS
SELECT
    {columns.prdn.name},
    {columns.assg_seq.name},
    SUM({columns.a1_prdns_with_standardized_name.name}) AS {columns.total_sn_count.name},
    SUM({columns.a1_prdns_with_alias_name.name}) AS {columns.total_an_count.name}
FROM
    {table_names.final_f_models}
GROUP BY
    {columns.prdn.name},
    {columns.assg_seq.name};
CREATE UNIQUE INDEX tc_indx ON {table_names.total_counts}(
    {columns.prdn.name},
    {columns.assg_seq.name}
);

CREATE TABLE {table_names.output_f_models}
AS
SELECT
    {table_names.final_f_models}.{columns.prdn.name},
    {table_names.final_f_models}.{columns.assg_seq.name},
    {columns.firmid.name},
    {columns.app_yr.name},
    {columns.grant_yr.name},
    {columns.assg_type.name},
    {columns.assg_st.name},
    {columns.assg_ctry.name},
    0 AS {columns.us_assg_flag.name},
    0 AS {columns.foreign_assg_flag.name},
    {columns.us_inv_flag.name},
    {columns.num_assg.name},
    {columns.cw_yr.name},
    {columns.emp_yr.name},
    {columns.model.name},
    {columns.uniq_firmid.name},
    {columns.num_inv.name}
FROM
    {table_names.final_f_models},
    {table_names.total_counts}
WHERE
    {table_names.final_f_models}.firmid != '' AND
    {table_names.final_f_models}.{columns.prdn.name} = {table_names.total_counts}.{columns.prdn.name} AND
    {table_names.final_f_models}.{columns.assg_seq.name} = {table_names.total_counts}.{columns.assg_seq.name} AND
    ( 1.0 * {columns.a1_prdns_with_standardized_name.name} ) / {columns.total_sn_count.name} > 0.7 AND
    ( 1.0 * {columns.a1_prdns_with_alias_name.name} ) / {columns.total_an_count.name} > 0.7
ORDER BY
    {table_names.final_f_models}.{columns.prdn.name},
    {table_names.final_f_models}.{columns.assg_seq.name};

-- a state => US assignee
UPDATE {table_names.output_f_models}
SET {columns.us_assg_flag.name} = 1
WHERE {columns.assg_st.name} != "";
-- no state + country => foreign assignee
UPDATE {table_names.output_f_models}
SET {columns.foreign_assg_flag.name} = 1
WHERE
    {columns.us_assg_flag.name} != 1 AND
    {columns.assg_ctry.name} != "";
        ''')


def create_standard_name_to_firmid(fh):
    """

    """
    fh.write(
        f'''
CREATE TABLE {table_names.standard_name_to_firmid} (
    {columns.standard_name.cmd},
    {columns.alias_name.cmd},
    {columns.valid_yr.cmd},
    {columns.firmid.cmd},
    {columns.state.cmd},
    {columns.country.cmd},
    {columns.model_origin.cmd},
    {columns.sn_on_prdn_count.cmd},
    {columns.alias_on_prdn_count.cmd}
);
CREATE UNIQUE INDEX {table_names.sn_idx} ON {table_names.standard_name_to_firmid}
(
    {columns.standard_name.name},
    {columns.valid_yr.name},
    {columns.firmid.name},
    {columns.alias_name.name},
    {columns.model_origin.name}
);
        ''')


def f_models(fh, earliest_grant_yr, shift_yrs):
    """

    """
    def inserts(f, first_grant_yr, name_column, shift_yr):
        fh.write(
            f'''
INSERT OR IGNORE INTO {table_names.f_models}
SELECT
    {table_names.prdn_assg_names}.{columns.prdn.name},
    "",
    {table_names.prdn_assg_names}.{columns.assg_seq.name},
    {table_names.standard_name_to_firmid}.{columns.valid_yr.name},
    "",
    {table_names.standard_name_to_firmid}.{columns.firmid.name},
    {table_names.prdn_metadata}.{columns.grant_yr.name},
    {table_names.prdn_metadata}.{columns.app_yr.name},
    {table_names.prdn_assg_names}.{columns.assg_st.name},
    {table_names.prdn_assg_names}.{columns.assg_ctry.name},
    {table_names.prdn_assg_names}.{columns.assg_type.name},
    {table_names.prdn_metadata}.{columns.us_inv_flag.name},
    {table_names.prdn_metadata}.{columns.num_assg.name},
    {table_names.standard_name_to_firmid}.{columns.model_origin.name},
    "",
    {table_names.standard_name_to_firmid}.{columns.standard_name.name},
    {table_names.standard_name_to_firmid}.{columns.sn_on_prdn_count.name},
    {table_names.standard_name_to_firmid}.{columns.alias_name.name},
    {table_names.standard_name_to_firmid}.{columns.alias_on_prdn_count.name}
FROM
    {table_names.prdn_metadata},
    {table_names.prdn_assg_names},
    {table_names.standard_name_to_firmid}
WHERE
    {table_names.prdn_metadata}.{columns.grant_yr.name} >= {first_grant_yr} AND
    {table_names.prdn_assg_names}.{columns.prdn.name} = {table_names.prdn_metadata}.{columns.prdn.name} AND
    {table_names.standard_name_to_firmid}.{columns.valid_yr.name} = {table_names.prdn_metadata}.{columns.grant_yr.name} {shift_yr} AND
    {table_names.standard_name_to_firmid}.{columns.alias_name.name} = {table_names.prdn_assg_names}.{name_column};
            ''')

    fh.write(
        f'''
-- this will be the final output
CREATE TABLE {table_names.f_models} (
    {columns.prdn.cmd},
    {columns.num_inv.cmd},
    {columns.assg_seq.cmd},
    {columns.cw_yr.cmd},
    {columns.emp_yr.cmd},
    {columns.firmid.cmd},
    {columns.grant_yr.cmd},
    {columns.app_yr.cmd},
    {columns.assg_st.cmd},
    {columns.assg_ctry.cmd},
    {columns.assg_type.cmd},
    {columns.us_inv_flag.cmd},
    {columns.num_assg.cmd},
    {columns.model.cmd},
    {columns.uniq_firmid.cmd},
    {columns.standard_name.cmd},
    {columns.a1_prdns_with_standardized_name.cmd},
    {columns.alias_name.cmd},
    {columns.a1_prdns_with_alias_name.cmd},
    UNIQUE (
        {columns.prdn.name},
        {columns.assg_seq.name},
        {columns.firmid.name},
        {columns.model.name},
        {columns.standard_name.name},
        {columns.alias_name.name}
    )
);
        ''')

    for shift_yr in shift_yrs:
        inserts(fh, earliest_grant_yr, columns.name_match_name.name, shift_yr)
        inserts(fh, earliest_grant_yr, columns.corrected_name.name, shift_yr)
        inserts(fh, earliest_grant_yr, columns.uspto_name.name, shift_yr)
        inserts(fh, earliest_grant_yr, columns.xml_name.name, shift_yr)
        fh.write(
            f'''
DELETE FROM {table_names.prdn_assg_names}
WHERE EXISTS
    (
        SELECT *
        FROM {table_names.f_models}
        WHERE
            {table_names.f_models}.{columns.prdn.name} = {table_names.prdn_assg_names}.{columns.prdn.name} AND
            {table_names.f_models}.{columns.assg_seq.name} = {table_names.prdn_assg_names}.{columns.assg_seq.name}
    );
            ''')


def final_standard_name_to_firmid(fh):
    """

    """
    fh.write(
        f'''
-- For non-1st assignees see if the alias is mapped to a standard name
UPDATE OR IGNORE {table_names.standard_name_to_firmid}
SET
    {columns.standard_name.name} = (
        SELECT sntf.{columns.standard_name.name}
        FROM {table_names.standard_name_to_firmid} AS sntf
        WHERE
            {table_names.standard_name_to_firmid}.{columns.alias_name.name} = sntf.{columns.alias_name.name} AND
            {table_names.standard_name_to_firmid}.{columns.valid_yr.name} = sntf.{columns.valid_yr.name} AND
            sntf.{columns.standard_name.name} != ""
    )
WHERE EXISTS (
        SELECT sntf.{columns.standard_name.name}
        FROM {table_names.standard_name_to_firmid} AS sntf
        WHERE
            {table_names.standard_name_to_firmid}.{columns.alias_name.name} = sntf.{columns.alias_name.name} AND
            {table_names.standard_name_to_firmid}.{columns.valid_yr.name} = sntf.{columns.valid_yr.name} AND
            sntf.{columns.standard_name.name} != ""
    )
    AND
    {columns.standard_name.name} = "";

UPDATE OR IGNORE {table_names.standard_name_to_firmid}
SET
    {columns.standard_name.name} = (
        SELECT DISTINCT sntf.{columns.standard_name.name}
        FROM {table_names.standard_name_to_firmid} AS sntf
        WHERE
            {table_names.standard_name_to_firmid}.{columns.alias_name.name} = sntf.{columns.alias_name.name} AND
            sntf.{columns.standard_name.name} != ""
    )
WHERE
    {columns.standard_name.name} = ""
    AND
    (
        SELECT COUNT( DISTINCT sntf.{columns.standard_name.name})
        FROM {table_names.standard_name_to_firmid} AS sntf
        WHERE
            {table_names.standard_name_to_firmid}.{columns.alias_name.name} = sntf.{columns.alias_name.name} AND
            sntf.{columns.standard_name.name} != ""
    ) = 1;
-- fallback position
UPDATE OR IGNORE {table_names.standard_name_to_firmid}
SET
    {columns.standard_name.name} = {columns.alias_name.name}
WHERE
    {columns.standard_name.name} = "";
-- and clean things up
DELETE FROM {table_names.standard_name_to_firmid} WHERE {columns.standard_name.name} = "";

-- put the counts in to determine which names are most common
-- only A1 1st assignee information is used
CREATE TABLE {table_names.name_counts} AS
SELECT
    COUNT( DISTINCT {table_names.prdn_assg_names}.{columns.prdn.name} ) AS {columns.count.name},
    {table_names.prdn_assg_names}.{columns.corrected_name.name} AS {columns.standard_name.name},
    {table_names.prdn_assg_names}.{columns.uspto_name.name} AS {columns.uspto_name.name},
    {table_names.prdn_assg_names}.{columns.xml_name.name} AS {columns.xml_name.name},
    {table_names.prdn_assg_names}.{columns.name_match_name.name} AS {columns.name_match_name.name},
    {table_names.a1_models}.{columns.cw_yr.name} AS {columns.cw_yr.name},
    {table_names.a1_models}.{columns.firmid.name} AS {columns.firmid.name}
FROM
    {table_names.prdn_assg_names},
    {table_names.a1_models}
WHERE
    {table_names.prdn_assg_names}.{columns.prdn.name} = {table_names.a1_models}.{columns.prdn.name} AND
    {table_names.prdn_assg_names}.{columns.assg_seq.name} = {table_names.a1_models}.{columns.assg_seq.name} AND
    {table_names.prdn_assg_names}.{columns.corrected_name.name} != "" AND
    {table_names.prdn_assg_names}.{columns.uspto_name.name} != "" AND
    {table_names.prdn_assg_names}.{columns.xml_name.name} != ""
GROUP BY
    {table_names.prdn_assg_names}.{columns.corrected_name.name},
    {table_names.prdn_assg_names}.{columns.uspto_name.name},
    {table_names.prdn_assg_names}.{columns.xml_name.name},
    {table_names.prdn_assg_names}.{columns.name_match_name.name},
    {table_names.a1_models}.{columns.cw_yr.name},
    {table_names.a1_models}.{columns.firmid.name};

CREATE INDEX sn_name_counts_indx ON {table_names.name_counts}
({columns.cw_yr.name}, {columns.firmid.name}, {columns.standard_name.name});
CREATE INDEX un_name_counts_indx ON {table_names.name_counts}
({columns.cw_yr.name}, {columns.firmid.name}, {columns.uspto_name.name});
CREATE INDEX xn_name_counts_indx ON {table_names.name_counts}
({columns.cw_yr.name}, {columns.firmid.name}, {columns.xml_name.name});
CREATE INDEX nm_name_counts_indx ON {table_names.name_counts}
({columns.cw_yr.name}, {columns.firmid.name}, {columns.name_match_name.name});

UPDATE {table_names.standard_name_to_firmid}
SET
    {columns.sn_on_prdn_count.name} = (
        SELECT SUM({columns.count.name})
        FROM {table_names.name_counts}
        WHERE
            {table_names.standard_name_to_firmid}.{columns.standard_name.name} = {table_names.name_counts}.{columns.standard_name.name} AND
            {table_names.standard_name_to_firmid}.{columns.valid_yr.name} = {table_names.name_counts}.{columns.cw_yr.name} AND
            {table_names.standard_name_to_firmid}.{columns.firmid.name} = {table_names.name_counts}.{columns.firmid.name}
    );

UPDATE {table_names.standard_name_to_firmid}
SET
    {columns.alias_on_prdn_count.name} = (
        SELECT SUM({columns.count.name})
        FROM {table_names.name_counts}
        WHERE
            (
                {table_names.standard_name_to_firmid}.{columns.alias_name.name} = {table_names.name_counts}.{columns.uspto_name.name}
                OR
                {table_names.standard_name_to_firmid}.{columns.alias_name.name} = {table_names.name_counts}.{columns.xml_name.name}
                OR
                {table_names.standard_name_to_firmid}.{columns.alias_name.name} = {table_names.name_counts}.{columns.name_match_name.name}
            ) AND
            {table_names.standard_name_to_firmid}.{columns.valid_yr.name} = {table_names.name_counts}.{columns.cw_yr.name} AND
            {table_names.standard_name_to_firmid}.{columns.firmid.name} = {table_names.name_counts}.{columns.firmid.name}
    );

DROP INDEX {table_names.sn_idx};
CREATE INDEX {table_names.sn_idx} ON {table_names.standard_name_to_firmid}
(
    {columns.valid_yr.name},
    {columns.alias_name.name}
);
        ''')


def put_d2_standard_name_to_firmid(fh):
    """

    """
    fh.write(
        f'''
INSERT OR REPLACE INTO {table_names.standard_name_to_firmid}
    (
        {columns.standard_name.name},
        {columns.alias_name.name},
        {columns.valid_yr.name},
        {columns.firmid.name},
        {columns.state.name},
        {columns.country.name},
        {columns.model_origin.name}
    )
SELECT
    {columns.assg_name.name},
    {columns.assg_name.name},
    {columns.year.name},
    {columns.firmid.name},
    "",
    "",
    "D2"
FROM
    {table_names.assg_name_firmid};
        ''')


def remove_trash_standard_name_to_firmid(fh):
    """

    """
    fh.write(
        f'''
DELETE FROM {table_names.standard_name_to_firmid}
WHERE
    (
        {columns.standard_name.name} == "" AND
        {columns.alias_name.name} == ""
    )
    OR
    (
        {columns.standard_name.name} == "INDIVIDUALLY OWNED PATENT" OR
        {columns.alias_name.name} == "INDIVIDUALLY OWNED PATENT"
    );
        ''')
    with open(file_names.f_model_standard_name_corrections_cfg_TITLED) as infile:
        fh.write(infile.read())


def update_standard_name_to_firmid(fh, model_name, model_firmid):
    """

    """
    def helper_fun(fh, alt_name, model, firmid):
        """

        """
        if model == 'A1':
            fh.write(
                f'''
--      standard name -> mis-/alternate spelling -> firmid
INSERT OR IGNORE INTO {table_names.standard_name_to_firmid}
                ''')
        elif model == 'D2':
            fh.write(
                f'''
--      standard name -> mis-/alternate spelling -> firmid
INSERT OR REPLACE INTO {table_names.standard_name_to_firmid}
                ''')

        fh.write(
            f'''
    (
        {columns.standard_name.name},
        {columns.alias_name.name},
        {columns.valid_yr.name},
        {columns.firmid.name},
        {columns.state.name},
        {columns.country.name},
        {columns.model_origin.name}
    )
SELECT
    {columns.corrected_name.name},
    {alt_name},
    {columns.cw_yr.name},
    {firmid},
    {columns.assg_st.name},
    {columns.assg_ctry.name},
    "{model}"
FROM
    {table_names.name_information}
WHERE
    {alt_name} != "";
            ''')

    if model_name == 'A1':
        helper_fun(fh, columns.corrected_name.name, model_name, model_firmid)
        helper_fun(fh, columns.xml_name.name, model_name, model_firmid)
        helper_fun(fh, columns.uspto_name.name, model_name, model_firmid)
        helper_fun(fh, columns.name_match_name.name, model_name, model_firmid)
    elif model_name == 'D2':
        helper_fun(fh, columns.corrected_name.name, model_name, model_firmid)
        helper_fun(fh, columns.uspto_name.name, model_name, model_firmid)
        helper_fun(fh, columns.xml_name.name, model_name, model_firmid)
        helper_fun(fh, columns.name_match_name.name, model_name, model_firmid)


def generate_f_model_sql_script(sql_script_fn, assignee_years):
    """

    """
    with open(sql_script_fn, 'w') as f:
        shared_code.model_header(f)
        shared_code.in_data_tables(f, 'F', assignee_years)
        create_standard_name_to_firmid(f)
        create_name_information(f, table_names.a1_models, columns.a1_model_firmid.name, columns.cw_yr.name)
        update_standard_name_to_firmid(f, 'A1', columns.a1_model_firmid.name)
        create_name_information(f, table_names.d2_models, columns.d2_model_firmid.name, columns.grant_yr.name)
        update_standard_name_to_firmid(f, 'D2', columns.d2_model_firmid.name)
        remove_trash_standard_name_to_firmid(f)
        put_d2_standard_name_to_firmid(f)
        create_expanded_d2_name(f)
        final_standard_name_to_firmid(f)
        shared_code.output_distinct_data(f, f'{table_names.standard_name_to_firmid}', f'{file_names.standard_name_to_firmid}')
        f_models(f, shared_code.earliest_grant_yr, shared_code.shift_yrs)
        create_output_file(f)
        shared_code.output_distinct_data(f, f'{table_names.output_f_models}', f'{file_names.f_models}')
