# import itertools
# import pandas as pd
# import shutil
# import uuid

import triangulation.src.shared_code.column_names as columns
import triangulation.src.shared_code.file_names as file_names
import triangulation.src.shared_code.table_names as table_names


# """
# Code for working with CSV files
# """


# def read_unique_csv_columns(in_filename, cols, out_file=None):
#     """

#     """
#     df = pd.read_csv(in_filename, usecols=cols)
#     if out_file:
#         df.to_csv(out_file, sep=',', encoding='utf-8')
#     else:
#         return df.drop_duplicates()


# def unique_sort_with_replacement(in_filename):
#     """

#     """
#     name = str(uuid.uuid4())
#     with open(in_filename, 'r') as f:
#         with open(name, 'w') as holder:
#             holder.writelines(unique_everseen(f))

#     shutil.move(name, in_filename)


# def unique_everseen(iterable, key=None):
#     """
#     List unique elements, preserving order. Remember all elements ever seen.
#     Taken from: https://docs.python.org/3.7/library/itertools.html
#     """
#     seen = set()
#     seen_add = seen.add
#     if key is None:
#         for element in itertools.filterfalse(seen.__contains__, iterable):
#             seen_add(element)
#             yield element
#     else:
#         for element in iterable:
#             k = key(element)
#             if k not in seen:
#                 seen_add(k)
#                 yield element


"""
Code for working with SQLite3 databases
"""

# Information for F models
earliest_grant_yr = '2000'
shift_yrs = ['', '- 1', '+ 1', '- 2', '+ 2']


def idx_a_model(fh):
    """

    """
    fh.write(
        f'''
CREATE INDEX
    {table_names.a_model_ein_big_idx}
ON
    {table_names.ein_data}(
        {columns.prdn.name},
        {columns.ein.name},
        {columns.firmid.name});

CREATE INDEX
    {table_names.a_model_pik_idx}
ON
    {table_names.pik_data}(
        {columns.prdn.name},
        {columns.ein.name},
        {columns.firmid.name});

CREATE INDEX
    {table_names.a_model_ein_small_idx}
ON
    {table_names.ein_data}(
        {columns.prdn.name},
        {columns.assg_seq.name});

CREATE INDEX
    assg_info_prdn_as_idx
ON
    {table_names.assignee_info}(
        {columns.prdn.name},
        {columns.assg_seq.name});

CREATE INDEX
    prdn_metadata_main_idx
ON
    {table_names.prdn_metadata}({columns.prdn.name});
    ''')


def idx_c_model(fh):
    """

    """
    fh.write(
        f'''
CREATE INDEX
    ein_data_main_idx
ON
    {table_names.ein_data}({columns.prdn.name});

CREATE INDEX
    pik_prdn_ein_firmid_idx
ON
    {table_names.pik_data}(
        {columns.prdn.name},
        {columns.app_yr.name},
        {columns.inv_seq.name},
        {columns.pik.name},
        {columns.firmid.name},
        {columns.emp_yr.name});
    ''')


def idx_d_model(fh):
    """

    """
    fh.write(
        f'''
CREATE INDEX a1_models_indx
ON {table_names.a1_models} (
    {columns.prdn.name},
    {columns.assg_firmid.name},
    {columns.assg_seq.name});
CREATE INDEX a2_models_indx
ON {table_names.a2_models} (
    {columns.prdn.name},
    {columns.assg_firmid.name},
    {columns.assg_seq.name});
CREATE INDEX a3_models_indx
ON {table_names.a3_models} (
    {columns.prdn.name},
    {columns.assg_firmid.name},
    {columns.assg_seq.name});
CREATE INDEX b1_models_indx
ON {table_names.b1_models} (
    {columns.prdn.name},
    {columns.assg_seq.name});
CREATE INDEX b2_models_indx
ON {table_names.b2_models} (
    {columns.prdn.name},
    {columns.assg_seq.name});
CREATE INDEX c1_models_indx
ON {table_names.c1_models} (
    {columns.prdn.name});
CREATE INDEX c2_models_indx
ON {table_names.c2_models} (
    {columns.prdn.name});
CREATE INDEX c3_models_indx
ON {table_names.c3_models} (
    {columns.prdn.name});
CREATE INDEX e1_models_indx
ON {table_names.e1_models} (
    {columns.prdn.name},
    {columns.assg_seq.name});
CREATE INDEX e2_models_indx
ON {table_names.e2_models} (
    {columns.prdn.name});
CREATE INDEX
    assignee_name_data_indx
ON
    {table_names.assignee_name_data} (
        {columns.xml_pat_num.name},
        {columns.assg_seq.name});

CREATE INDEX
    name_match_indx
ON
    {table_names.name_match} (
        {columns.xml_pat_num.name},
        {columns.assg_seq.name});

CREATE INDEX
    assg_info_prdn_as_idx
ON
    {table_names.assignee_info}(
        {columns.prdn.name},
        {columns.assg_seq.name});

CREATE INDEX
    prdn_metadata_main_idx
ON
    {table_names.prdn_metadata}(
        {columns.prdn.name});
    ''')


def idx_e_model(fh):
    """

    """
    fh.write(
        f'''
CREATE INDEX
    ein_data_main_idx
ON
    {table_names.ein_data}(
        {columns.prdn.name},
        {columns.assg_seq.name},
        {columns.firmid.name},
        {columns.cw_yr.name});

CREATE INDEX
    pik_data_main_idx
ON
    {table_names.pik_data}(
        {columns.prdn.name},
        {columns.firmid.name},
        {columns.emp_yr.name});

CREATE INDEX
    assg_info_prdn_as_idx
ON
    {table_names.assignee_info}(
        {columns.prdn.name},
        {columns.assg_seq.name});

CREATE INDEX
    prdn_metadata_main_idx
ON
    {table_names.prdn_metadata}(
        {columns.prdn.name});
    ''')


def idx_f_model(fh):
    """

    """
    fh.write(
        f'''
CREATE INDEX an_xml_indx
ON {table_names.prdn_assg_names}
({columns.prdn.name}, {columns.assg_seq.name}, {columns.xml_name.name});
CREATE INDEX an_uspto_indx
ON {table_names.prdn_assg_names}
({columns.prdn.name}, {columns.assg_seq.name}, {columns.uspto_name.name});
CREATE INDEX an_corrected_indx
ON {table_names.prdn_assg_names}
({columns.prdn.name}, {columns.assg_seq.name}, {columns.corrected_name.name});
CREATE INDEX an_name_match_indx
ON {table_names.prdn_assg_names}
({columns.prdn.name}, {columns.assg_seq.name}, {columns.name_match_name.name});

CREATE UNIQUE INDEX a1_models_indx
ON {table_names.a1_models} (
    {columns.prdn.name},
    {columns.assg_seq.name},
    {columns.assg_firmid.name});

CREATE INDEX UNIQUE d2_models_indx
ON {table_names.d2_models} (
    {columns.prdn.name},
    {columns.assg_seq.name},
    {columns.assg_firmid.name});

CREATE UNIQUE INDEX prdn_metadata_indx
ON {table_names.prdn_metadata} (
    {columns.prdn.name},
    {columns.grant_yr.name});
    ''')


def import_data(fh, tbl_name, csv_file):
    """
    Helper function to import data from a CSV file into a SQLite3 database.

    fh -- file handle
    tbl_name -- table in database to load data into
    csv_file -- CSV file
    """
    fh.write(
        f'''
.import {csv_file} {tbl_name}
    ''')


def import_other_models_for_f(fh, assignee_years):
    """
    """
    fh.write(
        f'''
.headers ON
.import {file_names.a1_models} {table_names.a1_models}
.import {file_names.d2_models} {table_names.d2_models}
.import {assignee_years} {table_names.assignee_name_data}
.headers OFF

-- this is what was sent into the name match
CREATE TABLE holder (
    {columns.prdn.cmd},
    {columns.assg_seq.cmd},
    {columns.grant_yr.cmd},
    {columns.assg_st.cmd},
    {columns.assg_ctry.cmd},
    {columns.xml_name.cmd},
    {columns.uspto_name.cmd},
    {columns.corrected_name.cmd},
    {columns.assg_type.cmd},
    UNIQUE({columns.prdn.name}, {columns.assg_seq.name})
);
.import {file_names.prdn_seq_stand_name} holder

CREATE INDEX assignee_name_data_indx
ON {table_names.assignee_name_data}
({columns.xml_pat_num.name}, {columns.assg_num.name});

CREATE TABLE {table_names.prdn_assg_names} AS
SELECT
    holder.*,
    {table_names.assignee_name_data}.{columns.name.name} AS {columns.name_match_name.name}
FROM
    holder
JOIN
    {table_names.assignee_name_data}
ON
    holder.{columns.prdn.name} = {table_names.assignee_name_data}.{columns.xml_pat_num.name} AND
    holder.{columns.assg_seq.name} = {table_names.assignee_name_data}.{columns.assg_num.name};

DROP TABLE {table_names.assignee_name_data};
DROP TABLE holder;
    ''')


def import_other_models(fh, assignee_years):
    """
    """
    fh.write(
        f'''
.headers ON
.import {file_names.a1_models} {table_names.a1_models}
.import {file_names.a2_models} {table_names.a2_models}
.import {file_names.a3_models} {table_names.a3_models}
.import {file_names.b1_models} {table_names.b1_models}
.import {file_names.b2_models} {table_names.b2_models}
.import {file_names.c1_models} {table_names.c1_models}
.import {file_names.c2_models} {table_names.c2_models}
.import {file_names.c3_models} {table_names.c3_models}
.import {file_names.e1_models} {table_names.e1_models}
.import {file_names.e2_models} {table_names.e2_models}
.import {assignee_years} {table_names.assignee_name_data}
.headers OFF

ALTER TABLE {table_names.assignee_name_data}
RENAME COLUMN {columns.assg_num.name}
TO {columns.assg_seq.name};
ALTER TABLE {table_names.assignee_name_data}
RENAME COLUMN {columns.name.name}
TO {columns.assg_name.name};
    ''')


def in_data_tables(fh, model, assignee_years=None):
    """

    """
    if model == 'C':
        preprocess_for_c_model(fh)
    elif model == 'D':
        preprocess_for_d_model(fh)
    elif model == 'E':
        preprocess_for_e_model(fh)
    elif model == 'F':
        preprocess_for_f_model(fh)

    # Create all of the tables
    if model != 'D' and model != 'F':
        fh.write(
            f'''
CREATE TABLE IF NOT EXISTS {table_names.ein_data} (
    {columns.prdn.cmd},
    {columns.grant_yr.cmd},
    {columns.assg_seq.cmd},
    {columns.ein.cmd},
    {columns.firmid.cmd},
    {columns.cw_yr.cmd},
    {columns.pass_num.cmd}
);
        ''')
        import_data(fh, table_names.ein_data, file_names.ein_data_csvfile)

    if model == 'A' or model == 'E':
        fh.write(
            f'''
CREATE TABLE IF NOT EXISTS {table_names.pik_data} (
    {columns.prdn.cmd},
    {columns.grant_yr.cmd},
    {columns.app_yr.cmd},
    {columns.inv_seq.cmd},
    {columns.pik.cmd},
    {columns.ein.cmd},
    {columns.firmid.cmd},
    {columns.emp_yr.cmd}
);
        ''')
        import_data(fh, table_names.pik_data, file_names.pik_data_csvfile)

    if model == 'A' or model == 'D' or model == 'E' or model == 'F':
        fh.write(
            f'''
CREATE TABLE IF NOT EXISTS {table_names.prdn_metadata} (
    {columns.prdn.cmd},
    {columns.grant_yr.cmd},
    {columns.app_yr.cmd},
    {columns.num_assg.cmd},
    {columns.us_inv_flag.cmd}
);
        ''')

        # Load the data in
        import_data(fh, table_names.prdn_metadata, file_names.prdn_metadata_csvfile)

    if model == 'A' or model == 'D' or model == 'E':
        fh.write(
            f'''
CREATE TABLE IF NOT EXISTS {table_names.assignee_info} (
    {columns.prdn.cmd},
    {columns.assg_seq.cmd},
    {columns.assg_type.cmd},
    {columns.assg_st.cmd},
    {columns.assg_ctry.cmd}
);
        ''')

        # Load the data in
        import_data(fh, table_names.assignee_info, file_names.assignee_info_csvfile)

    if model == 'D':
        fh.write(
            f'''
CREATE TABLE {table_names.name_match} (
    {columns.xml_pat_num.cmd},
    {columns.uspto_pat_num.cmd},
    {columns.assg_seq.cmd},
    {columns.grant_yr.cmd},
    {columns.zip3_flag.cmd},
    {columns.ein.cmd},
    {columns.firmid.cmd},
    {columns.pass_num.cmd},
    {columns.cw_yr.cmd}
);
        ''')
        import_data(fh, table_names.name_match, file_names.name_match_csvfile)

    if model == 'D' or model == 'F':
        fh.write(
            f'''
-- from the hand-corrected D2 models
CREATE TABLE {table_names.assg_name_firmid} (
    {columns.assg_name.cmd},
    {columns.year.cmd},
    {columns.firmid.cmd},
    UNIQUE({columns.assg_name.name}, {columns.year.name}, {columns.firmid.name})
);
        ''')
        import_data(fh, table_names.assg_name_firmid, file_names.assg_yr_firmid_csvfile)

    # Make the indexes
    if model == 'A':
        idx_a_model(fh)
    elif model == 'C':
        idx_c_model(fh)
    elif model == 'D':
        import_other_models(fh, assignee_years)
        idx_d_model(fh)
    elif model == 'E':
        idx_e_model(fh)
    elif model == 'F':
        import_other_models_for_f(fh, assignee_years)
        idx_f_model(fh)


def model_header(fh):
    """

    """
    fh.write(
        f'''
.mode csv
pragma temp_store = MEMORY;
    ''')


def output_data(fh, tbl_name, csv_file):
    """
    Helper function to outport data to a CSV file from a SQLite3 database.

    fh -- file handle
    tbl_name -- table in database to select data from
    csv_file -- CSV file to print to
    """
    fh.write(
        f'''
.headers ON
.output {csv_file}
SELECT * FROM {tbl_name};
.output stdout
.headers OFF
    ''')


def output_distinct_data(fh, tbl_name, csv_file):
    """
    Helper function to outport data to a CSV file from a SQLite3 database.

    fh -- file handle
    tbl_name -- table in database to select data from
    csv_file -- CSV file to print to
    """
    fh.write(
        f'''
.headers ON
.output {csv_file}
SELECT DISTINCT * FROM {tbl_name};
.output stdout
.headers OFF
    ''')


def preprocess_for_c_model(fh):
    """

    """
    fh.write(
        f'''
DROP TABLE {table_names.ein_data};
DROP INDEX IF EXISTS {table_names.a_model_pik_idx};
DROP INDEX IF EXISTS {table_names.a_model_ein_big_idx};
DROP INDEX IF EXISTS {table_names.a_model_ein_small_idx};
    ''')


def preprocess_for_d_model(fh):
    """

    """
    fh.write(
        f'''
DROP TABLE {table_names.ein_data};
DROP TABLE {table_names.pik_data};
DROP TABLE {table_names.assignee_info};
DROP TABLE {table_names.prdn_metadata};
    ''')


def preprocess_for_e_model(fh):
    """

    """
    fh.write(
        f'''
DROP TABLE {table_names.ein_data};
DROP TABLE {table_names.pik_data};
DROP TABLE {table_names.assignee_info};
DROP TABLE {table_names.prdn_metadata};
    ''')


def preprocess_for_f_model(fh):
    """

    """
    fh.write(
        f'''
DROP TABLE {table_names.assignee_name_data};
DROP TABLE {table_names.assg_name_firmid};
DROP TABLE {table_names.prdn_metadata};
DROP TABLE {table_names.a1_models};
    ''')
