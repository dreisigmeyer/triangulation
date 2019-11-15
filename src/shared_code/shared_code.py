import itertools
import pandas as pd
import shutil
import uuid

import triangulation.src.shared_code.column_names as columns
import triangulation.src.shared_code.file_names as file_names
import triangulation.src.shared_code.table_names as table_names


"""
Code for working with CSV files
"""


def read_unique_csv_columns(in_filename, cols, out_file=None):
    """

    """
    df = pd.read_csv(in_filename, usecols=cols)
    if out_file:
        df.to_csv(out_file, sep=',', encoding='utf-8')
    else:
        return df.drop_duplicates()


def unique_sort_with_replacement(in_filename):
    """

    """
    name = str(uuid.uuid4())
    with open(in_filename, 'r') as f:
        with open(name, 'w') as holder:
            holder.writelines(unique_everseen(f))

    shutil.move(name, in_filename)


def unique_everseen(iterable, key=None):
    """
    List unique elements, preserving order. Remember all elements ever seen.
    Taken from: https://docs.python.org/3.7/library/itertools.html
    """
    seen = set()
    seen_add = seen.add
    if key is None:
        for element in itertools.filterfalse(seen.__contains__, iterable):
            seen_add(element)
            yield element
    else:
        for element in iterable:
            k = key(element)
            if k not in seen:
                seen_add(k)
                yield element


"""
Code for working with SQLite3 databases
"""


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


def in_data_tables(fh, model):
    """

    """
    if model == 'C':
        preprocess_for_c_model(fh)

    # Create all of the tables
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

    if model == 'A':
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
CREATE TABLE IF NOT EXISTS {table_names.assignee_info} (
    {columns.prdn.cmd},
    {columns.assg_seq.cmd},
    {columns.assg_type.cmd},
    {columns.assg_st.cmd},
    {columns.assg_ctry.cmd}
);
CREATE TABLE IF NOT EXISTS {table_names.prdn_metadata} (
    {columns.prdn.cmd},
    {columns.grant_yr.cmd},
    {columns.app_yr.cmd},
    {columns.num_assg.cmd},
    {columns.us_inv_flag.cmd}
);
        ''')

        # Load the data in
        import_data(fh, table_names.pik_data, file_names.pik_data_csvfile)
        import_data(fh, table_names.assignee_info, file_names.assignee_info_csvfile)
        import_data(fh, table_names.prdn_metadata, file_names.prdn_metadata_csvfile)

    # Make the indexes
    if model == 'A':
        idx_a_model(fh)
    if model == 'C':
        idx_c_model(fh)


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
    tbl_name -- table in database to load data into
    csv_file -- CSV file
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
    tbl_name -- table in database to load data into
    csv_file -- CSV file
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
