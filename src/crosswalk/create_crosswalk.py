import triangulation.src.shared_code.column_names as columns
import triangulation.src.shared_code.file_names as file_names
import triangulation.src.shared_code.shared_code as shared_code
import triangulation.src.shared_code.table_names as table_names


models_and_tables = {
    file_names.a1_models: table_names.a1_models,
    file_names.a2_models: table_names.a2_models,
    file_names.a3_models: table_names.a3_models,
    file_names.b1_models: table_names.b1_models,
    file_names.b2_models: table_names.b2_models,
    file_names.c1_models: table_names.c1_models,
    file_names.c2_models: table_names.c2_models,
    file_names.c3_models: table_names.c3_models,
    file_names.d1_models: table_names.d1_models,
    file_names.d2_models: table_names.d2_models,
    file_names.e1_models: table_names.e1_models,
    file_names.e2_models: table_names.e2_models,
    file_names.f_models: table_names.f_models
}


def create_indexes(fh):
    """

    """
    for model, table in models_and_tables.items():
        fh.write(
            f'''
CREATE INDEX {table}_indx
ON {table}(
    {columns.prdn.name},
    {columns.assg_seq.name},
    {columns.firmid.name}
);
            ''')


def create_crosswalk_table(fh):
    '''

    '''
    tables = [*models_and_tables.values()]  # Want array of values
    del tables[-1]  # Don't want f_models
    fh.write(
        f'''
DROP TABLE IF EXISTS {table_names.crosswalk};
CREATE TABLE {table_names.crosswalk} AS SELECT * FROM {tables.pop(0)};''')
    for table in tables:
        fh.write(
            f'''
INSERT INTO {table_names.crosswalk} SELECT * FROM {table};''')
    fh.write(
        f'''
CREATE INDEX
    cw_indx
ON
    {table_names.crosswalk}(
        {columns.prdn.name},
        {columns.assg_seq.name}
);
-- Cleanup of the IOPs that escaped...
DELETE FROM {table_names.crosswalk}
WHERE EXISTS (
    SELECT *
    FROM {table_names.iops}
    WHERE
        {table_names.iops}.{columns.prdn.name} = {table_names.crosswalk}.{columns.prdn.name} AND
        {table_names.iops}.{columns.assg_seq.name} = {table_names.crosswalk}.{columns.assg_seq.name}
);

-- Now put in whatever didn't get a model assigned
DELETE FROM {table_names.full_frame}
WHERE EXISTS (
    SELECT *
    FROM {table_names.iops}
    WHERE
        {table_names.iops}.{columns.prdn.name} = {table_names.full_frame}.{columns.prdn.name} AND
        {table_names.iops}.{columns.assg_seq.name} = {table_names.full_frame}.{columns.assg_seq.name}
);
DELETE FROM {table_names.full_frame}
WHERE EXISTS (
    SELECT *
    FROM {table_names.crosswalk}
    WHERE
        {table_names.crosswalk}.{columns.prdn.name} = {table_names.full_frame}.{columns.prdn.name} AND
        {table_names.crosswalk}.{columns.assg_seq.name} = {table_names.full_frame}.{columns.assg_seq.name}
);
UPDATE {table_names.full_frame}
SET
    {columns.app_yr.name} =
        (
            SELECT {table_names.prdn_metadata}.{columns.app_yr.name}
            FROM {table_names.prdn_metadata}
            WHERE {table_names.full_frame}.{columns.prdn.name} = {table_names.prdn_metadata}.{columns.prdn.name}
        ),
    {columns.grant_yr.name} =
        (
            SELECT {table_names.prdn_metadata}.{columns.grant_yr.name}
            FROM {table_names.prdn_metadata}
            WHERE {table_names.full_frame}.{columns.prdn.name} = {table_names.prdn_metadata}.{columns.prdn.name}
        ),
    {columns.us_inv_flag.name} =
        (
            SELECT {table_names.prdn_metadata}.{columns.us_inv_flag.name}
            FROM {table_names.prdn_metadata}
            WHERE {table_names.full_frame}.{columns.prdn.name} = {table_names.prdn_metadata}.{columns.prdn.name}
        ),
    {columns.num_assg.name} =
        (
            SELECT {table_names.prdn_metadata}.{columns.num_assg.name}
            FROM {table_names.prdn_metadata}
            WHERE {table_names.full_frame}.{columns.prdn.name} = {table_names.prdn_metadata}.{columns.prdn.name}
        )
WHERE EXISTS (
    SELECT 1
    FROM {table_names.prdn_metadata}
    WHERE {table_names.full_frame}.{columns.prdn.name} = {table_names.prdn_metadata}.{columns.prdn.name}
);
ALTER TABLE {table_names.full_frame} ADD COLUMN {columns.f_model.name} TEXT;
INSERT INTO {table_names.crosswalk} SELECT * FROM {table_names.full_frame};''')


def import_full_frame(fh):
    """

    """
    fh.write(
        f'''
DROP TABLE IF EXISTS {table_names.full_frame};
.import {file_names.full_frame} {table_names.full_frame}
CREATE INDEX {table_names.full_frame}_indx
ON {table_names.full_frame}(
    {columns.prdn.name},
    {columns.assg_seq.name}
);''')


def import_other_models(fh):
    """

    """
    fh.write(
        f'''
.headers on''')
    for model, table in models_and_tables.items():
        fh.write(
            f'''
DROP TABLE IF EXISTS {table};
.import {model} {table}
ALTER TABLE {table} ADD COLUMN {columns.f_model.name};''')


def import_other_tables(fh):
    """

    """
    fh.write(
        f'''
.headers off
CREATE TABLE {table_names.iops} (
    {columns.prdn.cmd},
    {columns.assg_seq.cmd},
    UNIQUE(
        {columns.prdn.name},
        {columns.assg_seq.name}
    )
);
.import {file_names.iops_unique} {table_names.iops}

DROP TABLE IF EXISTS {table_names.prdn_metadata};
CREATE TABLE {table_names.prdn_metadata} (
    {columns.prdn.cmd},
    {columns.grant_yr.cmd},
    {columns.app_yr.cmd},
    {columns.num_assg.cmd},
    {columns.us_inv_flag.cmd},
    UNIQUE (
        {columns.prdn.name}
    )
);
.import {file_names.prdn_metadata_csvfile} {table_names.prdn_metadata}
.headers on''')


def output_crosswalk(fh, csv_file):
    """
    Helper function to outport data to a CSV file from a SQLite3 database.

    fh -- file handle
    tbl_name -- table in database to select data from
    csv_file -- CSV file to print to
    """
    fh.write(
        f'''
.output {csv_file}
SELECT *
FROM {table_names.crosswalk}
WHERE {columns.grant_yr.name} >= {shared_code.earliest_grant_yr} ;
.output stdout
    ''')


def prep_crosswalk_F(fh):
    """

    """
    tables = [*models_and_tables.values()]  # Want array of values
    f_model = tables.pop()  # Don't want f_models
    for table in tables:
        fh.write(
            f'''
UPDATE {table}
SET {columns.f_model.name} = (
    SELECT {f_model}.{columns.model.name}
    FROM {f_model}
    WHERE
        {table}.{columns.prdn.name} = {f_model}.{columns.prdn.name} AND
        {table}.{columns.assg_seq.name} = {f_model}.{columns.assg_seq.name} AND
        {table}.{columns.firmid.name} = {f_model}.{columns.firmid.name}
)
WHERE EXISTS (
    SELECT 1
    FROM {f_model}
    WHERE
        {table}.{columns.prdn.name} = {f_model}.{columns.prdn.name} AND
        {table}.{columns.assg_seq.name} = {f_model}.{columns.assg_seq.name} AND
        {table}.{columns.firmid.name} = {f_model}.{columns.firmid.name}
);
DELETE FROM {table}
WHERE EXISTS (
    SELECT 1
    FROM {f_model}
    WHERE
        {table}.{columns.prdn.name} = {f_model}.{columns.prdn.name} AND
        {table}.{columns.assg_seq.name} = {f_model}.{columns.assg_seq.name} AND
        {table}.{columns.firmid.name} = {f_model}.{columns.firmid.name}
);
DELETE FROM {f_model}
WHERE EXISTS (
    SELECT 1
    FROM {table}
    WHERE
        {table}.{columns.prdn.name} = {f_model}.{columns.prdn.name} AND
        {table}.{columns.assg_seq.name} = {f_model}.{columns.assg_seq.name} AND
        {table}.{columns.firmid.name} = {f_model}.{columns.firmid.name}
);
            ''')
    fh.write(
        f'''
UPDATE {f_model} SET {columns.f_model.name} = {f_model}.{columns.model.name};
        ''')


def generate_crosswalk_sql_script(sql_script_fn):
    """

    """
    with open(sql_script_fn, 'w') as f:
        shared_code.model_header(f)
        import_other_models(f)
        import_full_frame(f)
        import_other_tables(f)
        create_indexes(f)
        create_crosswalk_table(f)
        output_crosswalk(f, file_names.crosswalk)
        import_full_frame(f)
        prep_crosswalk_F(f)
        create_crosswalk_table(f)
        output_crosswalk(f, file_names.crosswalk_F)
