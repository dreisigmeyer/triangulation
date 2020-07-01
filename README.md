Code to find patent triangulation models.


## Getting the data
All of the required input datashould be placed into the **in\_data** directory.
Here is a list of the files and their descriptions.

- `assg_yr_firmid.csv`:
SQL table structure is  
~~~
CREATE TABLE assg_name_firmid (
    assg_name TEXT NOT NULL,
    year INTEGER,
    firmid TEXT NOT NULL,
    UNIQUE(assg_name, year, firmid)
);
~~~
- `assignee_76_16.csv`:
SQL table structure is  
~~~
CREATE TABLE IF NOT EXISTS assignee_name_data (
);
~~~
- `assignee_info.csv`: File created from CSV files generated during the preprocessing phase (see below for how to do this).
SQL table structure is  
~~~
CREATE TABLE IF NOT EXISTS assignee_info (
    prdn TEXT NOT NULL,
    assg_seq INTEGER NOT NULL,
    assg_type TEXT,
    assg_st TEXT,
    assg_ctry TEXT
);
~~~
- `name_match.csv`: Created upstream and may need to be preprocessed (see below for how to do this).
SQL table structure is  
~~~
CREATE TABLE name_match (
    xml_pat_num TEXT NOT NULL,
    uspto_pat_num TEXT NOT NULL,
    assg_seq INTEGER NOT NULL,
    grant_yr INTEGER NOT NULL,
    zip3_flag INTEGER,
    ein TEXT NOT NULL,
    firmid TEXT NOT NULL,
    pass_num INTEGER NOT NULL,
    cw_yr INTEGER NOT NULL
);
~~~
- `prdn_eins.csv`: File created from `name_match.csv` (see below for how to do this).  SQL table structure is  
~~~
CREATE TABLE IF NOT EXISTS ein_data (
    prdn TEXT NOT NULL,
    grant_yr INTEGER NOT NULL,
    assg_seq INTEGER NOT NULL,
    ein TEXT NOT NULL,
    firmid TEXT NOT NULL,
    cw_yr INTEGER NOT NULL,
    pass_num INTEGER NOT NULL
);
~~~
- `prdn_metadata.csv`:
SQL table structure is  
~~~
CREATE TABLE IF NOT EXISTS prdn_metadata (
    prdn TEXT NOT NULL,
    grant_yr INTEGER NOT NULL,
    app_yr INTEGER NOT NULL,
    num_assg INTEGER,
    us_inv_flag INTEGER
);
~~~
- `prdn_piks.csv`: File created from `carra_for_triangulation.csv` (see below for how to do this).  SQL table structure is  
~~~
CREATE TABLE IF NOT EXISTS pik_data (
    prdn TEXT NOT NULL,
    grant_yr INTEGER NOT NULL,
    app_yr INTEGER NOT NULL,
    inv_seq INTEGER NOT NULL,
    pik TEXT NOT NULL,
    ein TEXT NOT NULL,
    firmid TEXT NOT NULL,
    emp_yr INTEGER NOT NULL
);
~~~

Some of the input data files need to be preprocessed due to changes in file formats or files created.
As an example, the following commands were used for the run that occured in 2019-2020.

    # carra_for_triangulation  
    tail -n +2 carra_for_triangulation.csv > holder.csv  
    awk -F',' -v OFS=',' '{print $10,$6,$5,$8,$3,$4,$7,$1}' holder.csv > prdn_piks.csv
    rm holder.csv  
    # Put prdn_piks.csv into in_data directory  
    # Need to create firmid in name_match file and remove last two columns:  
    awk -F, -v OFS=, '{ print $1,$2,$3,$4,$5,$6,$12,$8,$9}' name_match.csv > name_match_HOLD.csv  
    mv name_match.csv name_match.csv_SAVED  
    tail -n +2 name_match_HOLD.csv > name_match.csv  
    awk -F',' -v OFS=',' '{print $1,$4,$3,$6,$7,$9,$8}' name_match.csv > prdn_eins.csv  
    rm name_match_HOLD.csv  
    # Put prdn_eins.csv into in_data directory  
    # Put name_match.csv into in_data directory  
    # Extend assg_yr_firmid.csv to new year  
    awk -F',' -v OFS=',' '{ if ($2==2015) {print $1,2016,$3"\n"$1,2017,$3}}' assg_yr_firmid.csv > holder.csv  
    cat holder.csv >> assg_yr_firmid.csv  
    # Create assignee_info.csv with structure PRDN,ASSG_NUM,ASSG_TYPE,ST,CTRY:  
    `awk -F'|' -v OFS=',' '{print $1,$6,$7,$9,$10}' assigneeOutData/*.csv > assignee_info.csv  
    # Cut last column off prdn_metadata.csv  


## Setting up the Python environment
The code was run with a standard Anaconda Python 3 environment (https://www.anaconda.com).


## Running the code
This can be run by issuing the following command outside of this directory  
`python -m triangulation -a assignee_76_16.csv`  
where **assignee_76_16.csv** is the assignee file.
The years may change on the file name.
The flag `-a` (or `--assignee_years`) is used on the command line to specify the assignee file.
This file name will likely change each year.


## The individual pieces of the code
The general methodology used her is to first generate SQLite3 code using Python, and then to run the SQLite3 code to find the triangulation models.
The Python code to generate each model is located in **src/[abcdef]_models** in the file **create\_[abcdef]\_models.py**.
The generated SQLite3 code is placed into **src/tmp** as **create\_[abcdef]\_models.sql**.


The directory **src/shared\_code** contains code that is called by multiple SQLite3 code generating Python files.
This includes files to collect together shared column names (**column\_names.py**), input and output file names (**file\_names.py**), and shared table names (**table\_names.py**).