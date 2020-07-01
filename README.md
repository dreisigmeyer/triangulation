Code to find patent triangulation models.


## Getting the data
All of the required input datashould be placed into the **in\_data** directory.
Here is a list of the files and their descriptions.

Some of the input data files needs to be preprocessed due to changes in file formats or files created.
As an example, the following commands were used for the run that occured in 2019-2020.

    `# carra_for_triangulation`  
    `tail -n +2 carra_for_triangulation.csv > holder.csv`  
    `awk -F',' -v OFS=',' '{print $10,$6,$5,$8,$3,$4,$7,$1}' holder.csv > prdn_piks.csv
    rm holder.csv`  
    `# Put prdn_piks.csv into sql directory`  
    `# Need to create firmid in name_match file and remove last two columns:`  
    `awk -F, -v OFS=, '{ print $1,$2,$3,$4,$5,$6,$12,$8,$9}' name_match.csv > name_match_HOLD.csv`  
    `mv name_match.csv name_match.csv_SAVED`  
    `tail -n +2 name_match_HOLD.csv > name_match.csv`  
    `awk -F',' -v OFS=',' '{print $1,$4,$3,$6,$7,$9,$8}' name_match.csv > prdn_eins.csv`  
    `rm name_match_HOLD.csv`  
    `# Put prdn_eins.csv into sql directory`  
    `# Put name_match.csv into inData directory`  
    `# Extend assg_yr_firmid.csv to new year`  
    `awk -F',' -v OFS=',' '{ if ($2==2015) {print $1,2016,$3"\n"$1,2017,$3}}' assg_yr_firmid.csv > holder.csv`  
    `cat holder.csv >> assg_yr_firmid.csv`  
    `# Create assignee_info.csv with structure PRDN,ASSG_NUM,ASSG_TYPE,ST,CTRY:`  
    `awk -F'|' -v OFS=',' '{print $1,$6,$7,$9,$10}' assigneeOutData/*.csv > assignee_info.csv`  
    `# Cut last column off prdn_metadata.csv`  
    `# Modify name ../inData/assignee_76_16.csv in create_Dmodels.sql and create_Fmodels.sql`  


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