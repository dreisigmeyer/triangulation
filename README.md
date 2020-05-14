Code to find patent triangulation models.


## Getting the data
All of the required input datashould be placed into the **in\_data** directory.
Here is a list of the files and their descriptions.


## Setting up the Python environment
The code was run with a standard Anaconda Python 3 environment (https://www.anaconda.com).


## Running the code
This can be run by issuing the following command outside of this directory  
`python -m triangulation -a assignee_76_16.csv`  
where **assignee_76_16.csv** is the assignee file.
The years may change on the file name.
The flags `-a` or `--assignee_years` can be used on the command line.

The general methodology used her is to generate SQLite code using Python, and then to run the SQL code to find the triangulation models.
