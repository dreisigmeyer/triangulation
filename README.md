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


## The individual pieces of the code
The general methodology used her is to first generate SQLite3 code using Python, and then to run the SQLite3 code to find the triangulation models.
The Python code to generate each model is located in **src/[abcdef]_models** in the file **create\_[abcdef]\_models.py**.
The generated SQLite3 code is placed into **src/tmp** as **create\_[abcdef]\_models.sql**.


The directory **src/shared\_code** contains code that is called by multiple SQLite3 code generating Python files.
This includes files to collect together shared column names (**column\_names.py**), input and output file names (**file\_names.py**), and shared table names (**table\_names.py**).