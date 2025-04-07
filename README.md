# sql_insert
Load txt files in csv format into MS SQL db table

Simple python script that loads the config from db_loader_config.py and adds the data from many txt files in csv format in a DB table in MSSQL. 
You can tweak the config file and the script to adapt it to your scenario.

It uses batches to load the files into DB and it can handle gigs of files 
