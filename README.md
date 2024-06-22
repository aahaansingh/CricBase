Transforms IPL JSON data from CricSheet into a relational database format, stored as a SQLite database on disk. 
The schema can be found at https://dbdiagram.io/d/Cricket-Database-Design-66569b02b65d933879efa970, and the
CricSheet data can be found at https://cricsheet.org/downloads/

A database can be created locally by creating a new SQLdb object and calling create_db(). 
Contents can be accessed and modified using a cursor from the object's connection to run SQL commands.
