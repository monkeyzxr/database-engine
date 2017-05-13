# database-engine

Created by Xiangru Zhou

The goal of this project is to implement a (very) rudimentary database engine that is loosely based on a hybrid between MySQL and SQLite.

This pseudo database will only need to support actions on a single table at a time, no joins or nested queries. Like MySQL's InnoDB data engine (SDL), this project will use file-per-table approach to physical storage. Each database table will be physcially stored as a separate file. Each table file will be subdivided into logical sections of fixed equal size call pages. Therefore, each table file size will be exact increments of the global page_size attribute, i.e. all data files must share the same page_size attribute.

*************************************
Supported Commands:

SHOW TABLES;

CREATE TABLE table_name (
column_name1 INT PRIMARY KEY, column_name2 data_type2 [NOT NULL], column_name3 data_type3 [NOT NULL], ...
);

INSERT INTO TABLE table_name VALUES (value1,value2,value3,...);

SELECT *
FROM table_name
WHERE column_name operator value;

***************************************


