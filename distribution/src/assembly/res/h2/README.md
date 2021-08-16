# MMA metadata debug tool
## Overview
MMA uses [H2](http://www.h2database.com/html/main.html) to store its metadata, including the 
migration configuration and progress. We can use the interactive command line tool provided by H2 
to query or modify the metadata of MMA. 

Execute the following command to run the command line tool:
```$xslt
sh h2.sh
```

You will be asked for a database URL, JDBC driver, user name, and password. The connection setting 
can also be set as command line parameters. After connecting, you will get the list of options. 
The built-in commands don't need to end with a semicolon, but SQL statements are only executed if 
the line ends with a semicolon ";".

## Examples
Please replace the variables with real values when executing the queries.

### Query the migration progress of a table
For non-partitioned tables:
```$xslt
sql> SELECT STATUS FROM MMA_TBL_META WHERE DB_NAME='${database_name}' AND TABLE_NAME='${table_name}';
```

For partitioned tables:
```$xslt
sql> SELECT STATUS, COUNT(1) FROM MMA_PT_META_DB_${database_name}.MMA_PT_META_TBL_${table_name} 
...> GROUP BY STATUS;
```
The query above shows the distribution of status over partitions.
