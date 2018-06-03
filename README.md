# ResultSetCapture
Capture one or more result sets from a MS SQL command

ResultSetCapture is a SQL CLR Procedure that can take a SQL procedure call that returns multiple result sets and map the results to temporary tables.

This can be useful in cases where the procedure performs an "Insert Into" as INSERT INTO cannot be nested. 

Useful when creating tests for tSQLt.
