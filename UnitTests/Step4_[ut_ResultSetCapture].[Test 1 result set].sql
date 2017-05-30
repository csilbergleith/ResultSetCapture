
if object_id('[ut_ResultSetCapture].[Test 1 result set]') is not null
	drop PROCEDURE [ut_ResultSetCapture].[Test 1 result set]
GO

CREATE procedure [ut_ResultSetCapture].[Test 1 result set]
as
BEGIN

	-- Stage the tables to hold the result sets
	CREATE  table #detail (id int identity);

	CREATE  table #summary (id int identity);

---------------------------------------------------------------

	-- Prepare tables and data for the test
	exec tSQLt.FakeTable @TableName ='Person', @SchemaName='dbo';

	INSERT Person (BusinessEntityID, FirstName, LastName)
	VALUES (17, 'Kevin', 'Brown'),(27, 'Jo', 'Brown'),(79, 'Eric', 'Brown');

	exec tSQLt.FakeTable @TableName = 'CustomerBalances', @SchemaName='dbo';

	INSERT CustomerBalances (customerId, clearedBalance, unClearedBalance)
	VALUES (17,173.8767, 630.7768),(27, 173.8818, 30.6216),(79, 629.8014, 140.8562);

	-- run the test
	EXEC exResultSetCapture 
		  @Command='exec [dbo].[customerNameAndBalance_sp]'  
		, @rsTable1='#detail', @rsColumnList1='*'
		, @rsTable2='#summary', @rsColumnList2='*';

	-- Expected result
	create table #expectedDetails
	(id int IDENTITY,
	 customerId int,
	 firstName varchar(128),
	 lastName varchar(128),
	 clearedBalance money,
	 unClearedBalance DECIMAL(9,4)
	 );

	INSERT #expectedDetails
	(customerId, firstName, lastName, clearedBalance, unClearedBalance)
	VALUES (17,'Kevin', 'Brown', 173.8767, 630.7768),
		   (27,'Jo', 'Brown', 173.8818, 30.6216),
		   (79,'Eric', 'Brown', 629.8014, 140.8562);
	
	-- Assert
	EXEC tSQLt.AssertEqualsTable @Expected='#expectedDetails', @Actual = '#detail';
	
end
GO
