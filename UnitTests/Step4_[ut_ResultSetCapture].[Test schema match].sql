
if object_id('[ut_ResultSetCapture].[Test schema match]') is not null
	drop PROCEDURE [ut_ResultSetCapture].[Test schema match]
GO

CREATE procedure [ut_ResultSetCapture].[Test schema match]
as
BEGIN

	-- Stage the tables to hold the result sets
	if object_id('tempdb..#detail') is not null
    drop table #detail;

	CREATE  table #detail (id int identity);

	if object_id('tempdb..#summary') is not null
		drop table #summary;

	CREATE  table #summary (id int identity);
---------------------------------------------------------------

	-- Prepare tables and data for the test
	exec tSQLt.FakeTable @TableName ='Person', @SchemaName='dbo';

	INSERT Person (BusinessEntityID, FirstName, LastName)
	VALUES (17, 'Kevin', 'Brown'),(27, 'Jo', 'Brown'),(79, 'Eric', 'Brown');

	exec tSQLt.FakeTable @TableName = 'CustomerBalances', @SchemaName='dbo';

	INSERT CustomerBalances (customerId, clearedBalance, unClearedBalance)
	VALUES (17,173.8767, 630.7768),(27, 173.8818, 30.6216),(79, 629.8014, 140.8562);

	exec tSQLt.FakeTable @TableName = 'EmailAddress', @SchemaName='dbo';

	INSERT EmailAddress (BusinessEntityID, EmailAddress, rowguid, ModifiedDate)
	VALUES (17, 'kevin0@adventure-works.com', 'F4332215-C861-4A54-99F5-B0C4F6406515', GETDATE()), 
		   (27, 'jo0@adventure-works.com', '98259561-ECCB-4257-9CF3-C4ED5E3356FC', GETDATE()), 
		   (79, 'eric1@adventure-works.com', 'DC243863-A00C-448C-B90F-509DFBE76F12', GETDATE())
		   	
	
	if object_id('tempdb.#expectedDetails') is not null
		drop table #expectedDetails;

	create table #expectedDetails
	(id int IDENTITY,
	 customerId int,
	 firstName varchar(128),
	 lastName varchar(128),
	 clearedBalance money,
	 unClearedBalance DECIMAL(9,4),
	 emailAddress nvarchar(50),
	 rowguid uniqueidentifier
	 );

	INSERT #expectedDetails
	(customerId, firstName, lastName, clearedBalance, unClearedBalance, emailAddress, rowguid)
	VALUES (17,'Kevin', 'Brown', 173.8767, 630.7768, 'kevin0@adventure-works.com', 'F4332215-C861-4A54-99F5-B0C4F6406515'),
		   (27,'Jo', 'Brown', 173.8818, 30.6216, 'jo0@adventure-works.com', '98259561-ECCB-4257-9CF3-C4ED5E3356FC'),
		   (79,'Eric', 'Brown', 629.8014, 140.8562, 'eric1@adventure-works.com', 'DC243863-A00C-448C-B90F-509DFBE76F12')
	
	CREATE table #expectedSummary
	(id int IDENTITY,
	 lastName varchar(128),
	 clearedBalance money,
	 unClearedBalance money
	);

	insert #expectedSummary (lastName, clearedBalance, unClearedBalance)
	VALUES ('Brown', 977.5599, 802.2546);

	--INSERT #expectedDetails
	--exec customerBalanceByLastName @lastName = 'Brown';
	
	EXEC exResultSetCapture 
		  @Command='exec [dbo].[customerBalanceByLastName]	@lastName=''Brown'''  
		, @rsTable1='#detail', @rsColumnList1='*'
		, @rsTable2='#summary', @rsColumnList2='*';

	EXEC tSQLt.AssertEqualsTableSchema @Expected='#expectedDetails', @Actual = '#detail';
	
	EXEC tSQLt.AssertEqualsTableSchema @Expected='#expectedSummary', @Actual = '#summary';
		
end
GO
