
if object_id('[ut_ResultSetCapture].[Test Varied Data Types - Schema Compare]') is not null
	drop PROCEDURE [ut_ResultSetCapture].[Test Varied Data Types - Schema Compare]
GO

CREATE procedure [ut_ResultSetCapture].[Test Varied Data Types - Schema Compare]
as
BEGIN

	-- Stage the tables to hold the result sets	
	if object_id('tempdb..#result') is not null
		drop table #result;

	CREATE  table #result (id int);
--------------------------------------------------

	-- Stage the dataTypes and populate
	if object_id('tempdb..#dataTypeTable') is not null
		drop table #dataTypeTable;

	CREATE Table #dataTypeTable
	(id		  int identity,
	tInt      int ,
	tNumeric   numeric(12,5) ,
	tVarChar   varchar(100) ,
	tGUID      uniqueIdentifier ,
	tVarBinary varbinary(200),
	tBit        bit ,
	tDatetime  datetime ,
	tBigInt    bigint ,
	tDecimal   decimal(16,4),
	tMoney     money ,
	tTinyInt  tinyint ,
	tSmallMoney smallmoney,
	tSmallInt  smallint ,
	tNVarChar  nvarchar(100),
	tText      text,
	config		xml 
	);

	insert #dataTypeTable
	(tInt,tVarChar, tGUID, tVarBinary, tBit, tDatetime, tBigInt,tDecimal,tMoney,tTinyInt, tSmallMoney, tSmallInt, tNumeric, tNVarChar, tText, config)
	values (100, 'This is a string', newId(), 0x0f031ead0f, 1, getdate(),847274885762, 324628734.8763, 3457.9841,11,19.95,509, 9845394.97543,N'is 你好，世界 |*','This is a text field', '<a><b>5</b></a>');
--------------------------------------------------

	-- Exec the SQLCLR and capture the results
	EXEC exResultSetCapture 
		  @Command='select * from #dataTypeTable'
		, @rsTable1='#result', @rsColumnList1='*';
--------------------------------------------------
	
	EXEC tSQLt.AssertEqualsTableSchema @Expected='#dataTypeTable', @Actual = '#result';


end
GO
