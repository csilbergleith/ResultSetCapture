
if object_id('[ut_ResultSetCapture].[Test Varied Data Types - Data Compare]') is not null
	drop PROCEDURE [ut_ResultSetCapture].[Test Varied Data Types - Data Compare]
GO

CREATE procedure [ut_ResultSetCapture].[Test Varied Data Types - Data Compare]
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
	tNVarChar  nvarchar(100)
	);

	insert #dataTypeTable
	(tInt,tVarChar, tGUID, tVarBinary, tBit, tDatetime, tBigInt,tDecimal,tMoney,tTinyInt, tSmallMoney, tSmallInt, tNumeric, tNVarChar)
	values (100, 'This is a string', newId(), 0x0f031ead0f, 1, getdate(),847274885762, 324628734.8763, 3457.9841,11,19.95,509, 9845394.97543,N'is 你好，世界 |*');
--------------------------------------------------

	-- Exec the SQLCLR and capture the results
	EXEC exResultSetCapture 
		  @Command='select * from #dataTypeTable'
		, @rsTable1='#result', @rsColumnList1='*';
--------------------------------------------------

	EXEC tSQLt.AssertEqualsTable @Expected='#dataTypeTable', @Actual = '#result';


end
GO
