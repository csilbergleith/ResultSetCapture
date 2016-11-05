
if object_id('tempdb..#result') is not null
    drop table #result;

CREATE  table #result (id int identity);

if object_id('tempdb..#dataTypeTable') is not null
    drop table #dataTypeTable;

CREATE Table #dataTypeTable
(tInt      int ,
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
tText      text 
);


insert #dataTypeTable
(tInt,tVarChar, tGUID, tVarBinary, tBit, tDatetime, tBigInt,tDecimal,tMoney,tTinyInt, tSmallMoney, tSmallInt, tNumeric, tNVarChar, tText)
values (100, 'This is a string', newId(), 0x0f031ead0f, 1, getdate(),847274885762, 324628734.8763, 3457.9841,11,19.95,509, 9845394.97543,N'is 你好，世界 |*','This is a text field');


--select * from #dataTypeTable

--EXEC exResultSetCapture 
--      @Command='customerNameAndBalance_sp' -- select * from #dataTypeTable 
--    , @rsTable1='#result', @rsColumnList1='*';

EXEC exResultSetCapture 
      @Command='exec [dbo].[customerBalanceByLastName]	@lastName=''Brown'''
    , @rsTable1='#result', @rsColumnList1='*';


--drop table tTbl

select * 
--into tTbl 
from #result;
