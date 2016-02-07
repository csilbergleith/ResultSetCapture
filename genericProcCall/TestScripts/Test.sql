

create table #Detail
(customerId	int,
 firstName nvarchar(50),
 lastname varchar(50),
 clearedBalance money,
 unclearedBalance money,
 emailAddress nvarchar(50),
 rowguid uniqueidentifier
);


create table #Summary
(lastName varchar(50),
 clearedBalance money,
 unClearedBalance money
 );
 
create table #Summary2
(lastName varchar(50),
 clearedBalance money,
 unClearedBalance money
 );

--EXEC ResultSetCapture @callXML= '<execute schema="dbo" proc="customerBalanceByLastName" >
--  <parm name="@lastName" value="Brown" />
--  <parm name="@NotUsed" value="******"  />
--  <parm name="@somexml" value="&lt;domain&gt;&lt;id&gt;1&lt;/id&gt;&lt;id&gt;2&lt;/id&gt;&lt;/domain&gt;" />
--  <output target="#Summary" resultsetseq="2" />
--</execute>';

EXEC ResultSetCapture @Command='exec customerBalanceByLastName @lastName=''Smith''', @rsTable1='#Detail', @rsColumnList1='customerId,firstName,lastname,clearedBalance,unclearedBalance,emailAddress,rowguid'
			,@rsTable2='#Summary', @rsColumnList2='*'



select '#Detail' as [#Detail], *
from #Detail;


select '#Summary' as [#Summary], *
from #Summary;
