-- ================================================

-- ================================================
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Can't use defaults in SQLCLR
-- so we'll abstract through a functions
-- and apply defaults there
-- =============================================
CREATE  procedure dbo.exResultSetCapture
(
    @Command varchar(max),
    @rsTable1 varchar(max) = '', @rsColumnList1 varchar(max) = '',
    @rsTable2 varchar(max) = '', @rsColumnList2 varchar(max) = ''

)
AS
BEGIN
	
    EXEC ResultSetCapture @Command=@Command, @rsTable1=@rsTable1, @rsColumnList1=@rsColumnList1
    , @rsTable2=@rsTable2, @rsColumnList2=@rsColumnList2

	RETURN 0

END
GO

