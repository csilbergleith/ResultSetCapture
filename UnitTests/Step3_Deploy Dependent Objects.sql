SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

if object_id('Person') is not null
	DROP TABLE [dbo].[Person]
GO

CREATE TABLE [dbo].[Person](
	[BusinessEntityID] [int] NOT NULL,
	[PersonType] [nchar](2) NOT NULL,
	[NameStyle] [int] NOT NULL,
	[Title] [nvarchar](8) NULL,
	[FirstName] varchar(128) NOT NULL,
	[MiddleName] varchar(50) NULL,
	[LastName] varchar(128) NOT NULL,
	[Suffix] [nvarchar](10) NULL,
	[EmailPromotion] [int] NOT NULL,
	[AdditionalContactInfo] [xml],
	[Demographics] [xml],
	[rowguid] [uniqueidentifier] ROWGUIDCOL  NOT NULL,
	[ModifiedDate] [datetime] NOT NULL,
 CONSTRAINT [PK_Person_BusinessEntityID] PRIMARY KEY CLUSTERED 
(
	[BusinessEntityID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

GO
------------------------------------------------------------------------------------------------------------------------------------------

if object_id('emailAddress') is not null
	DROP TABLE [dbo].[EmailAddress]
GO

CREATE TABLE [dbo].[EmailAddress](
	[BusinessEntityID] [int] NOT NULL,
	[EmailAddressID] [int] IDENTITY(1,1) NOT NULL,
	[EmailAddress] [nvarchar](50) NULL,
	[rowguid] [uniqueidentifier] ROWGUIDCOL  NOT NULL,
	[ModifiedDate] [datetime] NOT NULL,
 CONSTRAINT [PK_EmailAddress_BusinessEntityID_EmailAddressID] PRIMARY KEY CLUSTERED 
(
	[BusinessEntityID] ASC,
	[EmailAddressID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
;

GO
------------------------------------------------------------------------------------------------------------------------------------------

if object_id('[dbo].[CustomerBalances]') is not null
	DROP TABLE [dbo].[CustomerBalances]
GO

CREATE TABLE [dbo].[CustomerBalances](
	[id] [int] IDENTITY(1,1) NOT NULL,
	[customerId] [int] NULL,
	[clearedBalance] [money] NULL,
	[unClearedBalance] [money] NULL
) ON [PRIMARY]

GO
------------------------------------------------------------------------------------------------------------------------------------------
-- 2 result sets with INSERT INTO 
if object_id('[dbo].[customerBalanceByLastName]') is not null
	DROP PROCEDURE [dbo].[customerBalanceByLastName]
GO

CREATE  PROCEDURE [dbo].[customerBalanceByLastName]
	@lastName	nvarchar(50) 
AS
BEGIN
SET NOCOUNT ON;

	declare @sql	nvarchar(max) = '';
	declare @details table
	(customerId	int,
	 firstName nvarchar(50),
	 lastName nvarchar(50),
	 clearedBalance money,
	 unclearedBalance money
	);

	set @sql = 'select c.customerId, p.FirstName, p.LastName, c.clearedBalance, c.unClearedBalance
	from dbo.Person p
	join CustomerBalances c on p.BusinessEntityID = c.customerId
	where p.LastName = @lastName';

	insert @details
	exec sp_executesql @sql,
    @parameters = N'@lastName nvarchar(50)',
    @lastname = @lastname;

	select d.customerId, d.firstName, d.lastName, d.clearedBalance, d.unclearedBalance, a.EmailAddress, a.rowguid
	from @details d
	join dbo.EmailAddress a on d.customerId = a.BusinessEntityID


	select lastName, sum(clearedBalance) as clearedBalance,
			sum(unclearedBalance) as unClearedBalance
	from @details
	group by lastName;

END
GO
------------------------------------------------------------------------------------------------------------------------------------------
-- 1 result set
if object_id('[dbo].[customerNameAndBalance_sp]') is not null
	drop proc [dbo].[customerNameAndBalance_sp];
GO

CREATE PROCEDURE [dbo].[customerNameAndBalance_sp]
   
AS
BEGIN
	SELECT b.customerId, c.FirstName, c.LastName, b.clearedBalance, b.unClearedBalance
	from CustomerBalances b
	JOIN Person c on c.BusinessEntityID = b.customerId ;
END
GO
------------------------------------------------------------------------------------------------------------------------------------------


-- Create the Test Class
if (select 1 from tsqlt.TestClasses where [name] = 'ut_ResultSetCapture') is NULL
	exec tsqlt.NewTestClass 'ut_ResultSetCapture'
GO


