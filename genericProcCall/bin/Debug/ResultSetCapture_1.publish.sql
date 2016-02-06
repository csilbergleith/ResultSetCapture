﻿/*
Deployment script for sandbox

This code was generated by a tool.
Changes to this file may cause incorrect behavior and will be lost if
the code is regenerated.
*/

GO
SET ANSI_NULLS, ANSI_PADDING, ANSI_WARNINGS, ARITHABORT, CONCAT_NULL_YIELDS_NULL, QUOTED_IDENTIFIER ON;

SET NUMERIC_ROUNDABORT OFF;


GO
:setvar DatabaseName "sandbox"
:setvar DefaultFilePrefix "sandbox"
:setvar DefaultDataPath "C:\Program Files\Microsoft SQL Server\MSSQL11.MSSQLSERVER\MSSQL\DATA\"
:setvar DefaultLogPath "C:\Program Files\Microsoft SQL Server\MSSQL11.MSSQLSERVER\MSSQL\DATA\"

GO
:on error exit
GO
/*
Detect SQLCMD mode and disable script execution if SQLCMD mode is not supported.
To re-enable the script after enabling SQLCMD mode, execute the following:
SET NOEXEC OFF; 
*/
:setvar __IsSqlCmdEnabled "True"
GO
IF N'$(__IsSqlCmdEnabled)' NOT LIKE N'True'
    BEGIN
        PRINT N'SQLCMD mode must be enabled to successfully execute this script.';
        SET NOEXEC ON;
    END


GO
USE [$(DatabaseName)];


GO
PRINT N'Creating [dbo].[ResultSetCapture]...';


GO
CREATE PROCEDURE [dbo].[ResultSetCapture]
@callXML NVARCHAR (MAX), @Command NVARCHAR (MAX), @rsTable1 NVARCHAR (MAX), @rsColumnList1 NVARCHAR (MAX), @rsTable2 NVARCHAR (MAX), @rsColumnList2 NVARCHAR (MAX)
AS EXTERNAL NAME [genericProcCall].[StoredProcedures].[ResultSetCapture]


GO
PRINT N'Update complete.';


GO