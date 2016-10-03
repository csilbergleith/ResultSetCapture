using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;
using ResultSetCapture;
using execProcCall.Models;
using System.Security;
using System.Collections.Generic;

public partial class StoredProcedures
{
    [Microsoft.SqlServer.Server.SqlProcedure]
    public static void ResultSetCapture 
       (SqlString Command, 
        SqlString rsTable1, SqlString rsColumnList1, 
        SqlString rsTable2, SqlString rsColumnList2
       )
    {
        // Storage for the parameters: 
        CommandCall cmd = new CommandCall();

        // the command to execute
        cmd.Command = Command.ToString();

        // the tables to hold the result sets produced
        cmd.rsTable = new List<ResultTables>();

        // rsTableN where N = result set sequence number
        // rsColumnList is the list of columns to capture * = All
        if ( rsTable1.ToString() != "")
        {            
            cmd.rsTable.Add(new ResultTables { resultSetSeq = 1, tableName = rsTable1.ToString(), columnList = rsColumnList1.ToString().ToLower() });
        }
        
        if  ( rsTable2.ToString() != "" )
        {
            cmd.rsTable.Add(new ResultTables { resultSetSeq = 2, tableName = rsTable2.ToString(), columnList = rsColumnList2.ToString().ToLower() });
        }
       
        //*******************************************************************

        // Execute & Save results to a dataSet
        DataSet CommnandResults = CommandCallUtilities.ExecCommand(cmd);

        DataSet dsTargetTablesOut = CommandCallUtilities.getTargetTableMetaData(cmd);

        // Build a dataset with the output tables
        DataSet dsResultSetSchema = CommandCallUtilities.getResultSetMetaData(cmd);

        // Use the columnList choose the columns and meta data from the result set
        // and then add any columns that are missing to the target table

        // Map the columns of the result set to the columns of the output table; 
        bool result = CommandCallUtilities.mapResultsToOutputTables(CommnandResults, dsTargetTablesOut, dsResultSetSchema, cmd);

        return;
    }

}
