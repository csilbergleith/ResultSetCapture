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

        // Could have a series of cmd.rsTable.Add(...) for each result set if the rsTableN is not null
        // Add the result set table name, column list and a result set sequence number
        if ( rsTable1.ToString() != "")
        {
            cmd.rsTable.Add(new ResultTables { resultSetSeq = 1, tableName = rsTable1.ToString(), columnList = rsColumnList1.ToString() });
        }
        
        if  ( rsTable2.ToString() != "" )
        {
            cmd.rsTable.Add(new ResultTables { resultSetSeq = 2, tableName = rsTable2.ToString(), columnList = rsColumnList2.ToString() });
        }
       

        // the column list; null; empty or * means all; otherwise a csv list
        //***** just store it here
        //*******************************************************************

        // Execute & Save results to a dataSet
        DataSet CommnandResults = CommandCallUtilities.ExecCommand(cmd);

        // Build a dataset with the output tables
        DataSet dsTargetTables = CommandCallUtilities.getTargetTableMetaData(cmd);

        // At this point we have the result set of the command and the meta data for the 
        // target tables. 
        // Use the columnList choose the columns and meta data from the result set
        // and then add any columns that are missing to the target table

        // Map the columns of the result set to the columns of the output table; 
        bool result = CommandCallUtilities.mapResultsToOutputTables(CommnandResults, dsTargetTables, cmd);

        return;
    }

}
