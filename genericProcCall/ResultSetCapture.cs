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
    public static void ResultSetCapture (SqlString callXML, SqlString Command, SqlString rsTable1, SqlString rsColumnList1)
    {
        // Storage for the parameters: 
        CommandCall cmd = new CommandCall();

        // the command to execute
        cmd.Command = Command.ToString();

        // the tables to hold the result sets produced
        cmd.rsTable = new List<ResultTables>();

        // Could have a series of cmd.rsTable.Add(...) for each result set if the rsTableN is not null
        // Add the result set table name, column list and a result set sequence number
        cmd.rsTable.Add(new ResultTables { resultSetSeq = 1, tableName = rsTable1.ToString(), columnList = rsColumnList1.ToString() });

        // the column list; null; empty or * means all; otherwise a csv list
        //***** just store it here
        //*******************************************************************

        // Execute & Save results to a dataSet
        DataSet CommnandResults = CommandCallUtilities.ExecCommand(cmd);

        // Build a dataset with the output tables
        DataSet dsTargetTables = CommandCallUtilities.getTargetTableMetaData(cmd);

        // Map the columns of the result set to the columns of the output table; 
        bool result = CommandCallUtilities.mapResultsToOutputTables(CommnandResults, dsTargetTables, cmd);

        return;
    }

}
