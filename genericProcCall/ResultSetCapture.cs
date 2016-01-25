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
        // display help instructions
        if(callXML.ToString() == "?" || callXML.ToString().ToLower() == "help" )
        {
            CommandCallUtilities.showHelp();
            return;
        }

        CommandCall cmd = new CommandCall();

        cmd.Command = Command.ToString();

        cmd.rsTable = new List<ResultTables>();

        cmd.rsTable.Add(new ResultTables { resultSetSeq = 0, tableName = rsTable1.ToString(), columnList = rsColumnList1.ToString() });
        //cmd.rsTable = rsTable1.ToString();
        //cmd.rsColList = rsColumnList1.ToString();

        CommandCallUtilities.LogMessage("Command: " + cmd.Command);            
        //CommandCallUtilities.LogMessage("table: " + cmd.rsTable);
        //CommandCallUtilities.LogMessage("Columns: " + cmd.rsColList);

        // Parse command instructions to a procCall object
        //procCall CallRequest = tSQLtCallProcUtilities.parseXML(callXML.ToString());
        
        // Verify the proc & parameters exist against the DB; remove unmatched parameters
        //Boolean procCallVerified = tSQLtCallProcUtilities.verifyCallRequest(ref CallRequest);

        //if (procCallVerified)
        //{
        //    tSQLtCallProcUtilities.LogMessage("");
        //    tSQLtCallProcUtilities.LogMessage("Prepare execution for: " + CallRequest.schemaName + "." + CallRequest.procName);
        //}
        //else
        //{
        //    tSQLtCallProcUtilities.LogMessage("", 1);
        //    tSQLtCallProcUtilities.LogMessage("Could not find: " + CallRequest.schemaName + "." + CallRequest.procName + " terminating", 1);
        //    return;
        //}

        // Build the execute statement from the proc name and parameters; 
        // Execute & Save results to a dataSet
        //DataSet procCallResults = tSQLtCallProcUtilities.execProcCall(CallRequest);

        DataSet CommnandResults = CommandCallUtilities.ExecCommand(cmd);

        // Build a dataset with the output tables
        //DataSet dsTargetTables = tSQLtCallProcUtilities.getTargetTableMetaData(CallRequest);

        // Map the columns of the result set to the columns of the output table; 
        //bool result = CommandCallUtilities.mapResultsToOutputTables(procCallResults, dsTargetTables, CallRequest);

        return;
    }

}
