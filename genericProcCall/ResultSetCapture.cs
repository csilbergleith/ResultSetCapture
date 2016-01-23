using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;
using genericProcCall;
using execProcCall.Models;
using System.Security;

public partial class StoredProcedures
{
    [Microsoft.SqlServer.Server.SqlProcedure]
    public static void ResultSetCapture (SqlString callXML)
    {
        // display help instructions
        if(callXML.ToString() == "?" || callXML.ToString().ToLower() == "help" )
        {
            tSQLtCallProcUtilities.showHelp();
            return;
        }

        // Parse command instructions to a procCall object
        procCall CallRequest = tSQLtCallProcUtilities.parseXML(callXML.ToString());
        
        // Verify the proc & parameters exist against the DB; remove unmatched parameters
        Boolean procCallVerified = tSQLtCallProcUtilities.verifyCallRequest(ref CallRequest);

        if (procCallVerified)
        {
            tSQLtCallProcUtilities.LogMessage("");
            tSQLtCallProcUtilities.LogMessage("Prepare execution for: " + CallRequest.schemaName + "." + CallRequest.procName);
        }
        else
        {
            tSQLtCallProcUtilities.LogMessage("", 1);
            tSQLtCallProcUtilities.LogMessage("Could not find: " + CallRequest.schemaName + "." + CallRequest.procName + " terminating", 1);
            return;
        }

        // Build the execute statement from the proc name and parameters; 
        // Execute & Save results to a dataSet
        DataSet procCallResults = tSQLtCallProcUtilities.execProcCall(CallRequest);

        // Build a dataset with the output tables
        DataSet dsTargetTables = tSQLtCallProcUtilities.getTargetTableMetaData(CallRequest);

        // Map the columns of the result set to the columns of the output table; 
        bool result = tSQLtCallProcUtilities.mapResultsToOutputTables(procCallResults, dsTargetTables, CallRequest);

        return;
    }

}
