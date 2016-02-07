using System.Collections.Generic;
using System.Data.SqlClient;
using Microsoft.SqlServer.Server;
using System.Data;
using execProcCall.Models;
using System.Xml;
using System;

namespace ResultSetCapture
{
    
    internal sealed class CommandCallUtilities

    {
        // Parse Call XML into ProcCall Class object
        internal static procCall parseXML(string CallProcRequest)
        {
            string myXML = CallProcRequest;
        
            // SECTION 1: Shred the xml into parameters and result tables
            //------------------------------------------------------------
            string value = "";
            string attrbName = "";
            string dir = "";
            string targetTableName = "";
            int resultSetMappingId;

            procCall procCall = new procCall();

            XmlDocument xmlDoc = new XmlDocument();

            xmlDoc.LoadXml(myXML);

            XmlNodeList xnList = xmlDoc.SelectNodes("/execute");

            XmlNode node = xnList[0];

            string procName = node.Attributes["proc"].Value;
            string schema = node.Attributes["schema"].Value;

            LogMessage("");
            LogMessage("Proc: " + schema + "." + procName);

            procCall.procName = procName;
            procCall.schemaName = schema;
            
            xnList = xmlDoc.SelectNodes("/execute/output");

            // get the list of output tables, in the event of multiple 
            // result sets. Sequence is important here as we'll
            // map the resulting dataset.tables in the order listed here
            procCall.tblOut = new List<outTables>();

            // a default value for mapping the table to the result set
            int defaultSeq = 1;

            LogMessage("");
            foreach (XmlNode xn in xnList)
            {
                // get the list of output tables
                // resultSetSeq will be the order the results are
                // stored in if multiple result sets
                targetTableName = xn.Attributes["target"].Value;

                // if a resultsetseq is given use it otherwise use sequential counter
                resultSetMappingId = xn.Attributes["resultsetseq"] == null ? defaultSeq : Convert.ToInt32(xn.Attributes["resultsetseq"].Value);

                procCall.tblOut.Add(new outTables { resultSetSeq = resultSetMappingId, tableName = targetTableName });
                LogMessage("Output TableName: " + xn.Attributes["target"].Value);
                defaultSeq++;
            }

            xnList = xmlDoc.SelectNodes("/execute/parm");

            List<ProcParameters> parms = new List<ProcParameters>();
            procCall.procParms = new List<ProcParameters>();

            LogMessage("");
            // get the parameter list <parm name value direction />
            if(xnList != null)
            {   
                foreach (XmlNode xn in xnList)
                {
                    attrbName = xn.Attributes["name"].Value;
                    value = xn.Attributes["value"].Value;
                    dir = xn.Attributes["direction"] == null ? "input" : xn.Attributes["direction"].Value;
                    LogMessage("Attribute: " + attrbName + " = " + value);
                    // save the list of parameters and their values
                    parms.Add(new ProcParameters { parmName = attrbName, parmValue = value, parmDirection = dir });
                    procCall.procParms.Add(new ProcParameters { parmName = attrbName, parmValue = value, parmDirection = dir });
                }
            }

            return procCall;
        }
        
        // (obsolete) Verify the existance of the proc and parameters against the catalog using stored proc
        internal static bool verifyCallRequest(ref procCall CallRequest)
        {
            //string connectionString = getConnectionString();
            //int retCode;

            // using proc calls: verify the proc exists and get the paramter list
            // parameters that don't exist in the dd proc definition are removed
            // from CallRequest
            //using (SqlConnection conn = new SqlConnection(connectionString))
            //{
            //    conn.Open();

            //    // Setup to call admin.GetProcParameters_sp
            //    // which will verify if the proc exists and return the list of parameters
            //    SqlDataAdapter dbProc = new SqlDataAdapter("admin.GetProcParameters_sp", conn);

            //    dbProc.SelectCommand.CommandType = CommandType.StoredProcedure;
            //    dbProc.SelectCommand.Parameters.AddWithValue("@schema", CallRequest.schemaName);
            //    dbProc.SelectCommand.Parameters.AddWithValue("@procName", CallRequest.procName);
            //    dbProc.SelectCommand.Parameters.Add("@RETURN_VALUE", SqlDbType.Int).Direction = ParameterDirection.ReturnValue;
            //    DataSet dsParameters = new DataSet();

            //    retCode = dbProc.Fill(dsParameters);

            //    // retCode = 0 if the proc does not exist
            //    retCode = Convert.ToInt32(dbProc.SelectCommand.Parameters["@RETURN_VALUE"].Value.ToString());

            //    // If the proc doesn't exist, return false
            //    if (retCode == 0)
            //    {
            //        return false;
            //    }

            //    // list of the proc's parameters as indicated int the DB
            //    List<string> dbParmName = new List<string>(20);

            //    // for each parameter, get the name to lower case
            //    foreach (DataRow r in dsParameters.Tables[0].Rows)
            //    {
            //        dbParmName.Add(r["parameterName"].ToString().ToLower());
            //    }

            //    // Get a list of the parameters in the request that don't 
            //    // have the parameter as part of the proc parameters
            //    int matchIdx;
            //    List<ProcParameters> parmsNotMatched = new List<ProcParameters>();

            //    LogMessage("");
            //    foreach (ProcParameters p in CallRequest.procParms)
            //    {
            //        // we're really looking for the parameter that doesn't match
            //        matchIdx = dbParmName.FindIndex(m => m.Equals(p.parmName.ToLower()));

            //        // Save the list of user provided parameters that don't exist in the DB
            //        if (matchIdx == -1)
            //        {
            //            parmsNotMatched.Add(p);
            //            LogMessage("Supplied parameter not used by procedure: " + p.parmName);
            //        }
            //    }

            //    // remove the parameters that don't match the proc parameters on the DB
            //    foreach (ProcParameters p in parmsNotMatched)
            //    {
            //        CallRequest.procParms.RemoveAll(m => m.parmName == p.parmName);
            //    }
            //}

            return true;
        }
        
        // Execute the requested call 
        internal static DataSet execProcCall(procCall CallRequest)
        {
            DataSet dsResults = new DataSet();
            string connectionString = getConnectionString();

            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();

                SqlDataAdapter daExecProcCall = new SqlDataAdapter(CallRequest.schemaName + "." + CallRequest.procName, conn);
                daExecProcCall.SelectCommand.CommandType = CommandType.StoredProcedure;

                // Add parameters to the command
                foreach(ProcParameters p in CallRequest.procParms)
                {
                    daExecProcCall.SelectCommand.Parameters.Add(new SqlParameter(p.parmName, p.parmValue));
                }

                daExecProcCall.Fill(dsResults);

                conn.Close();
            }

            return dsResults;
        }

        // Execute the command, Return a DataSet Object 
        internal static DataSet ExecCommand(CommandCall cmd)
        {
            DataSet dsResults = new DataSet();
            string connectionString = getConnectionString();

            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();

                SqlDataAdapter daExecCommand = new SqlDataAdapter(cmd.Command, conn);
                daExecCommand.SelectCommand.CommandType = CommandType.Text;

                int rc = daExecCommand.Fill(dsResults);

                LogMessage("ExecCommand result = " + rc.ToString());

                conn.Close();
            }

            return dsResults;
        }
        
        // Returns a DataSet with meta data of the target tables
        internal static DataSet getTargetTableMetaData(CommandCall CallRequest)
        {
            DataSet dsTargets = new DataSet();
            DataTable dtTarget = new DataTable();
            string queryStr;
            string connectionString = getConnectionString();

            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                
                foreach (ResultTables tgt in CallRequest.rsTable)
                {                    
                    // ** Should be a Try Catch block here ************
                    conn.Open();

                    queryStr = "SELECT * FROM " + tgt.tableName + " WHERE 1 = 0";
                    SqlDataAdapter daTgtTable = new SqlDataAdapter(queryStr, conn);
                    daTgtTable.SelectCommand.CommandType = CommandType.Text;
                    daTgtTable.Fill(dsTargets, tgt.tableName);

                    conn.Close();                    
                }

            }

            return dsTargets;
        }

        // map matching column names between the results table and the target table
        internal static bool mapResultsToOutputTables(DataSet CommnandResults, DataSet dsTargetTables, CommandCall CallRequest)
        {
            // loop through result set tables; find matching columns and write out the results
            DataTable dtResultsTable = new DataTable();
            DataTable dtOuptutTable = new DataTable();
            List<string> matchedColumns = new List<string>();
            List<ColumnRef> crSelectedCoulmns = GetTargetColumns("");
            string sqlInsert = "";
            string sqlValues = "";
            string sqlCommandText = "";

            int dtIdx = 0;
            // Check how many we have of each; not enough output tables means nothing written to it
            int resultTableCount = CommnandResults.Tables.Count;
            int targetTableCount = dsTargetTables.Tables.Count;

            // no results, we're done
            if (resultTableCount == 0) return true;

            // no target tables, we're done
            if (targetTableCount == 0) return true;

            // resultToTargetMap let's us map a specific result set to a target table
            int resultToTargetMap;
            string tblName;
            string csvColList;

            // as long as we have another result set and target table, keep looping
            while(dtIdx <= targetTableCount -1 )
            {
                // get the output table name and find it in the target table list; get the resultSetSeq number
                tblName = dsTargetTables.Tables[dtIdx].TableName;

                resultToTargetMap = CallRequest.rsTable.Find(m => m.tableName == tblName).resultSetSeq -1;

                csvColList = CallRequest.rsTable.Find(m => m.tableName == tblName).columnList;

                // call function to get a list of the matching columns if a mapping exists
                if(resultToTargetMap > -1)
                {
                    matchedColumns = FindMatchingColumns(CommnandResults.Tables[resultToTargetMap], dsTargetTables.Tables[dtIdx], csvColList);
                }
                else
                {
                    matchedColumns.Clear();
                }
                
                // use the matching column list to build and execute an insert; loop through all rows
                if (matchedColumns.Count > 0)
                {
                    // We have matching columns:
                    // Build the insert statement with the column names from the matched columns
                    sqlInsert = "INSERT " + dsTargetTables.Tables[dtIdx].TableName + " (";

                    // Add the columns to be inserted
                    foreach(string m in matchedColumns)
                    {
                        sqlInsert += m + ", ";
                    }

                    // Remove the trailing comma and space(do we need to remove the space?)
                    sqlInsert = sqlInsert.TrimEnd(',', ' ');

                    // add the closing parenthesis
                    sqlInsert += ") ";

                    // get the connection string
                    string connectionString = getConnectionString();

                    LogMessage(""); // create a line break

                    // for each row in the result set, build the VALUES clause and execut the INSERT
                    using (SqlConnection conn = new SqlConnection(connectionString))
                    {
                        conn.Open();
                        SqlCommand tsqlWrite = new SqlCommand("", conn);
                        int rowCount;
                        bool mRow;

                        foreach (DataRow r in CommnandResults.Tables[resultToTargetMap].Rows)
                        {
                            // Add the VALUES clause
                            sqlValues = " VALUES ( ";
                            
                            // get the data from the columns
                            foreach (string m in matchedColumns)
                            {
                                //quoteName = true; //quote every thing 

                                mRow = DBNull.Value.Equals(r[m]);

                                if (DBNull.Value.Equals(r[m]))
                                {
                                    sqlValues +=  "NULL, ";
                                }
                                else
                                {
                                    sqlValues += "'" + r[m] + "', ";
                                }
                            }

                            //strip out the remnants
                            sqlValues = sqlValues.TrimEnd(',', ' ');

                            // add the closing parenthesis
                            sqlValues += ") ";

                            // Form the full INSERT statement
                            sqlCommandText = sqlInsert + sqlValues;

                            LogMessage("Command: " + sqlInsert + sqlValues);

                            // Prepare command
                            tsqlWrite.CommandText = sqlCommandText;
                            tsqlWrite.CommandType = CommandType.Text;

                            // Execute the insert
                            rowCount = tsqlWrite.ExecuteNonQuery();
                        }
                    }
                }
                dtIdx++;
            }

            return true;
        }

        // find that column names that are the same between the target table and the result sets table
        private static List<string> FindMatchingColumns(DataTable resultSet, DataTable targetTable, string csvColList)
        {
            List<string> resultSetColumnNames = new List<string>(100);
            List<string> targetTableColumnNames = new List<string>(100);
            List<string> matchingColumnNames = new List<string>(100);
            List<string> unMatchedColumnNames = new List<string>(100);

            List<string> rsColList = new List<string>(csvColList.Split(','));

            // get the column names for the resultSet
            foreach(DataColumn c in resultSet.Columns)
            {
                resultSetColumnNames.Add(c.ColumnName);   
                // if the column name is in the list columns, capture the meta data
            }

            // get the column names for the target table
            foreach (DataColumn col in targetTable.Columns)
            {
                targetTableColumnNames.Add(col.ColumnName);
            }
            
            // see if any columns are missing from the target table, based on column list
            // exec ALTER <tablename> ADD COLUMN for each missing column based on 

            // find the names that are in both lists
            foreach(string columnName in resultSetColumnNames)
            {
                // matchingColumnNames.Add(targetTableColumnNames.Find(m => m.ToLower() == columnName.ToLower()));
                matchingColumnNames.Add(targetTableColumnNames.Find(m => m.ToLower() == columnName.ToLower()));;
            }

            matchingColumnNames.RemoveAll(item => item == null);

            foreach(string columnName in matchingColumnNames)
            {
                targetTableColumnNames.Remove(columnName);
            }

            LogMessage("");

            foreach(string columnName in targetTableColumnNames)
            {
                LogMessage("Target table [" + targetTable.TableName + "] column not in result set: " + columnName);
            }

            return matchingColumnNames;
        }
        
        // return a connection string when asked for one (cetralized loaction for credetntials)
        internal static string getConnectionString()
        {
            return "context connection=true";
        }

        // Show Help - Obsolete
        internal static void showHelp()
        {
            LogMessage("XML Input Format", 1);
            LogMessage("<execute schema=\"dbo\" proc=\"myProc\" >", 1);
            LogMessage("<parm name=\"@myfirstParm\" value=\"someValue\" />", 1);
            LogMessage("... [n] repetitions");
            LogMessage("<parm name=\"@mylastParm\" value=\"someValue\" />", 1);
            LogMessage("<output target=\"#Actual1\" resultsetseq=\"1\" />", 1);
            LogMessage("... [n] repetitions", 1);
            LogMessage("<output target=\"#ActualN\" />", 1);
            LogMessage("</execute>", 1);
            LogMessage("NOTE:", 1);
            LogMessage("'resultsetseq' is optional; output target tables should be listed", 1);
            LogMessage("in the same order as the result sets", 1);
            LogMessage("resultsetseq' maps a specific result set to a specific table", 1);


        }

        // Centralize logic for writing messages
        // need to make this a single global objects
        internal static void LogMessage(string msg, int force = 1)
        {
            if (force == 1)
            {
                SqlContext.Pipe.Send(msg);
            }
           
        }

        // split the csv list of columns to capture and retur
        internal static List<ColumnRef> GetTargetColumns (string csvColList)
        {
            string [] ColList = new string [20] ;
            List<ColumnRef> rsColRef = new List<ColumnRef>();
            
            ColList = csvColList.Split(',');

            foreach (string ColName in ColList)
            {
                rsColRef.Add(new ColumnRef { columnName = ColName });
            }

            return rsColRef;

        }

    }
 
}
