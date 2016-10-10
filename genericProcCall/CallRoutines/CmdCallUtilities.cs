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

        // Execute the SQL command, Return a DataSet with the result set(s)
        internal static DataSet getResultSetDataSet(CommandCall cmd)
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
        
        // Returns dataset with meta data of the target tables
        internal static DataSet getTargetTableMetaData(CommandCall cmd)
        {
            DataSet dsTargets = new DataSet();
            DataTable dtTarget = new DataTable();
            string queryStr;
            string connectionString = getConnectionString();

            using (SqlConnection conn = new SqlConnection(connectionString))
            {

                foreach (ResultTables tgt in cmd.rsTable)
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

        // Returns a DataSet with meta data of the result sets
        internal static DataSet getResultSetMetaData(CommandCall CallRequest)
        {
            DataSet dsResultSetSchema = new DataSet();
            DataTable schemaTable = new DataTable();
            DataTable dtResultSet = new DataTable();
            string connectionString = getConnectionString();

            // Get Schema Table for the result sets
            SqlDataReader SqlDr;
            SqlCommand SqlCmd;

            int brkCount;
            int tblCount = 0;
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();

                SqlCmd = conn.CreateCommand();
                SqlCmd.CommandText = CallRequest.Command; 

                SqlDr = SqlCmd.ExecuteReader();
                SqlDr.Read();
                tblCount = 0;

                // for each result table in SqlDataReader:
                while (SqlDr.HasRows)
                {
                    schemaTable = SqlDr.GetSchemaTable();
                    schemaTable.TableName = CallRequest.rsTable[tblCount].tableName;
                    dsResultSetSchema.Tables.Add(schemaTable);
                    
                    //dtResultSet.Load(SqlDr);
                    //dtResultSet.TableName = "dtResult" + tblCount.ToString();

                    SqlDr.NextResult();
                    tblCount += 1;
                    dtResultSet.Clear();
                }

                conn.Close();
            }

            return dsResultSetSchema;
        }

        // map matching column names between the results table and the target table
        internal static bool mapResultsToOutputTables(DataSet dsResultSetData, DataSet dsTargetTablesMetaData, DataSet dsResultSetSchema, CommandCall CallRequest)
        {
            // loop through result set tables; find matching columns and write out the results
            DataTable dtResultsTable = new DataTable();
            DataTable dtOuptutTable = new DataTable();
            List<string> matchedColumns = new List<string>();
            List<ColumnRef> crSelectedCoulmns = GetTargetColumns("");
            string sqlInsert = "";
            var sqlValues = "";
            string sqlCommandText = "";

            int dtIdx = 0;
            // Check how many we have of each; not enough output tables means nothing written to it
            int resultTableCount = dsResultSetData.Tables.Count;
            int targetTableCount = dsResultSetSchema.Tables.Count;

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
                tblName = dsResultSetSchema.Tables[dtIdx].TableName;

                resultToTargetMap = CallRequest.rsTable.Find(m => m.tableName == tblName).resultSetSeq -1;

                csvColList = CallRequest.rsTable.Find(m => m.tableName == tblName).columnList;

                // call function to get a list of the matching columns if a mapping exists
                if(resultToTargetMap > -1)
                {
                    matchedColumns = FindMatchingColumns(dsResultSetData.Tables[resultToTargetMap], dsTargetTablesMetaData.Tables[dtIdx], dsResultSetSchema.Tables[dtIdx], csvColList);
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
                    sqlInsert = "INSERT " + dsResultSetSchema.Tables[dtIdx].TableName + " (";


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

                    // for each row in the result set, build the VALUES clause and execute the INSERT
                    using (SqlConnection conn = new SqlConnection(connectionString))
                    {
                        conn.Open();
                        SqlCommand tsqlWrite = new SqlCommand("", conn);
                        int rowCount;
                        bool mRow;
                        System.Type dc;
                        string colValue;
                        byte[] ba;

                        foreach (DataRow r in dsResultSetData.Tables[resultToTargetMap].Rows)
                        {
                            // Add the VALUES clause
                            sqlValues = " VALUES ( ";
                            
                            // get the data from the columns
                            foreach (string m in matchedColumns)
                            {
                                //quoteName = true; //quote every thing 

                                mRow = DBNull.Value.Equals(r[m]);
                                dc = r[m].GetType();
                                
                                switch (dc.Name.ToLower())
                                {
                                    case "byte[]":
                                        ba = (byte[])r[m];
                                        colValue = BitConverter.ToString(ba);
                                        colValue = "0x" + colValue.Replace("-","") + ",";
                                        break;
                                    case "boolean":
                                        if(r[m].ToString() == "True")
                                        {
                                            colValue = "1,";
                                        }
                                        else
                                        {
                                            colValue = "0,";
                                        }
                                        break;
                                    default:
                                        colValue = "N'" + r[m].ToString().Replace("'","''") + "', ";
                                        break;
                                }

                                if (DBNull.Value.Equals(r[m]))
                                {
                                    sqlValues +=  "NULL, ";
                                }
                                else
                                {
                                    sqlValues += colValue;
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
        // filter using the @rsColumnList
        private static List<string> FindMatchingColumns(DataTable resultSet, DataTable dtTargetTable, DataTable resultSetSchema, string csvColList)
        {
            List<string> resultSetColumnNames = new List<string>(100);
            List<string> targetTableColumnNames = new List<string>(100);
            List<string> matchingColumnNames = new List<string>(100);
            List<string> unMatchedColumnNames = new List<string>(100);

            // The list of columns to capture
            List<string> rsColList = new List<string>(csvColList.Split(','));
            List<ColumnRef> CaptureColumns = new List<ColumnRef>(100);
            
            // SQL dataTypes that use a width size
            List<string> sizedDataTypes = new List<string>(100);
                sizedDataTypes.Add("varchar"); 
                sizedDataTypes.Add("nvarchar");
                sizedDataTypes.Add("char");
                sizedDataTypes.Add("nchar");
                sizedDataTypes.Add("binary");
                sizedDataTypes.Add("varbinary");
            // SQL dataTypes that use precision and scale
            List<string> decimalDataTypes = new List<string>(100);
                decimalDataTypes.Add("decimal"); 
                decimalDataTypes.Add("numeric");
            // variables for building the SQL schema
            string dataTypeDef;
            string tableName;
            string colName;
            string precision;
            string scale;
            string colSize;

            string CaptureColumnMatch;
            string Found;
            SqlCommand sqlCmd;

            // get the column names for the resultSet and check if they're in the select list
            // if the selected column list is * get all the results set columns
            // otherwise only get the ones that matched the selected list
            foreach(DataRow r in resultSetSchema.Rows)
            {
                tableName = r["BaseTableName"].ToString();
                //colName = r["ColumnName"].ToString().ToLower();
                colName = r["ColumnName"].ToString();
                dataTypeDef = r["DataTypeName"].ToString().ToLower();
                scale = r["NumericScale"].ToString();
                precision = r["NumericPrecision"].ToString();
                colSize = r["ColumnSize"].ToString();

                resultSetColumnNames.Add(r["ColumnName"].ToString());
                // if the result set column name is in the rsColList capture columns
                // save the meta data
                CaptureColumnMatch = rsColList.Find(m => m.ToLower() == colName.ToLower());
                if ( (! string.IsNullOrEmpty(CaptureColumnMatch) ) || rsColList[0] == "*" )
                {
                    // Build the datatype: e.g.: nvarchar(250) or decimal(10,3)
                    if (sizedDataTypes.IndexOf(dataTypeDef) > -1)
                        dataTypeDef += " (" + colSize + ")";
                    else
                    if (decimalDataTypes.IndexOf(dataTypeDef) > -1)
                        dataTypeDef += " (" + precision + "," + scale + ")";
                
                    CaptureColumns.Add(new ColumnRef { columnName = colName, dataType = dataTypeDef});  
                }
            }

            // get the column names for the target table
            foreach (DataColumn c in dtTargetTable.Columns) 
            {
                colName = c.ColumnName.ToLower();
                targetTableColumnNames.Add(colName);
            }
          
            // see if any columns are missing from the target table, based on column list
            // exec ALTER <tablename> ADD COLUMN for each missing column based on 
            foreach (ColumnRef cr in CaptureColumns)
            {
                // if the column is not found add it to the table
                Found = targetTableColumnNames.Find(m => m.ToLower() == cr.columnName.ToLower());
                if(string.IsNullOrEmpty(Found))
                {
                    LogMessage("Column not in target table: " + resultSetSchema.TableName + ". Column: " + cr.columnName + " Datatype: " + cr.dataType, 0 );
                    //resultSetSchema.Columns.Add(cr.columnName, cr.dataType);    // **** Redundant? **** maybe; might need for alternatives to SQL INSERT Statements
                    targetTableColumnNames.Add(cr.columnName); // It wasn't there so add it
                
                    using (var sqlConn = new SqlConnection(getConnectionString()))
                    {                        
                        LogMessage("Column datatype: " + cr.dataType + " ColumnName: [" + cr.columnName + "] mapping to: " + cr.dataType);
                        sqlCmd = sqlConn.CreateCommand();
                        sqlCmd.CommandText = String.Format("ALTER TABLE {0} ADD {1} {2} ",  resultSetSchema.TableName, cr.columnName, cr.dataType);
                        sqlConn.Open();
                        sqlCmd.ExecuteNonQuery();
                    }
                }
            }

            //*****************************************************
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
                LogMessage("Target table [" + resultSetSchema.TableName + "] column not in result set: " + columnName);
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
