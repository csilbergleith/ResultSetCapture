using System.Collections.Generic;

namespace execProcCall.Models
{
    public class procCall
    {
        public string schemaName { get; set; }
        public string procName { get; set; }
        public List<ProcParameters> procParms { get; set; }
        public List<outTables> tblOut { get; set; }

    }

    public class CommandCall
    {
        public string Command { get; set; }
        public List<ResultTables> rsTable { get; set; }

    }

    public class ProcParameters
    {
        public string parmName { get; set; }
        public string parmValue { get; set; }
        public string parmDirection { get; set; }
    }

    public class outTables
    {
        public int resultSetSeq { get; set; }
        public string tableName { get; set; }
    }

    public class ResultTables
    {
        public int resultSetSeq { get; set; }
        public string tableName { get; set; }
        public string columnList { get; set; }
    }
    public class ColumnRef
    {
        public string columnName { get; set; }
        public string dataType { get; set; }
    }
}