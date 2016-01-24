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
        public string rsTable { get; set; }
        public string rsColList { get; set; }
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

    public class columnReferences
    {
        public string databaseName { get; set; }
        public string schema { get; set; }
        public string tableName { get; set; }
        public string alias { get; set; }
        public string columnName { get; set; }
    }
}