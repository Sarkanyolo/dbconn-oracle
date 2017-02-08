namespace DBConn {
    using System;
    using System.Data;
    using System.Data.OracleClient;

    public sealed class DB {
        private sealed class _DB {
            private OracleConnection OConn;

            public _DB() {
                OConn = new OracleConnection();
                DBConnection();
            }

            private void DBConnection() {
                const string CONN = "Data Source={0}{1};Persist Security Info=True;User ID={2};Password={3};Unicode=True";
		const string USER = "user";
                const string PASSWORD = "pass";
		const string DBNAME = "odb";
		const string ORGANIZATION = ".organization.gd"
                try {
                    if (OConn.State != ConnectionState.Open) {
                        OConn.ConnectionString = String.Format(CONN, DBNAME, String.Empty, USER, PASSWORD);
                        OConn.Open();
                    }
                } catch (OracleException ex) {
                    if (OConn.State == ConnectionState.Open) {
                        throw new Exception(ex.Message);
                    }
                    OConn.ConnectionString = String.Format(CONN, DBNAME, ORGANIZATION, USER, PASSWORD);
                    OConn.Open();
                }
            }

            public string call_sp(string sp, OracleParameterCollection ParamList) {
                using (var cmd_sp = new OracleCommand()) {
                    if (OConn.State != ConnectionState.Open) {
                        DBConnection();
                    }
                    cmd_sp.Parameters.Clear();
                    for (int i = 0; i < ParamList.Count; i++) {
                        cmd_sp.Parameters.Add(ParamList[i].ParameterName, ParamList[i].OracleType, ParamList[i].Size).Direction = ParamList[i].Direction;
                        cmd_sp.Parameters[i].Value = ParamList[i].Value;
                    }
                    cmd_sp.Connection = OConn;
                    cmd_sp.CommandType = CommandType.StoredProcedure;
                    cmd_sp.CommandText = sp;
                    cmd_sp.ExecuteNonQuery();
                    return cmd_sp.Parameters["var_o_message"].Value.ToString();
                }
            }

            public DataTable GetDataTable(string sql) {
                using (var cmd_text = new OracleCommand()) {
                    if (OConn.State != ConnectionState.Open) {
                        DBConnection();
                    }
                    cmd_text.Connection = OConn;
                    cmd_text.CommandType = CommandType.Text;
                    cmd_text.CommandText = sql;
                    var dt = new DataTable();
                    using (OracleDataReader dr = cmd_text.ExecuteReader()) {
                        dt.Load(dr);
                    }
                    return dt;
                }
            }

            public void Execute(string sql) {
                using (var cmd_text = new OracleCommand()) {
                    if (OConn.State != ConnectionState.Open) {
                        DBConnection();
                    }
                    cmd_text.Connection = OConn;
                    cmd_text.CommandType = CommandType.Text;
                    cmd_text.CommandText = sql;
                    cmd_text.ExecuteNonQuery();
                }
            }
        }

        private static _DB myDB = new _DB();

        public static DataTable GetDataTable(string sql) {
            return myDB.GetDataTable(sql);
        }

        public static string GetResultAsStr(string sql) {
            using (var dt = myDB.GetDataTable(sql)) {
                if (dt.Rows.Count == 0 || dt.Rows[0][0] == DBNull.Value) {
                    return null;
                }
                return dt.Rows[0][0].ToString();
            }
        }

        public static int? GetResultAsInt(string sql) {
            using (var dt = myDB.GetDataTable(sql)) {
                if (dt.Rows.Count == 0 || dt.Rows[0][0] == DBNull.Value) {
                    return null;
                }
                return int.Parse(dt.Rows[0][0].ToString());
            }
        }

        public static string[] GetFirstRow(string sql) {
            using (var dt = myDB.GetDataTable(sql)) {
                var t = new System.Collections.Generic.List<string>();
                foreach (var i in dt.Rows[0].ItemArray) {
                    if (i is DBNull) {
                        t.Add(null);
                    } else {
                        t.Add(i.ToString());
                    }
                }
                return t.ToArray();
            }
        }

        public static string GetResultAsConcatedStr(string sql, string separator = " ") {
            using (var dt = myDB.GetDataTable(sql)) {
                var t = new System.Text.StringBuilder();
                for (int i = 0; i < dt.Rows.Count; i++) {
                    t.Append(dt.Rows[i][0].ToString());
                    t.Append(separator);
                }
                return t.ToString();
            }
        }

        public static string[] getFirstColumn(string sql) {
            using (var dt = myDB.GetDataTable(sql)) {
                string[] t = new string[dt.Rows.Count];
                for (int i = 0; i < dt.Rows.Count; i++) {
                    if (dt.Rows[i][0] == DBNull.Value) {
                        t[i] = null;
                    } else {
                        t[i] = dt.Rows[i][0].ToString();
                    }
                }
                return t;
            }
        }

        public static System.Collections.Generic.Dictionary<string, string> getDictionary(string distinctSQLWithTwoColumns) {
            using (var dt = myDB.GetDataTable(distinctSQLWithTwoColumns)) {
                var dict = new System.Collections.Generic.Dictionary<string, string>();
                for(int i = 0; i < dt.Rows.Count; i++) {
                    if(dt.Rows[i][0] == DBNull.Value || dt.Rows[i][1] == DBNull.Value) {
                        try {
                            dict.Add(dt.Rows[i][0].ToString(), dt.Rows[i][1].ToString());
                        } catch {
                            // Elem cannot be inserted
                        }
                    }
                }
                return dict;
            }
        }

        public static System.Collections.Generic.Dictionary<string, string> getDictionary(string table, string keyColumn, string valueColumn) {
            using (var dt = myDB.GetDataTable(string.Format("SELECT DISTINCT {0}, {1} FROM {2} WHERE {0} IS NOT NULL AND {1} IS NOT NULL", keyColumn, valueColumn, table))) {
                var dict = new System.Collections.Generic.Dictionary<string, string>();
                for (int i = 0; i < dt.Rows.Count; i++) {
                    try {
                        dict.Add(dt.Rows[i][0].ToString(), dt.Rows[i][1].ToString());
                    } catch {
                        // Elem cannot be inserted
                    }
                }
                return dict;
            }
        }

        public static void Execute(string sql) {
            myDB.Execute(sql);
        }

        public static string GetString(object value) {
            if (value is DBNull) { return null; }
            return value.ToString();
        }

        public static int? GetInt(object value) {
            if (value is DBNull) { return null; }
            return int.Parse(value.ToString());
        }

        public static int getNextID(string table, string column, UInt16 startfrom = 1) {
            using (var dt = myDB.GetDataTable(string.Format("SELECT {1} FROM {0} ORDER BY {1}", table, column))) {
                for (int i = 0; i < dt.Rows.Count; i++) {
                    if (i + startfrom < int.Parse(dt.Rows[i][0].ToString())) { return i + startfrom; }
                }
                return dt.Rows.Count + startfrom;
            }
        }

        public static bool Exists(string table, string clause) {
            return GetResultAsInt(string.Format("SELECT COUNT(*) FROM {0} WHERE {1}",table,clause)) > 0;
        }

        /// <summary>
        /// If a table cotains a specific data update it, if it doesn't insert
        /// </summary>
        /// <param name="table">The table's name</param>
        /// <param name="Values">List of columns and values we want to update or insert</param>
        /// <param name="OnClause">Conditions to decide if the table contains the data(from the values list)</param>
        public static void Merge(string table, Dictionary<string, string> Values, HashSet<string> OnClause, HashSet<string> NeedToUpdate = null) {
            string mergesql = "MERGE INTO {0} a USING (SELECT {1} FROM DUAL) b ON ({2}) {3} WHEN NOT MATCHED THEN INSERT ({4}) VALUES ({5})";
            string s = string.Empty, s1 = string.Empty, s2 = string.Empty, s3 = string.Empty, s4 = string.Empty;
            try {
                if (Values == null || Values.Count == 0) {
                    throw new Exception("Values cannot be null or empty!");
                }
                if (Values.Where(v => v.Key.Trim() == string.Empty || v.Value.Trim() == string.Empty).Count() > 0) {
                    throw new Exception("Values cannot contain empty string!");
                }
                if (OnClause == null || OnClause.Count == 0) {
                    throw new Exception("OnClause cannot be null or empty!");
                }
                if ((OnClause.Where(o => o.Trim() == string.Empty).Count() > 0)) {
                    throw new Exception("OnClause cannot contain empty string!");
                }
                if (Values.Where(v => OnClause.Contains(v.Key)).Count() != OnClause.Count()) {
                    throw new IndexOutOfRangeException("Onclause contains item not existing in Values!");
                }
                if (NeedToUpdate != null) {
                    if (Values.Where(v => NeedToUpdate.Contains(v.Key)).Count() != NeedToUpdate.Count()) {
                        throw new IndexOutOfRangeException("NeedToUpdate contains item not existing in Values!");
                    } else {
                        if ((NeedToUpdate.Where(o => o.Trim() == string.Empty).Count() > 0)) {
                            throw new Exception("NeedToUpdate cannot contain empty string!");
                        }
                    }
                }
                foreach (var t in Values) {
                    if (t.Value.ToUpper() == "SYSDATE") {
                        s += string.Format("SYSDATE as {0},", t.Key);
                    } else {
                        s += string.Format("'{0}' as {1},", t.Value, t.Key);
                    }
                    if (OnClause.Contains(t.Key)) s1 += string.Format("a.{0} = b.{0} AND ", t.Key);
                    if (!OnClause.Contains(t.Key)) {
                        if (NeedToUpdate == null) {
                            s2 += string.Format("a.{0} = b.{0},", t.Key);
                        } else if (NeedToUpdate.Contains(t.Key)) {
                            s2 += string.Format("a.{0} = b.{0},", t.Key);
                        }
                    }
                    s3 += t.Key + ",";
                    s4 += string.Format("b.{0},", t.Key);
                }
                s = s.Substring(0, s.Length - 1);
                s1 = s1.Substring(0, s1.LastIndexOf("AND"));
                if (!string.IsNullOrEmpty(s2)) s2 = "WHEN MATCHED THEN UPDATE SET" + s2.Substring(0, s2.Length - 1);
                if (!string.IsNullOrEmpty(s3)) s3 = s3.Substring(0, s3.Length - 1);
                if (!string.IsNullOrEmpty(s4)) s4 = s4.Substring(0, s4.Length - 1);

                mergesql = string.Format(mergesql, table, s, s1, s2, s3, s4);
                Execute(mergesql);
            } catch (Exception) {
                return;
            }
        }

        public sealed class SP {
            private OracleParameterCollection opc;
            private OracleParameter output;
            private string spname;
            public SP(string name, string outname = "var_o_message", int outlength = 200) {
                spname = name;
                opc = new OracleParameterCollection();
                output = new OracleParameter(outname, OracleType.VarChar, outlength);
                output.Direction = ParameterDirection.Output;
                opc.Add(output);
            }

            public SP Add(string name, string value = "") {
                opc.Add(new OracleParameter(name, value));
                return this;
            }

            public string Run() {
                return myDB.call_sp(spname, opc);
            }
        }

        public static DateTime Sysdate { get { return DateTime.ParseExact(GetResultAsStr("SELECT to_char(sysdate,'yyyy/MM/dd HH24:MI:SS') FROM dual"), "yyyy/MM/dd HH:mm:ss", System.Globalization.CultureInfo.CreateSpecificCulture("en-US")); } }

        private DB() { }
    }
}
