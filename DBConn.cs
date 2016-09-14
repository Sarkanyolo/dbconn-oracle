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

            public SP Add(string name, string value) {
                opc.Add(new OracleParameter(name, value));
                return this;
            }

            public string Run() {
                return myDB.call_sp(spname, opc);
            }
        }

        private DB() { }
    }
}