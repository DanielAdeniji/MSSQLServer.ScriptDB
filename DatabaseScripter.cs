/*
 * Copyright 2006 Jesse Hersch
 *
 * Permission to use, copy, modify, and distribute this software
 * and its documentation for any purpose is hereby granted without fee,
 * provided that the above copyright notice appears in all copies and that
 * both that copyright notice and this permission notice appear in
 * supporting documentation, and that the name of Jesse Hersch or
 * Elsasoft LLC not be used in advertising or publicity
 * pertaining to distribution of the software without specific, written
 * prior permission.  Jesse Hersch and Elsasoft LLC make no
 * representations about the suitability of this software for any
 * purpose.  It is provided "as is" without express or implied warranty.
 *
 * Jesse Hersch and Elsasoft LLC disclaim all warranties with
 * regard to this software, including all implied warranties of
 * merchantability and fitness, in no event shall Jesse Hersch or
 * Elsasoft LLC be liable for any special, indirect or
 * consequential damages or any damages whatsoever resulting from loss of
 * use, data or profits, whether in an action of contract, negligence or
 * other tortious action, arising out of or in connection with the use or
 * performance of this software.
 *
 * Author:
 *  Jesse Hersch
 *  Elsasoft LLC
 * 
*/

using System;
using System.Collections.Generic;
using System.Text;
using System.Collections.Specialized;
using System.Data.SqlClient;
using System.IO;
using System.Diagnostics;

using Microsoft.SqlServer.Management.Smo;
using Microsoft.SqlServer.Management.Common;

using System.Text.RegularExpressions;

namespace Elsasoft.ScriptDb
{
    public class DatabaseScripter
    {

        private const string createSprocStub = @"
IF NOT EXISTS (SELECT * FROM sysobjects WHERE id = OBJECT_ID(N'[{0}].[{1}]') AND type in (N'P', N'PC'))
EXEC sp_executesql N'CREATE PROCEDURE [{0}].[{1}] AS SELECT ''this is a stub.  replace me with real code please.'''
GO
";

        #region Private Variables

        private string[] _TableFilter = new string[0];
        private string[] _RulesFilter = new string[0];
        private string[] _DefaultsFilter = new string[0];
        private string[] _UddtsFilter = new string[0];
        private string[] _UdfsFilter = new string[0];
        private string[] _ViewsFilter = new string[0];
        private string[] _SprocsFilter = new string[0];
        private string[] _UdtsFilter = new string[0];
        private string[] _SchemasFilter = new string[0];
        private string[] _TableTriggerFilter = new string[0];		
        private string[] _DdlTriggersFilter = new string[0];

        private bool _TableOneFile = false;
        private bool _ScriptAsCreate = false;
        private bool _Permissions = false;
        private bool _NoCollation = false;
        private bool _IncludeDatabase;
        private bool _CreateOnly = false;
        private bool _ScriptProperties = false;

        private string _OutputFileName = null;
		
		private String PROCEDURE_CREATE = "CREATE.*PROCEDURE\\s";
		private String PROCEDURE_ALTER = "ALTER Procedure  ";
		
		private String VIEW_CREATE = "Create.*View\\s";
		private String VIEW_ALTER = "Alter View   ";
		
		//private String FUNCTION_CREATE_INCORRECT = "Create.*Function";
		private String FUNCTION_CREATE = "Create.+Function\\s";		
		private String FUNCTION_ALTER = "Alter Function  ";

		
		private String RULE_CREATE = "CREATE.*Rule\\s";
		private String RULE_ALTER = "Alter Rule  ";
		
		private String TRIGGER_CREATE = "Create.*Trigger\\s";
		private String TRIGGER_ALTER = "Alter trigger  ";

		
        #endregion

        private bool FilterExists()
        {
            return _TableFilter.Length > 0 || _RulesFilter.Length > 0 || _DefaultsFilter.Length > 0
                   || _UddtsFilter.Length > 0 || _UdfsFilter.Length > 0 || _ViewsFilter.Length > 0
                   || _SprocsFilter.Length > 0 || _UdtsFilter.Length > 0 || _SchemasFilter.Length > 0
                   || _DdlTriggersFilter.Length > 0
				   || _TableTriggerFilter.Length > 0
				   ;
        }

        /// <summary>
        /// does all the work.
        /// </summary>
        /// <param name="connStr"></param>
        /// <param name="outputDirectory"></param>
        /// <param name="verbose"></param>
        public void GenerateScripts(string connStr, string outputDirectory,
                                    bool scriptAllDatabases, bool purgeDirectory,
                                    bool scriptData, bool verbose, bool scriptProperties)
        {

            SqlConnection connection = new SqlConnection(connStr);
            ServerConnection sc = new ServerConnection(connection);
            Server s = new Server(sc);

            s.SetDefaultInitFields(typeof(StoredProcedure), "IsSystemObject", "IsEncrypted");
            s.SetDefaultInitFields(typeof(Table), "IsSystemObject");
            s.SetDefaultInitFields(typeof(View), "IsSystemObject", "IsEncrypted");
            s.SetDefaultInitFields(typeof(UserDefinedFunction), "IsSystemObject", "IsEncrypted");
            s.ConnectionContext.SqlExecutionModes = SqlExecutionModes.CaptureSql;

            if (scriptAllDatabases)
            {
                foreach (Database db in s.Databases)
                {
                    try
                    {
                        GenerateDatabaseScript(db, outputDirectory, purgeDirectory, scriptData, verbose, scriptProperties);
                    }
                    catch (Exception e)
                    {
                        if (verbose) Console.WriteLine("Exception: {0}", e.Message);
                    }
                }
            }
            else
                GenerateDatabaseScript(s.Databases[connection.Database], outputDirectory, purgeDirectory,
                    scriptData, verbose, scriptProperties);

        }

        private void GenerateDatabaseScript(Database db, string outputDirectory, bool purgeDirectory,
                           bool scriptData, bool verbose, bool scriptProperties)
        {
            this._ScriptProperties = scriptProperties;

            // Output folder
            outputDirectory = Path.Combine(outputDirectory, db.Name);
            if (Directory.Exists(outputDirectory))
            {
                if (purgeDirectory) Program.PurgeDirectory(outputDirectory, "*.sql");
            }
            else
            {
                Directory.CreateDirectory(outputDirectory);
            }

            ScriptingOptions so = new ScriptingOptions();
            so.Default = true;
            so.DriDefaults = true;
            so.DriUniqueKeys = true;
            so.Bindings = true;
            so.Permissions = _Permissions;
            so.NoCollation = _NoCollation;
            so.IncludeDatabaseContext = _IncludeDatabase;

            ScriptTables(verbose, db, so, outputDirectory, scriptData);
            ScriptDefaults(verbose, db, so, outputDirectory);
            ScriptRules(verbose, db, so, outputDirectory);
            ScriptUddts(verbose, db, so, outputDirectory);
            ScriptUdfs(verbose, db, so, outputDirectory);
            ScriptViews(verbose, db, so, outputDirectory);
            ScriptSprocs(verbose, db, so, outputDirectory);

            if (db.Version >= 9 &&
                db.CompatibilityLevel >= CompatibilityLevel.Version90)
            {
                ScriptUdts(verbose, db, so, outputDirectory);
                ScriptSchemas(verbose, db, so, outputDirectory);
                ScriptDdlTriggers(verbose, db, so, outputDirectory);
                //ScriptAssemblies(verbose, db, so, outputDirectory);
            }
        }

        #region Private Script Functions

        private void ScriptTables(bool verbose, Database db, ScriptingOptions so, string outputDirectory, bool scriptData)
        {
			
            string data = Path.Combine(outputDirectory, "Data");
            string tables = Path.Combine(outputDirectory, "Tables");
            string programmability = Path.Combine(outputDirectory, "Programmability");
            string indexes = Path.Combine(tables, "Indexes");
            string constraints = Path.Combine(tables, "Constraints");
            string foreignKeys = Path.Combine(tables, "ForeignKeys");
            string primaryKeys = Path.Combine(tables, "PrimaryKeys");
            string uniqueKeys = Path.Combine(tables, "UniqueKeys");
            string triggers = Path.Combine(programmability, "Triggers");
			
			string FileName;
			Boolean bAppendIntoFile;

            //            if (!Directory.Exists(tables)) Directory.CreateDirectory(tables);
            //            if (!Directory.Exists(programmability)) Directory.CreateDirectory(programmability);
            //            if (!Directory.Exists(indexes)) Directory.CreateDirectory(indexes);
            //            if (!Directory.Exists(constraints)) Directory.CreateDirectory(constraints);
            //            if (!Directory.Exists(foreignKeys)) Directory.CreateDirectory(foreignKeys);
            //            if (!Directory.Exists(uniqueKeys)) Directory.CreateDirectory(uniqueKeys);
            //            if (!Directory.Exists(primaryKeys)) Directory.CreateDirectory(primaryKeys);
            //            if (!Directory.Exists(triggers)) Directory.CreateDirectory(triggers);
            //            if (!Directory.Exists(data)) Directory.CreateDirectory(data);

            foreach (Table table in db.Tables)
            {
				
				
				if (_TableOneFile)
				{	
				
					FileName = tables + ".sql";
					bAppendIntoFile = true;
					
				}					
				else
				{	
				
					FileName = Path.Combine(tables, FixUpFileName(table.Schema, table.Name) + ".sql");
					bAppendIntoFile = false;
					
				}

				
                if (!table.IsSystemObject)
                {
					
                    if (
							   (FilterExists() == false)
							|| ( Array.IndexOf(_TableFilter, table.Name) >= 0) 
						)
					{	
					
					
						if (
								   (FilterExists() == false)
								|| ( Array.IndexOf(_TableFilter, table.Name) >= 0) 
						   )			
					    {

							#region Table Definition
							using (StreamWriter sw = GetStreamWriter(FileName, bAppendIntoFile))
							{
								if (verbose) Console.WriteLine("{0} Scripting {1}", db.Name, table.Name);
								if (!_CreateOnly)
								{
									so.ScriptDrops = so.IncludeIfNotExists = true;
									WriteScript(table.Script(so), sw);
								}
								so.ScriptDrops = so.IncludeIfNotExists = false;
								WriteScript(table.Script(so), sw);

								if (_ScriptProperties && table is IExtendedProperties)
								{
									ScriptProperties((IExtendedProperties)table, sw);
								}
							}

						} // Table Filter
						
                        #endregion

                        #region Triggers

						if (verbose)
						{
							if (_TableTriggerFilter != null)
							{
								Console.WriteLine("_DdlTriggersFilter is " + _TableTriggerFilter.ToString());
								Console.WriteLine("_DdlTriggersFilter Count " + _TableTriggerFilter.Length);
								Console.WriteLine("db.Triggers Count " + table.Triggers.Count);
							}			
						}
						
                        foreach (Trigger smo in table.Triggers)
                        {
							
							if 
							(
								   ( FilterExists() == false)
								|| ( Array.IndexOf(_TableTriggerFilter, smo.Name) >= 0) 
							)
							{
								
								if (!smo.IsSystemObject && !smo.IsEncrypted)
								{
									if (!_TableOneFile)
										FileName =
											Path.Combine(triggers,
															FixUpFileName
															(
																  table.Schema
																, string.Format
																	(
																	"{0}.{1}.sql"
																	, table.Name
																	, smo.Name
																	)
															)
														);
														
									using (StreamWriter sw = GetStreamWriter(FileName, _TableOneFile))
									{
										
										if (verbose) 
										{
											
											Console.WriteLine
											(
												  "{0] Scripting {1}.{2}"
												, db.Name
												, table.Name
												, smo.Name
											);
										}			
										
										if (!_CreateOnly)
										{
											so.ScriptDrops = so.IncludeIfNotExists = true;
											WriteScript(smo.Script(so), sw);
										}
										
										so.ScriptDrops = so.IncludeIfNotExists = false;
										
										
										//WriteScript(smo.Script(so), sw);
										if (verbose)
										{				
											Console.WriteLine("Keeping Trigger Create");
											Console.WriteLine("Trigger Create " + TRIGGER_CREATE);
											Console.WriteLine("Trigger Alter " + TRIGGER_ALTER);
										}
										
										if (_ScriptAsCreate)
										{
											
											
											WriteScript(smo.Script(so), sw);
											
										}
										else
										{
											
											if (verbose)
											{
												Console.WriteLine("Replacing TRigger Create with Alter");
											}			
											
											//replace original code with Regular Expression
											WriteScript(smo.Script(so), sw, TRIGGER_CREATE, TRIGGER_ALTER, verbose);
											
										}									
										

										if (_ScriptProperties && smo is IExtendedProperties)
										{
											ScriptProperties((IExtendedProperties)smo, sw);
										}
									} // using
									
								} //if (!smo.IsSystemObject && !smo.IsEncrypted)
									
							} 	// ( Array.IndexOf(_TableTriggerFilter, smo.Name) >= 0) 	
							
                        } // foreach (Trigger smo in table.Triggers)

                        #endregion

                        #region Indexes

                        foreach (Index smo in table.Indexes)
                        {
                            if (!smo.IsSystemObject)
                            {
                                string dir =
                                    (smo.IndexKeyType == IndexKeyType.DriPrimaryKey) ? primaryKeys :
                                    (smo.IndexKeyType == IndexKeyType.DriUniqueKey) ? uniqueKeys : indexes;
                                if (!_TableOneFile)
                                    FileName =
                                        Path.Combine(dir,
                                                     FixUpFileName(table.Schema, string.Format("{0}.{1}.sql", table.Name, smo.Name)));
                                using (StreamWriter sw = GetStreamWriter(FileName, _TableOneFile))
                                {
                                    if (verbose) Console.WriteLine("{0} Scripting {1}.{2}", db.Name, table.Name, smo.Name);
                                    if (!_CreateOnly)
                                    {
                                        so.ScriptDrops = so.IncludeIfNotExists = true;
                                        WriteScript(smo.Script(so), sw);
                                    }
                                    so.ScriptDrops = so.IncludeIfNotExists = false;
                                    WriteScript(smo.Script(so), sw);

                                    if (_ScriptProperties && smo is IExtendedProperties)
                                    {
                                        ScriptProperties((IExtendedProperties)smo, sw);
                                    }
                                }
                            }
                        }

                        #endregion

                        #region Foreign Keys

                        foreach (ForeignKey smo in table.ForeignKeys)
                        {
                            if (!_TableOneFile)
                                FileName =
                                    Path.Combine(foreignKeys,
                                                 FixUpFileName(table.Schema, string.Format("{0}.{1}.sql", table.Name, smo.Name)));
                            using (StreamWriter sw = GetStreamWriter(FileName, _TableOneFile))
                            {
                                if (verbose) Console.WriteLine("{0} Scripting {1}.{2}", db.Name, table.Name, smo.Name);
                                if (!_CreateOnly)
                                {
                                    so.ScriptDrops = so.IncludeIfNotExists = true;
                                }
                                WriteScript(smo.Script(), sw);

                                if (_ScriptProperties && smo is IExtendedProperties)
                                {
                                    ScriptProperties((IExtendedProperties)smo, sw);
                                }
                            }
                        }

                        #endregion

                        #region Constraints

                        foreach (Check smo in table.Checks)
                        {
                            if (!_TableOneFile)
                                FileName =
                                    Path.Combine(constraints,
                                                 FixUpFileName(table.Schema, string.Format("{0}.{1}.sql", table.Name, smo.Name)));
                            using (StreamWriter sw = GetStreamWriter(FileName, _TableOneFile))
                            {
                                if (verbose) Console.WriteLine("{0} Scripting {1}.{2}", db.Name, table.Name, smo.Name);
                                WriteScript(smo.Script(), sw);
                                if (_ScriptProperties && smo is IExtendedProperties)
                                {
                                    ScriptProperties((IExtendedProperties)smo, sw);
                                }
                            }
                        }

                        #endregion

                        #region Script Data

                        if (scriptData)
                        {
                            using (Process p = new Process())
                            {
                                //
                                // makes more sense to pass this cmd line as an arg to scriptdb.exe, 
                                // but I am too lazy to do that now...
                                // besides, we have to leave some work for others!
                                //
                                p.StartInfo.Arguments = string.Format("\"{0}.{1}.{2}\" out {2}.txt -c -T -S{3}",
                                                                      db.Name,
                                                                      table.Schema,
                                                                      table.Name,
                                                                      db.Parent.Name);

                                p.StartInfo.FileName = "bcp.exe";
                                p.StartInfo.WorkingDirectory = data;
                                p.StartInfo.UseShellExecute = false;
                                p.StartInfo.RedirectStandardOutput = true;
                                if (verbose) Console.WriteLine("bcp.exe {0}", p.StartInfo.Arguments);
                                p.Start();
                                string output = p.StandardOutput.ReadToEnd();
                                p.WaitForExit();
                                if (verbose) Console.WriteLine(output);
                            }
                        }

                        #endregion
                    }
                }
                else
                {
                    if (verbose) Console.WriteLine("skipping system object {0}", table.Name);
                }
            }
        }

        private void ScriptAssemblies(bool verbose, Database db, ScriptingOptions so, string outputDirectory)
        {
            string programmability = Path.Combine(outputDirectory, "Programmability");
            string assemblies = Path.Combine(programmability, "Assemblies");
            string dropAssemblies = Path.Combine(assemblies, "Drop");
            //            if (!Directory.Exists(programmability)) Directory.CreateDirectory(programmability);
            //            if (!Directory.Exists(assemblies)) Directory.CreateDirectory(assemblies);
            //            if (!Directory.Exists(dropAssemblies)) Directory.CreateDirectory(dropAssemblies);

            foreach (SqlAssembly smo in db.Assemblies)
            {
                if (!_CreateOnly)
                {
                    using (StreamWriter sw = GetStreamWriter(Path.Combine(dropAssemblies, FixUpFileName(smo.Name) + ".DROP.sql"), false))
                    {
                        if (verbose) Console.WriteLine("Scripting Drop {0}", smo.Name);
                        so.ScriptDrops = so.IncludeIfNotExists = true;

                        //
                        // need to drop any objects that depend on 
                        // this assembly before dropping the assembly!
                        //
                        foreach (UserDefinedFunction ss in db.UserDefinedFunctions)
                        {
                            if (ss.AssemblyName == smo.Name)
                            {
                                WriteScript(ss.Script(so), sw);
                            }
                        }

                        foreach (StoredProcedure ss in db.StoredProcedures)
                        {
                            if (ss.AssemblyName == smo.Name)
                            {
                                WriteScript(ss.Script(so), sw);
                            }
                        }

                        foreach (UserDefinedType ss in db.UserDefinedTypes)
                        {
                            if (ss.AssemblyName == smo.Name)
                            {
                                WriteScript(ss.Script(so), sw);
                            }
                        }

                        WriteScript(smo.Script(so), sw);
                    }
                }
                using (StreamWriter sw = GetStreamWriter(Path.Combine(assemblies, FixUpFileName(smo.Name) + ".sql"), false))
                {
                    if (verbose) Console.WriteLine("{0} Scripting {1}", db.Name, smo.Name);
                    so.ScriptDrops = so.IncludeIfNotExists = false;
                    WriteScript(smo.Script(so), sw);

                    if (_ScriptProperties && smo is IExtendedProperties)
                    {
                        ScriptProperties((IExtendedProperties)smo, sw);
                    }
                }
            }
        }

        private void ScriptSprocs(bool verbose, Database db, ScriptingOptions so, string outputDirectory)
        {
            string programmability = Path.Combine(outputDirectory, "Programmability");
            string sprocs = Path.Combine(programmability, "StoredProcedures");
            //            if (!Directory.Exists(programmability)) Directory.CreateDirectory(programmability);
            //            if (!Directory.Exists(sprocs)) Directory.CreateDirectory(sprocs);


            foreach (StoredProcedure smo in db.StoredProcedures)
            {
                if (!smo.IsSystemObject && !smo.IsEncrypted)
                {
                    if (!FilterExists() || Array.IndexOf(_SprocsFilter, smo.Name) >= 0)
                    {
                        using (StreamWriter sw = GetStreamWriter(Path.Combine(sprocs, FixUpFileName(smo.Schema, smo.Name) + ".sql"), false))
                        {
                            if (verbose) Console.WriteLine("{0} Scripting {1}", db.Name, smo.Name);
                            if (_ScriptAsCreate)
                            {
                                so.ScriptDrops = so.IncludeIfNotExists = true;
                                WriteScript(smo.Script(so), sw);
                            }
                            so.ScriptDrops = so.IncludeIfNotExists = false;

                            if (_ScriptAsCreate)
                            {
                                WriteScript(smo.Script(so), sw);
                            }
                            else
                            {
								
								//replace original code with Regular Expression
								WriteScript
									(
										  smo.Script(so)
										, sw
										, PROCEDURE_CREATE
										, PROCEDURE_ALTER
										, verbose
									);
                            }

                            if (_ScriptProperties && smo is IExtendedProperties)
                            {
                                ScriptProperties((IExtendedProperties)smo, sw);
                            }
                        }
                    }
                }
                else
                {
                    if (verbose) Console.WriteLine("skipping system object {0}", smo.Name);
                }
            }
        }

        private void ScriptViews(bool verbose, Database db, ScriptingOptions so, string outputDirectory)
        {
            string views = Path.Combine(outputDirectory, "Views");
            //            if (!Directory.Exists(views)) Directory.CreateDirectory(views);

            foreach (View smo in db.Views)
            {
                if (!smo.IsSystemObject && !smo.IsEncrypted)
                {
                    if (!FilterExists() || Array.IndexOf(_ViewsFilter, smo.Name) >= 0)
                    {
                        using (StreamWriter sw = GetStreamWriter(Path.Combine(views, FixUpFileName(smo.Schema, smo.Name) + ".sql"), false))
                        {
                            if (verbose) Console.WriteLine("{0} Scripting {1}", db.Name, smo.Name);
                            if (!_CreateOnly)
                            {
                                so.ScriptDrops = so.IncludeIfNotExists = true;
                                WriteScript(smo.Script(so), sw);
                            }
                            so.ScriptDrops = so.IncludeIfNotExists = false;
							
							
                            //WriteScript(smo.Script(so), sw);
							
                            if (_ScriptAsCreate)
                            {
                                WriteScript
								(
									smo.Script(so)
									, sw
								);
                            }
                            else
                            {
								//replace original code with Regular Expression
								WriteScript
									(
										  smo.Script(so)
										, sw
										, VIEW_CREATE
										, VIEW_ALTER
										, verbose
									);
                            }							

                            if (_ScriptProperties && smo is IExtendedProperties)
                            {
                                ScriptProperties((IExtendedProperties)smo, sw);
                            }
                        }
                    }
                }
                else
                {
                    if (verbose) Console.WriteLine("skipping system object {0}", smo.Name);
                }
            }
        }

        private void ScriptUdfs(bool verbose, Database db, ScriptingOptions so, string outputDirectory)
        {
            string programmability = Path.Combine(outputDirectory, "Programmability");
            string udfs = Path.Combine(programmability, "Functions");
            //if (!Directory.Exists(programmability)) Directory.CreateDirectory(programmability);
            //if (!Directory.Exists(udfs)) Directory.CreateDirectory(udfs);


            foreach (UserDefinedFunction smo in db.UserDefinedFunctions)
            {

                if (!smo.IsSystemObject && !smo.IsEncrypted)
                {
                    if (!FilterExists() || Array.IndexOf(_UdfsFilter, smo.Name) >= 0)
                    {
                        using (StreamWriter sw = GetStreamWriter(Path.Combine(udfs, FixUpFileName(smo.Schema, smo.Name) + ".sql"), false))
                        {
                            if (verbose) Console.WriteLine("{0} Scripting {1}", db.Name, smo.Name);
                            if (!_CreateOnly)
                            {
                                so.ScriptDrops = so.IncludeIfNotExists = true;
                                WriteScript(smo.Script(so), sw);
                            }
                            so.ScriptDrops = so.IncludeIfNotExists = false;
							
                            //WriteScript(smo.Script(so), sw);
							
                            if (_ScriptAsCreate)
                            {
                                WriteScript(smo.Script(so), sw);
                            }
                            else
                            {
								//replace original code with Regular Expression
								WriteScript
								(
									  smo.Script(so)
									, sw
									, FUNCTION_CREATE
									, FUNCTION_ALTER
									, verbose
								);
                            }							
							

                            if (_ScriptProperties && smo is IExtendedProperties)
                            {
                                ScriptProperties((IExtendedProperties)smo, sw);
                            }
                        }
                    }
                }
                else
                {
                    if (verbose) Console.WriteLine("skipping system object {0}", smo.Name);
                }
            }
        }

        private void ScriptUdts(bool verbose, Database db, ScriptingOptions so, string outputDirectory)
        {
            string programmability = Path.Combine(outputDirectory, "Programmability");
            string types = Path.Combine(programmability, "Types");
            //if (!Directory.Exists(programmability)) Directory.CreateDirectory(programmability);
            //if (!Directory.Exists(types)) Directory.CreateDirectory(types);

            foreach (UserDefinedType smo in db.UserDefinedTypes)
            {
                if (!FilterExists() || Array.IndexOf(_UdtsFilter, smo.Name) >= 0)
                {
                    using (StreamWriter sw = GetStreamWriter(Path.Combine(types, FixUpFileName(smo.Schema, smo.Name) + ".sql"), false))
                    {
                        if (verbose) Console.WriteLine("{0} Scripting {1}", db.Name, smo.Name);
                        if (!_CreateOnly)
                        {
                            so.ScriptDrops = so.IncludeIfNotExists = true;
                            WriteScript(smo.Script(so), sw);
                        }
                        so.ScriptDrops = so.IncludeIfNotExists = false;
                        WriteScript(smo.Script(so), sw);

                        if (_ScriptProperties && smo is IExtendedProperties)
                        {
                            ScriptProperties((IExtendedProperties)smo, sw);
                        }
                    }
                }
            }
        }

        private void ScriptUddts(bool verbose, Database db, ScriptingOptions so, string outputDirectory)
        {
            string programmability = Path.Combine(outputDirectory, "Programmability");
            string types = Path.Combine(programmability, "Types");
            //if (!Directory.Exists(programmability)) Directory.CreateDirectory(programmability);
            //if (!Directory.Exists(types)) Directory.CreateDirectory(types);

            foreach (UserDefinedDataType smo in db.UserDefinedDataTypes)
            {
                if (!FilterExists() || Array.IndexOf(_UddtsFilter, smo.Name) >= 0)
                {
                    using (StreamWriter sw = GetStreamWriter(Path.Combine(types, FixUpFileName(smo.Schema, smo.Name) + ".sql"), false))
                    {
                        if (verbose) Console.WriteLine("{0} Scripting {1}", db.Name, smo.Name);
                        if (!_CreateOnly)
                        {
                            so.ScriptDrops = so.IncludeIfNotExists = true;
                            WriteScript(smo.Script(so), sw);
                        }
                        so.ScriptDrops = so.IncludeIfNotExists = false;
                        WriteScript(smo.Script(so), sw);

                        if (_ScriptProperties && smo is IExtendedProperties)
                        {
                            ScriptProperties((IExtendedProperties)smo, sw);
                        }
                    }
                }
            }
        }

        private void ScriptRules(bool verbose, Database db, ScriptingOptions so, string outputDirectory)
        {
            string programmability = Path.Combine(outputDirectory, "Programmability");
            string rules = Path.Combine(programmability, "Rules");
            //if (!Directory.Exists(programmability)) Directory.CreateDirectory(programmability);
            //if (!Directory.Exists(rules)) Directory.CreateDirectory(rules);


            foreach (Rule smo in db.Rules)
            {
                if (!FilterExists() || Array.IndexOf(_RulesFilter, smo.Name) >= 0)
                {
                    using (StreamWriter sw = GetStreamWriter(Path.Combine(rules, FixUpFileName(smo.Schema, smo.Name) + ".sql"), false))
                    {
                        if (verbose) Console.WriteLine("{0} Scripting {1}", db.Name, smo.Name);
                        if (!_CreateOnly)
                        {
                            so.ScriptDrops = so.IncludeIfNotExists = true;
                            WriteScript(smo.Script(so), sw);
                        }
                        so.ScriptDrops = so.IncludeIfNotExists = false;

                        //WriteScript(smo.Script(so), sw);						
						
						if (_ScriptAsCreate)
						{
							WriteScript(smo.Script(so), sw);
						}
						else
						{
							//replace original code with Regular Expression
							WriteScript
							(
								  smo.Script(so)
								, sw
								, RULE_CREATE
								, RULE_ALTER
								, verbose
							);
						}
							


                        if (_ScriptProperties && smo is IExtendedProperties)
                        {
                            ScriptProperties((IExtendedProperties)smo, sw);
                        }
                    }
                }
            }
        }

        private void ScriptDefaults(bool verbose, Database db, ScriptingOptions so, string outputDirectory)
        {
            string programmability = Path.Combine(outputDirectory, "Programmability");
            //            if (!Directory.Exists(programmability)) Directory.CreateDirectory(programmability);
            string defaults = Path.Combine(programmability, "Defaults");
            //            if (!Directory.Exists(defaults)) Directory.CreateDirectory(defaults);

            foreach (Default smo in db.Defaults)
            {
                if (!FilterExists() || Array.IndexOf(_DefaultsFilter, smo.Name) >= 0)
                {
                    using (
                        StreamWriter sw =
                            GetStreamWriter(Path.Combine(defaults, FixUpFileName(smo.Name) + ".sql"), false))
                    {
                        if (verbose) Console.WriteLine("{0} Scripting {1}", db.Name, smo.Name);
                        if (!_CreateOnly)
                        {
                            so.ScriptDrops = so.IncludeIfNotExists = true;
                            WriteScript(smo.Script(so), sw);
                        }
                        so.ScriptDrops = so.IncludeIfNotExists = false;
                        WriteScript(smo.Script(so), sw);

                        if (_ScriptProperties && smo is IExtendedProperties)
                        {
                            ScriptProperties((IExtendedProperties)smo, sw);
                        }
                    }
                }
            }
        }

        private void ScriptDdlTriggers(bool verbose, Database db, ScriptingOptions so, string outputDirectory)
        {
            string programmability = Path.Combine(outputDirectory, "Programmability");
            //            if (!Directory.Exists(programmability)) Directory.CreateDirectory(programmability);
            string triggers = Path.Combine(programmability, "Triggers");
            //            if (!Directory.Exists(triggers)) Directory.CreateDirectory(triggers);

			if (verbose)
			{			
				if (_DdlTriggersFilter != null)
				{
					Console.WriteLine("_DdlTriggersFilter is " + _DdlTriggersFilter.ToString());
					Console.WriteLine("_DdlTriggersFilter Count " + _DdlTriggersFilter.Length);
					Console.WriteLine("db.Triggers Count " + db.Triggers.Count);
				}			
			}
			
            foreach (DatabaseDdlTrigger smo in db.Triggers)
            {
				
				if (verbose)
				{	
					Console.WriteLine("smo.Name is " + smo.Name);
				}
				
                if (!FilterExists() || Array.IndexOf(_DdlTriggersFilter, smo.Name) >= 0)
                {
                    using (StreamWriter sw = GetStreamWriter(Path.Combine(triggers, FixUpFileName(smo.Name) + ".sql"), false))
                    {
                        
						if (verbose) Console.WriteLine("{0} Scripting {1}", db.Name, smo.Name);
						
                        if (!_CreateOnly)
                        {
                            so.ScriptDrops = so.IncludeIfNotExists = true;
                            WriteScript(smo.Script(so), sw);
                        }
						
                        so.ScriptDrops = so.IncludeIfNotExists = false;
						
                        //WriteScript(smo.Script(so), sw);
						

						if (_ScriptAsCreate)
						{
							WriteScript(smo.Script(so), sw);
						}
						else
						{
							//replace original code with Regular Expression
							WriteScript
							(
								  smo.Script(so)
								, sw
								, TRIGGER_CREATE
								, TRIGGER_ALTER
								, verbose
							);
						}
						

                        if (_ScriptProperties && smo is IExtendedProperties)
                        {
                            ScriptProperties((IExtendedProperties)smo, sw);
                        }
                    }
                }
            }
        }

        private void ScriptSchemas(bool verbose, Database db, ScriptingOptions so, string outputDirectory)
        {
            string schemas = Path.Combine(outputDirectory, "Schemas");
            //            if (!Directory.Exists(schemas)) Directory.CreateDirectory(schemas);

            foreach (Schema smo in db.Schemas)
            {
                // IsSystemObject doesn't exist for schemas.  Bad Cip!!!
                if (smo.Name == "sys" ||
                    smo.Name == "dbo" ||
                    smo.Name == "db_accessadmin" ||
                    smo.Name == "db_backupoperator" ||
                    smo.Name == "db_datareader" ||
                    smo.Name == "db_datawriter" ||
                    smo.Name == "db_ddladmin" ||
                    smo.Name == "db_denydatawriter" ||
                    smo.Name == "db_denydatareader" ||
                    smo.Name == "db_owner" ||
                    smo.Name == "db_securityadmin" ||
                    smo.Name == "INFORMATION_SCHEMA" ||
                    smo.Name == "guest") continue;

                if (!FilterExists() || Array.IndexOf(_SchemasFilter, smo.Name) >= 0)
                {
                    using (StreamWriter sw = GetStreamWriter(Path.Combine(schemas, FixUpFileName(smo.Name) + ".sql"), false))
                    {
                        if (verbose) Console.WriteLine("{0} Scripting {1}", db.Name, smo.Name);
                        if (!_CreateOnly)
                        {
                            so.ScriptDrops = so.IncludeIfNotExists = true;
                            WriteScript(smo.Script(so), sw);
                        }
                        so.ScriptDrops = so.IncludeIfNotExists = false;
                        WriteScript(smo.Script(so), sw);

                        if (_ScriptProperties && smo is IExtendedProperties)
                        {
                            ScriptProperties((IExtendedProperties)smo, sw);
                        }
                    }
                }
            }
        }

        private void ScriptProperties(IExtendedProperties obj, StreamWriter sw)
        {
            if (obj == null) throw new ArgumentNullException("obj");
            if (sw == null) throw new ArgumentNullException("sw");

            foreach (ExtendedProperty ep in obj.ExtendedProperties)
            {
                WriteScript(ep.Script(), sw);
            }
        }

        #endregion

        #region Private Utility Functions

        private void WriteScript20160216(StringCollection script, StreamWriter sw, string replaceMe, string replaceWith)
        {
			
			string sss;
			
            foreach (string ss in script)
            {
                if (ss == "SET QUOTED_IDENTIFIER ON" ||
                    ss == "SET QUOTED_IDENTIFIER OFF" ||
                    ss == "SET ANSI_NULLS ON" ||
                    ss == "SET ANSI_NULLS OFF")
                {
                    continue;
                }

                sss = ReplaceEx(ss, replaceMe, replaceWith);
				
                sw.WriteLine(sss);
                sw.WriteLine("GO\r\n");
            }
        }

		/*
			Convert Array, String
			http://www.dotnetperls.com/convert-string-array-string
		*/
		private String convertStringCollectionToString
		(
			  StringCollection script
			, String separator
		)
		{
			StringBuilder builder = new StringBuilder();
			
			foreach (string value in script)
            {
				
				builder.Append(value);
				
				if (separator != null)
				{

					builder.Append( System.Environment.NewLine);	
					
					builder.Append(separator);				
					
					builder.Append( System.Environment.NewLine);	
				}			
				
				builder.Append( System.Environment.NewLine);				

			}
				
			return builder.ToString();
	
			
		}
		
        private void WriteScript
		(
			  StringCollection script
			, StreamWriter sw
			, string replaceMe
			, string replaceWith
			, Boolean verbose
		)
        {
			
			string sss;
			string scriptBuffer = null;
			
			//scriptBuffer = script.ToString();
			
			scriptBuffer = convertStringCollectionToString(script, "GO");
			
			sss = ReplaceExUsingRegularExpression
					(
						  scriptBuffer
						, replaceMe
						, replaceWith
						, verbose
					);
				
            sw.WriteLine(sss);
			
            sw.WriteLine("GO\r\n");

        }
		
        private void WriteScript(StringCollection script, StreamWriter sw)
        {
            foreach (string ss in script)
            {
                if (ss == "SET QUOTED_IDENTIFIER ON" ||
                    ss == "SET QUOTED_IDENTIFIER OFF" ||
                    ss == "SET ANSI_NULLS ON" ||
                    ss == "SET ANSI_NULLS OFF")
                {
                    continue;
                }

                sw.WriteLine(ss);
                sw.WriteLine("GO\r\n");
            }
        }		

        /// <summary>
        /// for case-insensitive string replace.  from www.codeproject.com
        /// </summary>
        /// <param name="original"></param>
        /// <param name="pattern"></param>
        /// <param name="replacement"></param>
        /// <returns></returns>
        private string ReplaceEx(string original, string pattern, string replacement)
        {
            int count, position0, position1;
            count = position0 = position1 = 0;
            string upperString = original.ToUpper();
            string upperPattern = pattern.ToUpper();
            int inc = (original.Length / pattern.Length) * (replacement.Length - pattern.Length);
            char[] chars = new char[original.Length + Math.Max(0, inc)];
            while ((position1 = upperString.IndexOf(upperPattern, position0)) != -1)
            {
                for (int i = position0; i < position1; ++i) chars[count++] = original[i];
                for (int i = 0; i < replacement.Length; ++i) chars[count++] = replacement[i];
                position0 = position1 + pattern.Length;
            }
            if (position0 == 0) return original;
            for (int i = position0; i < original.Length; ++i) chars[count++] = original[i];
            return new string(chars, 0, count);
        }
		
        /// <summary>
        /// for case-insensitive string replace.  from www.codeproject.com
        /// </summary>
        /// <param name="original"></param>
        /// <param name="pattern"></param>
        /// <param name="replacement"></param>
        /// <returns></returns>
        private string ReplaceExUsingRegularExpression(string original, string pattern, string replacement, Boolean verbose)
        {
			
			Regex  rgx = null;
            String result;
			
			if (verbose)
			{
				
				Console.WriteLine
					(
						   "Original is " 
						+  System.Environment.NewLine 
						+ original
					);
					
				Console.WriteLine
					(
						   "Pattern is "  
						+  System.Environment.NewLine
						+ pattern
					);
					
				Console.WriteLine
					(
						   "Replacement is "  
						+  System.Environment.NewLine
						+  replacement
					);
					
			}
			
			rgx = new Regex
					(
						  pattern
						, RegexOptions.IgnoreCase
					);
			
			result = rgx.Replace
						(
							  original
							, replacement
						);

			if (verbose)
			{
			
				Console.WriteLine("Result is "  +  System.Environment.NewLine + result);
				
			}						
			
			return (result);
			
        }		

        private string sanitizeFileName(string filename)
        {		
		            return filename
                .Replace("[", ".")
                .Replace("]", ".")
                .Replace(" ", ".")
                .Replace("&", ".")
                .Replace("'", ".")
                .Replace("\"", ".")
                .Replace(">", ".")
                .Replace("<", ".")
                .Replace("!", ".")
                .Replace("@", ".")
                .Replace("#", ".")
                .Replace("$", ".")
                .Replace("%", ".")
                .Replace("^", ".")
                .Replace("*", ".")
                .Replace("(", ".")
                .Replace(")", ".")
                .Replace("+", ".")
                .Replace("{", ".")
                .Replace("}", ".")
                .Replace("|", ".")
                .Replace("\\", ".")
                .Replace("?", ".")
                .Replace(",", ".")
                .Replace("/", ".")
                .Replace(";", ".")
                .Replace(":", ".")
                .Replace("-", ".")
                .Replace("=", ".")
                .Replace("`", ".")
                .Replace("~", ".");
		}

        private string FixUpFileName(string objectName)
        {
			
			return (sanitizeFileName(objectName));

        }
		
        private string FixUpFileName(string schema, string objectName)
        {
			
			String fullObjectName = schema + "." + objectName;
			
			fullObjectName = sanitizeFileName(fullObjectName);
			
			return (fullObjectName);

        }

        /// <summary>
        /// THIS FUNCTION HAS A SIDEEFFECT.
        /// If OutputFileName is set, it will always open the filename
        /// </summary>
        /// <param name="Path"></param>
        /// <param name="Append"></param>
        /// <returns></returns>
        private StreamWriter GetStreamWriter(string Path, bool Append)
        {
            if (_OutputFileName != null)
            {
                Path = OutputFileName;
                Append = true;
            }
            if (OutputFileName == "-")
                return new StreamWriter(System.Console.OpenStandardOutput());

            if (!Directory.Exists(System.IO.Path.GetDirectoryName(Path))) Directory.CreateDirectory(System.IO.Path.GetDirectoryName(Path));
            return new StreamWriter(Path, Append);
        }

        #endregion

        #region Public Properties

        public string[] TableFilter
        {
            get { return _TableFilter; }
            set { _TableFilter = value; }
        }

        public string[] RulesFilter
        {
            get { return _RulesFilter; }
            set { _RulesFilter = value; }
        }

        public string[] DefaultsFilter
        {
            get { return _DefaultsFilter; }
            set { _DefaultsFilter = value; }
        }

        public string[] UddtsFilter
        {
            get { return _UddtsFilter; }
            set { _UddtsFilter = value; }
        }

        public string[] UdfsFilter
        {
            get { return _UdfsFilter; }
            set { _UdfsFilter = value; }
        }

        public string[] ViewsFilter
        {
            get { return _ViewsFilter; }
            set { _ViewsFilter = value; }
        }

        public string[] SprocsFilter
        {
            get { return _SprocsFilter; }
            set { _SprocsFilter = value; }
        }

        public string[] UdtsFilter
        {
            get { return _UdtsFilter; }
            set { _UdtsFilter = value; }
        }

        public string[] SchemasFilter
        {
            get { return _SchemasFilter; }
            set { _SchemasFilter = value; }
        }

        public string[] DdlTriggersFilter
        {
            get { return _DdlTriggersFilter; }
            set { _DdlTriggersFilter = value; }
        }

		public string[] TableTriggerFilter
        {
            get { return _TableTriggerFilter; }
            set { _TableTriggerFilter = value; }
        }
		
        public bool TableOneFile
        {
            get { return _TableOneFile; }
            set { _TableOneFile = value; }
        }

        public bool ScriptAsCreate
        {
            get { return _ScriptAsCreate; }
            set { _ScriptAsCreate = value; }
        }

        public bool Permissions
        {
            get { return _Permissions; }
            set { _Permissions = value; }
        }

        public bool NoCollation
        {
            get { return _NoCollation; }
            set { _NoCollation = value; }
        }

        public bool CreateOnly
        {
            get { return _CreateOnly; }
            set { _CreateOnly = value; }
        }

        public string OutputFileName
        {
            get { return _OutputFileName; }
            set { _OutputFileName = value; }
        }
        public bool IncludeDatabase
        {
            get { return _IncludeDatabase; }
            set { _IncludeDatabase = value; }
        }
        #endregion
    }
}
