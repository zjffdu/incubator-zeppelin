/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.flink;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.zeppelin.flink.sql.SqlCommandParser;
import org.apache.zeppelin.flink.sql.SqlCommandParser.SqlCommand;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.util.SqlSplitter;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Matcher;

public abstract class FlinkSqlInterrpeter extends Interpreter {

  protected static final Logger LOGGER = LoggerFactory.getLogger(FlinkSqlInterrpeter.class);

  protected static final String MESSAGE_HELP = new AttributedStringBuilder()
          .append("The following commands are available:\n\n")
          .append(formatCommand(SqlCommand.CREATE_TABLE, "Create table under current catalog and database."))
          .append(formatCommand(SqlCommand.DROP_TABLE, "Drop table with optional catalog and database. Syntax: 'DROP TABLE [IF EXISTS] <name>;'"))
          .append(formatCommand(SqlCommand.CREATE_VIEW, "Creates a virtual table from a SQL query. Syntax: 'CREATE VIEW <name> AS <query>;'"))
          .append(formatCommand(SqlCommand.DESCRIBE, "Describes the schema of a table with the given name."))
          .append(formatCommand(SqlCommand.DROP_VIEW, "Deletes a previously created virtual table. Syntax: 'DROP VIEW <name>;'"))
          .append(formatCommand(SqlCommand.EXPLAIN, "Describes the execution plan of a query or table with the given name."))
          .append(formatCommand(SqlCommand.HELP, "Prints the available commands."))
          .append(formatCommand(SqlCommand.INSERT_INTO, "Inserts the results of a SQL SELECT query into a declared table sink."))
          .append(formatCommand(SqlCommand.INSERT_OVERWRITE, "Inserts the results of a SQL SELECT query into a declared table sink and overwrite existing data."))
          .append(formatCommand(SqlCommand.SELECT, "Executes a SQL SELECT query on the Flink cluster."))
          .append(formatCommand(SqlCommand.SHOW_FUNCTIONS, "Shows all user-defined and built-in functions."))
          .append(formatCommand(SqlCommand.SHOW_TABLES, "Shows all registered tables."))
          .append(formatCommand(SqlCommand.USE_CATALOG, "Sets the current catalog. The current database is set to the catalog's default one. Experimental! Syntax: 'USE CATALOG <name>;'"))
          .append(formatCommand(SqlCommand.USE, "Sets the current default database. Experimental! Syntax: 'USE <name>;'"))
          .style(AttributedStyle.DEFAULT.underline())
          .append("\nHint")
          .style(AttributedStyle.DEFAULT)
          .append(": Make sure that a statement ends with ';' for finalizing (multi-line) statements.")
          .toAttributedString()
          .toString();

  protected FlinkInterpreter flinkInterpreter;
  protected TableEnvironment tbenv;
  private SqlSplitter sqlSplitter;

  public FlinkSqlInterrpeter(Properties properties) {
    super(properties);
  }

  protected abstract boolean isBatch();

  @Override
  public void open() throws InterpreterException {
    flinkInterpreter =
            getInterpreterInTheSameSessionByClassName(FlinkInterpreter.class);
    this.sqlSplitter = new SqlSplitter();
  }

  @Override
  public InterpreterResult interpret(String st,
                                     InterpreterContext context) throws InterpreterException {
    LOGGER.debug("Interpret code: " + st);
    flinkInterpreter.getZeppelinContext().setInterpreterContext(context);
    flinkInterpreter.getZeppelinContext().setNoteGui(context.getNoteGui());
    flinkInterpreter.getZeppelinContext().setGui(context.getGui());

    // set ClassLoader of current Thread to be the ClassLoader of Flink scala-shell,
    // otherwise codegen will fail to find classes defined in scala-shell
    ClassLoader originClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(flinkInterpreter.getFlinkScalaShellLoader());
      return runSqlList(st, context);
    } finally {
      Thread.currentThread().setContextClassLoader(originClassLoader);
    }
  }

  private Optional<SqlCommandParser.SqlCommandCall> parse(String stmt) {
    // normalize
    stmt = stmt.trim();
    // remove ';' at the end
    if (stmt.endsWith(";")) {
      stmt = stmt.substring(0, stmt.length() - 1).trim();
    }

    // parse
    for (SqlCommandParser.SqlCommand cmd : SqlCommandParser.SqlCommand.values()) {
      final Matcher matcher = cmd.pattern.matcher(stmt);
      if (matcher.matches()) {
        final String[] groups = new String[matcher.groupCount()];
        for (int i = 0; i < groups.length; i++) {
          groups[i] = matcher.group(i + 1);
        }
        return cmd.operandConverter.apply(groups)
                .map((operands) -> new SqlCommandParser.SqlCommandCall(cmd, operands));
      }
    }
    return Optional.empty();
  }

  private InterpreterResult runSqlList(String st, InterpreterContext context) {
    List<String> sqls = sqlSplitter.splitSql(st);
    List<SqlCommandParser.SqlCommandCall> sqlCommands = new ArrayList<>();
    for (String sql : sqls) {
      Optional<SqlCommandParser.SqlCommandCall> sqlCommand = parse(sql);
      if (!sqlCommand.isPresent()) {
        try {
          context.out.write("Invalid Sql statement: " + sql + "\n");
          context.out.write(MESSAGE_HELP);
        } catch (IOException e) {
          return new InterpreterResult(InterpreterResult.Code.ERROR, e.toString());
        }
        return new InterpreterResult(InterpreterResult.Code.ERROR);
      }
      sqlCommands.add(sqlCommand.get());
    }
    for (SqlCommandParser.SqlCommandCall sqlCommand : sqlCommands) {
      try {
        callCommand(sqlCommand, context);
        context.out.flush();
      }  catch (Throwable e) {
        LOGGER.error("Fail to run sql:" + sqlCommand.operands[0], e);
        try {
          context.out.write("Fail to run sql command: " +
                  sqlCommand.operands[0] + "\n" + ExceptionUtils.getStackTrace(e));
        } catch (IOException ex) {
          LOGGER.warn("Unexpected exception:", ex);
          return new InterpreterResult(InterpreterResult.Code.ERROR,
                  ExceptionUtils.getStackTrace(e));
        }
        return new InterpreterResult(InterpreterResult.Code.ERROR);
      }
    }
    return new InterpreterResult(InterpreterResult.Code.SUCCESS);
  }

  private void callCommand(SqlCommandParser.SqlCommandCall cmdCall,
                                        InterpreterContext context) throws Exception {
    switch (cmdCall.command) {
      case HELP:
        callHelp(context);
        break;
      case SHOW_CATALOGS:
        callShowCatalogs(context);
        break;
      case SHOW_DATABASES:
        callShowDatabases(context);
        break;
      case SHOW_TABLES:
        callShowTables(context);
        break;
      case SHOW_FUNCTIONS:
        callShowFunctions(context);
        break;
      case SHOW_MODULES:
        callShowModules();
        break;
      case USE_CATALOG:
        callUseCatalog(cmdCall.operands[0], context);
        break;
      case USE:
        callUseDatabase(cmdCall.operands[0], context);
        break;
      case DESCRIBE:
        callDescribe(cmdCall.operands[0], context);
        break;
      case EXPLAIN:
        callExplain(cmdCall.operands[0], context);
        break;
      case SELECT:
        callSelect(cmdCall.operands[0], context);
        break;
      case INSERT_INTO:
      case INSERT_OVERWRITE:
        callInsertInto(cmdCall.operands[0], context);
        break;
      case CREATE_TABLE:
        callCreateTable(cmdCall.operands[0], context);
        break;
      case DROP_TABLE:
        callDropTable(cmdCall.operands[0], context);
        break;
      case CREATE_VIEW:
        callCreateView(cmdCall.operands[0], cmdCall.operands[1], context);
        break;
      case DROP_VIEW:
        callDropView(cmdCall.operands[0], context);
        break;
      case CREATE_DATABASE:
        callCreateDatabase(cmdCall.operands[0], context);
        break;
      case DROP_DATABASE:
        callDropDatabase(cmdCall.operands[0], context);
        break;
      case ALTER_DATABASE:
        callAlterDatabase(cmdCall.operands[0], context);
        break;
      case ALTER_TABLE:
        callAlterTable(cmdCall.operands[0], context);
        break;
      default:
        throw new Exception("Unsupported command: " + cmdCall.command);
    }
  }

  private void callAlterTable(String sql, InterpreterContext context) throws IOException {
    this.tbenv.sqlUpdate(sql);
    context.out.write("Table has been modified.\n");
  }

  private void callAlterDatabase(String sql, InterpreterContext context) throws IOException {
    this.tbenv.sqlUpdate(sql);
    context.out.write("Database has been modified.\n");
  }

  private void callDropDatabase(String sql, InterpreterContext context) throws IOException {
    this.tbenv.sqlUpdate(sql);
    context.out.write("Database has been dropped.\n");
  }

  private void callCreateDatabase(String sql, InterpreterContext context) throws IOException {
    this.tbenv.sqlUpdate(sql);
    context.out.write("Database has been created.\n");
  }

  private void callDropView(String sql, InterpreterContext context) throws IOException {
    this.tbenv.sqlUpdate(sql);
    context.out.write("View has been dropped.\n");
  }

  private void callCreateView(String name, String query, InterpreterContext context) {
    this.tbenv.createTemporaryView(name, tbenv.sqlQuery(query));
    tbenv.createTemporaryView(name, tbenv.sqlQuery(query));
  }

  private void callCreateTable(String sql, InterpreterContext context) throws IOException {
    this.tbenv.sqlUpdate(sql);
    context.out.write("Table has been created.\n");
  }

  private void callDropTable(String sql, InterpreterContext context) throws IOException {
    this.tbenv.sqlUpdate(sql);
    context.out.write("Table has been dropped.\n");
  }

  private void callUseCatalog(String catalog, InterpreterContext context) {
    this.tbenv.useCatalog(catalog);
  }

  private void callShowModules() {

  }

  private void callHelp(InterpreterContext context) throws IOException {
    context.out.write(MESSAGE_HELP);
  }

  private void callShowCatalogs(InterpreterContext context) throws IOException {
    String[] catalogs = this.tbenv.listCatalogs();
    context.out.write("%table catalog\n" + StringUtils.join(catalogs, "\n") + "\n");
  }

  private void callShowDatabases(InterpreterContext context) throws IOException {
    String[] databases = this.tbenv.listDatabases();
    context.out.write(
            "%table database\n" + StringUtils.join(databases, "\n") + "\n");
  }

  private void callShowTables(InterpreterContext context) throws IOException {
    String[] tables = this.tbenv.listTables();
    context.out.write(
            "%table table\n" + StringUtils.join(tables, "\n") + "\n");
  }

  private void callShowFunctions(InterpreterContext context) throws IOException {
    String[] functions = this.tbenv.listUserDefinedFunctions();
    context.out.write(
            "%table function\n" + StringUtils.join(functions, "\n") + "\n");
  }

  private void callUseDatabase(String databaseName,
                               InterpreterContext context) throws IOException {
    tbenv.useDatabase(databaseName);
  }

  private void callDescribe(String name, InterpreterContext context) throws IOException {
    TableSchema schema = tbenv.scan(name).getSchema();
    StringBuilder builder = new StringBuilder();
    builder.append("Column\tType\n");
    for (int i = 0; i < schema.getFieldCount(); ++i) {
      builder.append(schema.getFieldName(i).get() + "\t" + schema.getFieldDataType(i).get() + "\n");
    }
    context.out.write("%table\n" + builder.toString());
  }

  private void callExplain(String sql, InterpreterContext context) throws IOException {
    Table table = this.tbenv.sqlQuery(sql);
    context.out.write(this.tbenv.explain(table) + "\n");
  }

  public abstract void callSelect(String sql, InterpreterContext context) throws IOException;

  private void callInsertInto(String sql,
                              InterpreterContext context) throws IOException {
     if (!isBatch()) {
       context.getLocalProperties().put("flink.streaming.insert_into", "true");
     }
     this.tbenv.sqlUpdate(sql);
     try {
       this.tbenv.execute(sql);
     } catch (Exception e) {
       throw new IOException(e);
     }
     context.out.write("Insertion successfully.\n");
  }

  private static AttributedString formatCommand(SqlCommand cmd, String description) {
    return new AttributedStringBuilder()
            .style(AttributedStyle.DEFAULT.bold())
            .append(cmd.toString())
            .append("\t\t")
            .style(AttributedStyle.DEFAULT)
            .append(description)
            .append('\n')
            .toAttributedString();
  }
}
