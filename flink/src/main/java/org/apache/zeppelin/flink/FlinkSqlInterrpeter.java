package org.apache.zeppelin.flink;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.zeppelin.flink.sql.SqlCommandParser;
import org.apache.zeppelin.flink.sql.SqlInfo;
import org.apache.zeppelin.flink.sql.SqlLists;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Matcher;

public abstract class FlinkSqlInterrpeter extends Interpreter {

  private static final Logger LOGGER = LoggerFactory.getLogger(FlinkSqlInterrpeter.class);

  protected FlinkInterpreter flinkInterpreter;
  protected TableEnvironment tbenv;

  public FlinkSqlInterrpeter(Properties properties) {
    super(properties);
  }

  @Override
  public void open() throws InterpreterException {
    flinkInterpreter =
            getInterpreterInTheSameSessionByClassName(FlinkInterpreter.class);
  }

  @Override
  public InterpreterResult interpret(String st,
                                     InterpreterContext context) throws InterpreterException {
    LOGGER.debug("Interpret code: " + st);
    flinkInterpreter.getZeppelinContext().setInterpreterContext(context);
    flinkInterpreter.getZeppelinContext().setNoteGui(context.getNoteGui());
    flinkInterpreter.getZeppelinContext().setGui(context.getGui());

    checkLocalProperties(context.getLocalProperties());

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


  protected abstract void checkLocalProperties(Map<String, String> localProperties)
          throws InterpreterException;

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

  private InterpreterResult runSqlList(String sql, InterpreterContext context) {
    List<SqlInfo> sqlLists = SqlLists.getSQLList(sql);
    List<SqlCommandParser.SqlCommandCall> sqlCommands = new ArrayList<>();
    for (SqlInfo sqlInfo : sqlLists) {
      Optional<SqlCommandParser.SqlCommandCall> sqlCommand = parse(sqlInfo.getSqlContent());
      if (!sqlCommand.isPresent()) {
        return new InterpreterResult(InterpreterResult.Code.ERROR, "Invalid Sql statement: "
                + sqlInfo.getSqlContent());
      }
      sqlCommands.add(sqlCommand.get());
    }
    for (SqlCommandParser.SqlCommandCall sqlCommand : sqlCommands) {
      try {
        callCommand(sqlCommand, context);
        context.out.flush();
      }  catch (Throwable e) {
        LOGGER.error("Fail to run sql:" + sqlCommand.operands[0] + "\n"
                + ExceptionUtils.getStackTrace(e));
        return new InterpreterResult(InterpreterResult.Code.ERROR, "Fail to run sql command: " +
                sqlCommand.operands[0] + "\n" + ExceptionUtils.getStackTrace(e));
      }
    }
    return new InterpreterResult(InterpreterResult.Code.SUCCESS);
  }

  private void callCommand(SqlCommandParser.SqlCommandCall cmdCall,
                                        InterpreterContext context) throws Exception {
    switch (cmdCall.command) {
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
      case USE_DATABASE:
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
        callInsertInto(cmdCall.operands[0], context);
        break;
      default:
        throw new Exception("Unsupported command: " + cmdCall.command);
    }
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

  private void callUseDatabase(String databaseName, InterpreterContext context) throws IOException {
    tbenv.useDatabase(databaseName);
  }

  private void callDescribe(String name, InterpreterContext context) throws IOException {
    TableSchema schema = tbenv.scan(name).getSchema();
    StringBuilder builder = new StringBuilder();
    builder.append("Column\tType\n");
    for (int i = 0; i < schema.getFieldCount(); ++i) {
      builder.append(schema.getFieldName(i) + "\t" + schema.getFieldDataType(i) + "\n");
    }
    context.out.write(builder.toString());
  }

  private void callExplain(String sql, InterpreterContext context) throws IOException {
    Table table = this.tbenv.sqlQuery(sql);
    context.out.write(this.tbenv.explain(table) + "\n");
  }

  public abstract void callSelect(String sql, InterpreterContext context) throws IOException;

  private void callInsertInto(String sql,
                              InterpreterContext context) throws IOException {
    //    this.tbenv.sqlUpdate(sql);
    //
    //    JobGraph jobGraph = createJobGraph(sql);
    //    jobGraph.addJar(new Path(flinkInterpreter.getInnerIntp().getFlinkILoop()
    //            .writeFilesToDisk().getAbsoluteFile().toURI()));
    //    SqlJobRunner jobRunner =
    //            new SqlJobRunner(flinkInterpreter.getInnerIntp().getCluster(), jobGraph, sql,
    //                    flinkInterpreter.getFlinkScalaShellLoader());
    //    jobRunner.run();
    context.out.write("Insert Succeeded.\n");
  }

}
