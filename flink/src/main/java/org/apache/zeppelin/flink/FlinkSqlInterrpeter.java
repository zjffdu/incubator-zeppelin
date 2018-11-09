package org.apache.zeppelin.flink;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.core.fs.Path;
import org.apache.flink.optimizer.plan.FlinkPlan;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.sql.parser.ddl.SqlCreateDatabase;
import org.apache.flink.sql.parser.ddl.SqlCreateFunction;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateView;
import org.apache.flink.sql.parser.ddl.SqlDropFunction;
import org.apache.flink.sql.parser.ddl.SqlDropTable;
import org.apache.flink.sql.parser.ddl.SqlDropView;
import org.apache.flink.sql.parser.ddl.SqlNodeInfo;
import org.apache.flink.sql.parser.util.SqlInfo;
import org.apache.flink.sql.parser.util.SqlLists;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.functions.FunctionService;
import org.apache.flink.table.api.functions.UserDefinedFunction;
import org.apache.flink.table.client.cli.CliStrings;
import org.apache.flink.table.client.cli.SqlCommandParser;
import org.apache.flink.table.client.config.entries.FunctionEntry;
import org.apache.flink.table.client.utils.SqlJobUtil;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.FunctionDescriptor;
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

  private InterpreterResult runSqlList(String sql, InterpreterContext context) {
    List<SqlInfo> sqlLists = SqlLists.getSQLList(sql);
    List<SqlCommandParser.SqlCommandCall> sqlCommands = new ArrayList<>();
    for (SqlInfo sqlInfo : sqlLists) {
      Optional<SqlCommandParser.SqlCommandCall> sqlCommand =
              SqlCommandParser.parse(sqlInfo.getSqlContent());
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
      case SHOW_VIEWS:
        callShowViews(context);
        break;
      case SHOW_FUNCTIONS:
        callShowFunctions(context);
        break;
      case USE:
        callUseDatabase(cmdCall.operands[0]);
        break;
      case DESCRIBE:
      case DESC:
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
      case CREATE_TABLE:
        callCreateTable(cmdCall.operands[0], context);
        break;
      case DROP_TABLE:
        callDropTable(cmdCall.operands[0], context);
        break;
      case CREATE_VIEW:
        callCreateView(cmdCall.operands[0], context);
        break;
      case DROP_VIEW:
        callDropView(cmdCall.operands[0], context);
        break;
      case CREATE_FUNCTION:
        callCreateFunction(cmdCall.operands[0], context);
        break;
      case DROP_FUNCTION:
        callDropFunction(cmdCall.operands[0], context);
        break;
      case CREATE_DATABASE:
        callCreateDatabase(cmdCall.operands[0], context);
        break;
      case DROP_DATABASE:
        callDropDatabase(cmdCall.operands[0], context);
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

  private void callShowViews(InterpreterContext context) throws IOException {
    String[] views = this.tbenv.listViews();
    context.out.write(
            "%table view\n" + StringUtils.join(views, "\n") + "\n");
  }

  private void callShowFunctions(InterpreterContext context) throws IOException {
    String[] functions = this.tbenv.listUserDefinedFunctions();
    context.out.write(
            "%table function\n" + StringUtils.join(functions, "\n") + "\n");
  }

  private void callUseDatabase(String database)  {
    this.tbenv.setDefaultDatabase(database.split("\\."));
  }

  private void callDescribe(String table, InterpreterContext context) throws IOException {
    TableSchema schema = this.tbenv.scan(table.split("\\.")).getSchema();
    //TODO(zjffdu) to table style
    context.out.write(schema.toString() + "\n");
  }

  private void callExplain(String sql, InterpreterContext context) throws IOException {
    Table table = this.tbenv.sqlQuery(sql);
    context.out.write(this.tbenv.explain(table) + "\n");
  }

  public abstract void callSelect(String sql, InterpreterContext context) throws IOException;

  private void callInsertInto(String sql,
                              InterpreterContext context) throws IOException {
    this.tbenv.sqlUpdate(sql);

    JobGraph jobGraph = createJobGraph(sql);
    jobGraph.addJar(new Path(flinkInterpreter.getInnerScalaInterpreter().getFlinkILoop()
            .writeFilesToDisk().getAbsoluteFile().toURI()));
    SqlJobRunner jobRunner =
            new SqlJobRunner(flinkInterpreter.getInnerIntp().getCluster(), jobGraph, sql,
                    flinkInterpreter.getFlinkScalaShellLoader());
    jobRunner.run();
    context.out.write("Insert Succeeded.\n");
  }

  private FlinkPlan createPlan(String name) {
    StreamGraph graph = tbenv.generateStreamGraph();
    graph.setJobName(name);
    return graph;
  }

  public JobGraph createJobGraph(String name) {
    final FlinkPlan plan = createPlan(name);
    return ClusterClient.getJobGraph(
            flinkInterpreter.getFlinkConfiguration(),
            plan,
            new ArrayList<>(),
            new ArrayList<>(),
            SavepointRestoreSettings.none());
  }

  private void callCreateTable(String sql, InterpreterContext context) throws IOException {
    List<SqlNodeInfo> sqlNodeList = SqlJobUtil.parseSqlContext(tbenv, sql);
    sqlNodeList
            .stream()
            .filter((node) -> node.getSqlNode() instanceof SqlCreateTable)
            .forEach((node) -> SqlJobUtil.registerExternalTable(tbenv, node));
    context.out.write(CliStrings.MESSAGE_TABLE_CREATE + "\n");

  }

  private void callDropTable(String sql, InterpreterContext context) throws IOException {
    List<SqlNodeInfo> sqlNodeList = SqlJobUtil.parseSqlContext(tbenv, sql);
    sqlNodeList
            .stream()
            .filter((node) -> node.getSqlNode() instanceof SqlDropTable)
            .forEach((node) -> {
              SqlDropTable sqlDropTable = (SqlDropTable) node.getSqlNode();
              String funcName = sqlDropTable.getTableName().toString();
              tbenv.dropTable(funcName.split("."), sqlDropTable.getIfExists());
            });
    context.out.write(CliStrings.MESSAGE_TABLE_REMOVED + "\n");
  }

  private void callCreateView(String sql, InterpreterContext context) throws IOException {
    List<SqlNodeInfo> sqlNodeList = SqlJobUtil.parseSqlContext(tbenv, sql);
    sqlNodeList
            .stream()
            .filter((node) -> node.getSqlNode() instanceof SqlCreateView)
            .forEach((node) -> SqlJobUtil.registerExternalView(tbenv, node));
    context.out.write(CliStrings.MESSAGE_VIEW_CREATED + "\n");
  }

  private void callDropView(String sql, InterpreterContext context) throws IOException {
    List<SqlNodeInfo> sqlNodeList = SqlJobUtil.parseSqlContext(tbenv, sql);
    sqlNodeList
            .stream()
            .filter((node) -> node.getSqlNode() instanceof SqlDropView)
            .forEach((node) -> {
              SqlDropView sqlDropView = (SqlDropView) node.getSqlNode();
              String viewName = sqlDropView.getViewName().toString();
              tbenv.dropTable(viewName.split("."), sqlDropView.getIfExists());
            });
    context.out.write(CliStrings.MESSAGE_VIEW_REMOVED + "\n");
  }

  private void callCreateFunction(String sql, InterpreterContext context) throws IOException {
    List<SqlNodeInfo> sqlNodeList = SqlJobUtil.parseSqlContext(tbenv, sql);
    sqlNodeList
            .stream()
            .filter((node) -> node.getSqlNode() instanceof SqlCreateFunction)
            .forEach((node) -> {
              SqlCreateFunction sqlCreateFunction = (SqlCreateFunction) node.getSqlNode();
              String funcName = sqlCreateFunction.getFunctionName().toString();
              String funcDef = sqlCreateFunction.getClassName();
              // ClassLoader may not be correct
              tbenv.registerFunction(funcName,
                      createUserDefinedFunction(Thread.currentThread().getContextClassLoader(),
                              funcName,
                              funcDef));
            });
    context.out.write(CliStrings.MESSAGE_FUNCTION_CREATE + "\n");
  }

  /**
   * Create user defined function.
   */
  private static UserDefinedFunction createUserDefinedFunction(ClassLoader classLoader,
                                                               String funcName,
                                                               String funcDef) {
    DescriptorProperties properties = new DescriptorProperties();
    properties.putString("name", funcName);
    properties.putString("from", "class");
    properties.putString("class", funcDef);
    final FunctionDescriptor desc = FunctionEntry.create(properties).getDescriptor();
    return FunctionService.createFunction(desc, classLoader, false);
  }

  private void callDropFunction(String sql, InterpreterContext context) throws IOException {
    List<SqlNodeInfo> sqlNodeList = SqlJobUtil.parseSqlContext(tbenv, sql);
    sqlNodeList
            .stream()
            .filter((node) -> node.getSqlNode() instanceof SqlDropFunction)
            .forEach((node) -> {
              SqlDropFunction sqlDropFunction = (SqlDropFunction) node.getSqlNode();
              String funcName = sqlDropFunction.getFunctionName().toString();
              tbenv.dropFunction(funcName, sqlDropFunction.getIfExists());
            });
    context.out.write(CliStrings.MESSAGE_FUNCTION_REMOVED + "\n");
  }

  private void callCreateDatabase(String sql, InterpreterContext context) throws IOException {
    List<SqlNodeInfo> sqlNodeList = SqlJobUtil.parseSqlContext(tbenv, sql);
    sqlNodeList
            .stream()
            .filter((node) -> node.getSqlNode() instanceof SqlCreateDatabase)
            .forEach((node) -> SqlJobUtil.registerExternalDatabase(tbenv, node));
    context.out.write(CliStrings.MESSAGE_DATABASE_CREATED + "\n");
  }

  private void callDropDatabase(String database, InterpreterContext context) throws IOException {
    tbenv.dropDatabase(database.split("."), true);
    context.out.write(CliStrings.MESSAGE_FUNCTION_REMOVED + "\n");
  }
}
