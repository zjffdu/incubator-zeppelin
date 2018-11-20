package com.alibaba.blink.benchmark.blinkperf;

import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public abstract class Benchmark {
  private static final Logger LOG = LoggerFactory.getLogger(Benchmark.class);
  private final String name;
  private final String sqlQuery;
  private final int numIters;
  private StreamExecutionEnvironment env;

  public Benchmark(StreamExecutionEnvironment env, String name, String sqlQuery, int numIters) {
    this.env = env;
    this.name = name;
    this.sqlQuery = sqlQuery;
    this.numIters = numIters;
    Preconditions.checkArgument(numIters > 0);
  }

  abstract void registerTables(TableEnvironment tEnv);

  public void run(List<Tuple2<String, Long>> bestArray) throws Exception {
    List<Result> results = new ArrayList<>();
    for (int i = 0; i < numIters; ++i) {
      System.err.println(
              String.format("--------------- Running %s %s/%s ---------------",
                      name, (i + 1), numIters));
      results.add(runInternal());
    }
    printResults(results, bestArray);
  }

  private Result runInternal() throws Exception {
    System.gc();  // ensures garbage from previous cases don't impact this one

    env.setMultiHeadChainMode(true);
    TableEnvironment tEnv = TableEnvironment.getBatchTableEnvironment(env);

    LOG.info("begin register tables.");
    registerTables(tEnv);

    long startTime = System.currentTimeMillis();
    LOG.info(" begin optimize.");

    tEnv.getConfig().setTimeZone(DateTimeUtils.UTC_ZONE);
    Table table = tEnv.sqlQuery(sqlQuery);
    long executingTime;

    long endOptimizing = System.currentTimeMillis();
    LOG.info(" begin execute.");
    table.print(name);
    long endExecutingTime = System.currentTimeMillis();
    executingTime = endExecutingTime - endOptimizing;
    LOG.info(" end execute");

    System.out.println();

    long endTime = System.currentTimeMillis();
    long optimizingTime = endOptimizing - startTime;
    long totalTime = endTime - startTime;

    LOG.info("total execute: " + executingTime + "ms.");
    System.out.println("total execute " + executingTime + "ms.");

    return new Result(optimizingTime, executingTime, totalTime);
  }

  private void printResults(List<Result> results,
                            List<Tuple2<String, Long>> bestArray) throws Exception {
    int itemMaxLength = 20;
    System.err.println();
    printLine('-', "+", itemMaxLength, "", "", "", "");
    printLine(' ', "|", itemMaxLength, " " + name, " Best Time(ms)",
            " Avg Time(ms)", " Max Time(ms)");
    printLine('-', "+", itemMaxLength, "", "", "", "");

    Tuple3<Long, Long, Long> t3 = getBestAvgMaxTime(results, "getSqlOptimizingTime");
    printLine(' ', "|", itemMaxLength, " SQL Optimizing", " " + t3.f0, " " + t3.f1, " " + t3.f2);
    printLine('-', "+", itemMaxLength, "", "", "", "");

    t3 = getBestAvgMaxTime(results, "getExecutingTime");
    printLine(' ', "|", itemMaxLength, " Executing", " " + t3.f0, " " + t3.f1, " " + t3.f2);
    printLine('-', "+", itemMaxLength, "", "", "", "");

    t3 = getBestAvgMaxTime(results, "getTotalTime");
    printLine(' ', "|", itemMaxLength, " Total", " " + t3.f0, " " + t3.f1, " " + t3.f2);
    printLine('-', "+", itemMaxLength, "", "", "", "");
    bestArray.add(new Tuple2<String, Long>(name, t3.f0));
    System.err.println();
  }

  private void printLine(char charToFill, String separator, int itemMaxLength, String... items) {
    StringBuilder builder = new StringBuilder();
    for (String item : items) {
      builder.append(separator);
      builder.append(item);
      int left = itemMaxLength - item.length() - separator.length();
      for (int i = 0; i < left; ++i) {
        builder.append(charToFill);
      }
    }
    builder.append(separator);
    System.err.println(builder.toString());
  }

  private Tuple3<Long, Long, Long> getBestAvgMaxTime(List<Result> results,
                                                     String methodName) throws Exception {
    long best = Long.MAX_VALUE;
    long sum = 0L;
    long max = Long.MIN_VALUE;
    Method method = Result.class.getMethod(methodName);
    for (Result result : results) {
      long time = (long) method.invoke(result);
      if (time < best) {
        best = time;
      }
      sum += time;
      if (time > max) {
        max = time;
      }
    }
    return new Tuple3<>(best, sum / results.size(), max);
  }

  private class Result {
    private final long sqlOptimizingTime;
    private final long executingTime;
    private final long totalTime;

    private Result(long sqlOptimizingTime, long executingTime, long totalTime) {
      this.sqlOptimizingTime = sqlOptimizingTime;
      this.executingTime = executingTime;
      this.totalTime = totalTime;
    }

    public long getSqlOptimizingTime() {
      return sqlOptimizingTime;
    }

    public long getExecutingTime() {
      return executingTime;
    }

    public long getTotalTime() {
      return totalTime;
    }
  }

}
