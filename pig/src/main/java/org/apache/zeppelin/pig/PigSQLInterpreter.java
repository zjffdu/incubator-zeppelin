/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.pig;

import org.apache.commons.lang3.StringUtils;
import org.apache.pig.PigServer;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Pig Interpreter which support Spark SQL
 */
public class PigSQLInterpreter extends BasePigInterpreter {

  private static Logger LOGGER = LoggerFactory.getLogger(PigSQLInterpreter.class);

  private SQLContext sqlContext;

  public PigSQLInterpreter(Properties property) {
    super(property);
  }

  @Override
  public void open() {

  }

  @Override
  public void close() {

  }

  @Override
  public InterpreterResult interpret(String st, InterpreterContext context) {
    if (sqlContext == null) {
      SparkContext sc = SparkContext.getOrCreate();
      sqlContext = SQLContext.getOrCreate(sc);
    }
    DataFrame df = sqlContext.sql(st);
    StructType sType = df.schema();
    StringBuilder builder = new StringBuilder();
    builder.append(StringUtils.join(sType.fieldNames(), "\t"));
    builder.append("\n");
    for (Row row : df.take(1000)) {
      for (int i = 0; i < row.size(); ++i) {
        builder.append(row.get(i) + "\t");
      }
      builder.append("\n");
    }

    return new InterpreterResult(
        InterpreterResult.Code.SUCCESS,
        InterpreterResult.Type.TABLE,
        builder.toString());
  }

  @Override
  public PigServer getPigServer() {
    return null;
  }
}
