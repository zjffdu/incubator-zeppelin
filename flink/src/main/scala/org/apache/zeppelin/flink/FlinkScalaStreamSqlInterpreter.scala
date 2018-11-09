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

package org.apache.zeppelin.flink

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.datastream.DataStreamUtils
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row
import org.apache.zeppelin.interpreter.{InterpreterContext, InterpreterResult}

class FlinkScalaStreamSqlInterpreter(scalaInterpreter: FlinkScalaInterpreter,
                                     z: FlinkZeppelinContext,
                                     maxRow: Int) {

  private val stEnv: StreamTableEnvironment = scalaInterpreter.getStreamTableEnvionment()

  def interpret(code: String, context: InterpreterContext): InterpreterResult = {
    try {
      val table: Table = this.stEnv.sqlQuery(code)
      if (context.getLocalProperties.getOrDefault("stream_type", "append").equals("append")) {
        val iter = DataStreamUtils.collect(table.toAppendStream[Row].javaStream)
        context.out.write("%table\n")
        val columnsNames = table.getSchema.getFieldNames
        context.out.write(columnsNames.mkString("\t"))
        context.out.write("\n")

        while (iter.hasNext) {
          val row = iter.next()
          context.out.write(toSeq(row).mkString("\t") + "\n")
          Thread.sleep(1000)
        }
      }
      return new InterpreterResult(InterpreterResult.Code.SUCCESS, "")
    } catch {
      case e: Exception =>
        return new InterpreterResult(InterpreterResult.Code.ERROR,
          "Fail to fetch result: " + e.getMessage)
    }
  }

  private def toSeq(row: Row): Seq[Any] = {
    val n = row.getArity
    val values = new Array[Any](n)
    var i = 0
    while (i < n) {
      values.update(i, row.getField(i))
      i += 1
    }
    values.toSeq
  }
}
