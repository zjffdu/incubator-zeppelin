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

package org.apache.zeppelin.flink;

import org.apache.zeppelin.display.AngularObjectRegistry;

import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterGroup;
import org.apache.zeppelin.interpreter.InterpreterOutput;
import org.apache.zeppelin.interpreter.InterpreterOutputListener;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResultMessageOutput;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FlinkBatchSqlInterpreterTest {

  private FlinkInterpreter interpreter;
  private FlinkBatchSqlInterpreter sqlInterpreter;
  private InterpreterContext context;

  // catch the streaming output in onAppend
  private volatile String output = "";
  // catch the interpreter output in onUpdate
  private InterpreterResultMessageOutput messageOutput;

  @Before
  public void setUp() throws InterpreterException {
    Properties p = new Properties();
    interpreter = new FlinkInterpreter(p);
    sqlInterpreter = new FlinkBatchSqlInterpreter(p);
    InterpreterGroup intpGroup = new InterpreterGroup();
    interpreter.setInterpreterGroup(intpGroup);
    sqlInterpreter.setInterpreterGroup(intpGroup);
    intpGroup.addInterpreterToSession(interpreter, "session_1");
    intpGroup.addInterpreterToSession(sqlInterpreter, "session_1");

    interpreter.open();
    sqlInterpreter.open();
    context = InterpreterContext.builder().build();
  }

  @After
  public void tearDown() throws InterpreterException {
    interpreter.close();
  }

  @Test
  public void testSQLInterpreter() throws InterpreterException {
    InterpreterResult result = interpreter.interpret(
        "val ds = benv.fromElements((1, \"jeff\"), (2, \"andy\"))", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());

    result = interpreter.interpret("btenv.registerDataSet(\"table_1\", ds)",
        getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());

    result = sqlInterpreter.interpret("select * from table_1", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertEquals(InterpreterResult.Type.TABLE, result.message().get(0).getType());
    assertEquals("_1\t_2\n" +
        "1\tjeff\n" +
        "2\tandy\n", result.message().get(0).getData());
  }

  @Test
  public void testInvalidTable() throws InterpreterException {
    InterpreterResult result = sqlInterpreter.interpret("select * from invalid_table",
            getInterpreterContext());
    assertEquals(InterpreterResult.Code.ERROR, result.code());
    assertTrue(result.message().get(0).getData().contains("Object 'invalid_table' not found"));
  }

  private InterpreterContext getInterpreterContext() {
    output = "";
    InterpreterContext context = InterpreterContext.builder()
        .setInterpreterOut(new InterpreterOutput(null))
        .setAngularObjectRegistry(new AngularObjectRegistry("flink", null))
        .build();
    context.out = new InterpreterOutput(
        new InterpreterOutputListener() {
          @Override
          public void onUpdateAll(InterpreterOutput out) {

          }

          @Override
          public void onAppend(int index, InterpreterResultMessageOutput out, byte[] line) {
            try {
              output = out.toInterpreterResultMessage().getData();
            } catch (IOException e) {
              e.printStackTrace();
            }
          }

          @Override
          public void onUpdate(int index, InterpreterResultMessageOutput out) {
            messageOutput = out;
          }
        });
    return context;
  }
}
