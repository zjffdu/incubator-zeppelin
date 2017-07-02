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

package org.apache.zeppelin.python;

import org.apache.zeppelin.display.GUI;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterOutput;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion;
import org.apache.zeppelin.user.AuthenticationInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;


public class IPython3InterpreterTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(IPython3InterpreterTest.class);
  private IPythonInterpreter interpreter;

  @Before
  public void setUp() {
    Properties properties = new Properties();
    interpreter = new IPythonInterpreter(properties);
    interpreter.open();
  }

  @After
  public void close() {
    interpreter.close();
  }


  @Test
  public void testPython3() {
    if (!interpreter.getPythonVersion().equals("3")) {
      LOGGER.warn("Skip test because it is not python3");
      return;
    }
    InterpreterResult result = interpreter.interpret("print('hello world')", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertEquals("hello world\n", result.message().get(0).getData());

    // assignment
    result = interpreter.interpret("abc=1", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertTrue(result.message().isEmpty());

    // if block
    result = interpreter.interpret("if abc > 0:\n\tprint('True')\nelse:\n\tprint('False')", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertEquals("True\n", result.message().get(0).getData());

    // for loop
    result = interpreter.interpret("for i in range(3):\n\tprint(i)", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertEquals("0\n1\n2\n", result.message().get(0).getData());

    // syntax error
    result = interpreter.interpret("print(unknown)", getInterpreterContext());
    assertEquals(InterpreterResult.Code.ERROR, result.code());
    assertTrue(result.message().get(0).getData().contains("name 'unknown' is not defined"));

    // ZEPPELIN-1133
    result = interpreter.interpret("def greet(name):\n" +
        "    print('Hello', name)\n" +
        "greet('Jack')", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertEquals("Hello Jack\n",result.message().get(0).getData());

    // ZEPPELIN-1114
    result = interpreter.interpret("print('there is no Error: ok')", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertEquals("there is no Error: ok\n",result.message().get(0).getData());

    // complete
    List<InterpreterCompletion> completions = interpreter.completion("ab", 2, getInterpreterContext());
    assertEquals(2, completions.size());
    assertEquals("abc", completions.get(0).getValue());
    assertEquals("abs", completions.get(1).getValue());

    // ipython help
    result = interpreter.interpret("range?", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertTrue(result.message().get(0).getData().contains("Return an object that produces a sequence of"));

    result = interpreter.interpret("%timeit range(100)", getInterpreterContext());
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    LOGGER.info(result.message().get(0).getData());
    assertTrue(result.message().get(0).getData().contains("loops"));
  }


  private InterpreterContext getInterpreterContext() {
    return new InterpreterContext(
        "noteId",
        "paragraphId",
        "replName",
        "paragraphTitle",
        "paragraphText",
        new AuthenticationInfo(),
        new HashMap<String, Object>(),
        new GUI(),
        null,
        null,
        null,
        new InterpreterOutput(null));
  }
}
