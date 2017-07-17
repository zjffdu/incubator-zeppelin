///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//
//package org.apache.zeppelin.spark;
//
//
//import com.google.common.io.Files;
//import org.apache.zeppelin.display.AngularObjectRegistry;
//import org.apache.zeppelin.display.GUI;
//import org.apache.zeppelin.interpreter.Interpreter;
//import org.apache.zeppelin.interpreter.InterpreterContext;
//import org.apache.zeppelin.interpreter.InterpreterGroup;
//import org.apache.zeppelin.interpreter.InterpreterOutput;
//import org.apache.zeppelin.interpreter.InterpreterOutputListener;
//import org.apache.zeppelin.interpreter.InterpreterResult;
//import org.apache.zeppelin.interpreter.InterpreterResultMessageOutput;
//import org.apache.zeppelin.user.AuthenticationInfo;
//import org.junit.Test;
//
//import java.util.HashMap;
//import java.util.LinkedList;
//import java.util.Properties;
//
//import static org.junit.Assert.assertEquals;
//
//public class DepInterpreterTest {
//
//  @Test
//  public void testDepInterpreter() {
//    Properties properties = new Properties();
//    properties.setProperty("spark.master", "local");
//    properties.setProperty("spark.app.name", "test");
//    properties.setProperty("zeppelin.spark.maxResult", "100");
//    properties.setProperty("zeppelin.dep.localrepo", Files.createTempDir().getAbsolutePath());
//    InterpreterGroup intpGroup = new InterpreterGroup();
//
//    SparkInterpreter interpreter = new SparkInterpreter(properties);
//    interpreter.setInterpreterGroup(intpGroup);
//
//    DepInterpreter depInterpreter = new DepInterpreter(properties);
//    depInterpreter.setInterpreterGroup(intpGroup);
//
//    intpGroup.put("session_1", new LinkedList<Interpreter>());
//    intpGroup.get("session_1").add(interpreter);
//    intpGroup.get("session_1").add(depInterpreter);
//
//    // open DepInterpreter first
//    depInterpreter.open();
//    InterpreterResult result = depInterpreter.interpret("z.load(\"com.databricks:spark-avro_2.11:3.2.0\")", getInterpreterContext());
//    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
//
//    interpreter.open();
//    result = interpreter.interpret("import com.databricks.spark.avro._", getInterpreterContext());
//    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
//  }
//
//  private InterpreterContext getInterpreterContext() {
//    return new InterpreterContext(
//        "noteId",
//        "paragraphId",
//        "replName",
//        "paragraphTitle",
//        "paragraphText",
//        new AuthenticationInfo(),
//        new HashMap<String, Object>(),
//        new GUI(),
//        new AngularObjectRegistry("spark", null),
//        null,
//        null,
//        new InterpreterOutput(
//
//            new InterpreterOutputListener() {
//              @Override
//              public void onUpdateAll(InterpreterOutput out) {
//
//              }
//
//              @Override
//              public void onAppend(int index, InterpreterResultMessageOutput out, byte[] line) {
//
//              }
//
//              @Override
//              public void onUpdate(int index, InterpreterResultMessageOutput out) {
//
//              }
//            })
//    );
//  }
//}
