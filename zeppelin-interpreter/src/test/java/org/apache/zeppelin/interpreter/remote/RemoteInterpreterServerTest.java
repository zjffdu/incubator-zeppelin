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

package org.apache.zeppelin.interpreter.remote;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.thrift.TException;
import org.apache.zeppelin.display.GUI;
import org.apache.zeppelin.interpreter.*;
import org.apache.zeppelin.interpreter.thrift.RemoteInterpreterContext;
import org.apache.zeppelin.interpreter.thrift.RemoteInterpreterResult;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class RemoteInterpreterServerTest {

  private static Logger LOGGER = LoggerFactory.getLogger(RemoteInterpreterServerTest.class);
  private RemoteInterpreterServer server;
  private boolean running = false;

  @Before
  public void setUp() throws Exception {
    server = new RemoteInterpreterServer(
        RemoteInterpreterUtils.findRandomAvailablePortOnAllLocalInterfaces(), true);
    assertEquals(false, server.isRunning());
    LOGGER.info("Starting RemoteInterpreterServer");
    server.start();
    long startTime = System.currentTimeMillis();

    while (System.currentTimeMillis() - startTime < 10 * 1000) {
      if (server.isRunning()) {
        running = true;
        break;
      } else {
        Thread.sleep(200);
      }
    }
    assertEquals(true, running);
    assertEquals(true, RemoteInterpreterUtils.checkIfRemoteEndpointAccessible("localhost", server.getPort()));
  }

  @After
  public void tearDown() throws Exception {
    LOGGER.info("Shutting down RemoteInterpreterServer");
    server.shutdown();
    long startTime = System.currentTimeMillis();
    while (System.currentTimeMillis() - startTime < 10 * 1000) {
      if (server.isRunning()) {
        Thread.sleep(200);
      } else {
        running = false;
        break;
      }
    }
    assertEquals(false, running);
  }

  class ShutdownRun implements Runnable {
    private RemoteInterpreterServer serv = null;
    public ShutdownRun(RemoteInterpreterServer serv) {
      this.serv = serv;
    }
    @Override
    public void run() {
      try {
        serv.shutdown();
      } catch (Exception ex) {};
    }
  };

  @Test
  public void testStartStopWithQueuedEvents() throws InterruptedException, IOException, TException {
    RemoteInterpreterServer server = new RemoteInterpreterServer(
        RemoteInterpreterUtils.findRandomAvailablePortOnAllLocalInterfaces());
    assertEquals(false, server.isRunning());

    server.start();
    long startTime = System.currentTimeMillis();
    boolean running = false;

    while (System.currentTimeMillis() - startTime < 10 * 1000) {
      if (server.isRunning()) {
        running = true;
        break;
      } else {
        Thread.sleep(200);
      }
    }

    assertEquals(true, running);
    assertEquals(true, RemoteInterpreterUtils.checkIfRemoteEndpointAccessible("localhost", server.getPort()));

    //just send an event on the client queue
    server.eventClient.onAppStatusUpdate("","","","");

    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    Runnable task = new ShutdownRun(server);

    executor.schedule(task, 0, TimeUnit.MILLISECONDS);

    while (System.currentTimeMillis() - startTime < 10 * 1000) {
      if (server.isRunning()) {
        Thread.sleep(200);
      } else {
        running = false;
        break;
      }
    }

    executor.shutdown();

    //cleanup environment for next tests
    server.shutdown();

    assertEquals(false, running);
  }

  @Test
  public void testInterpreter() throws TException {
    Map<String, String> properties = new HashMap<>();
    properties.put("prop_1", "value_1");
    // always set zeppelin.interpreter.localRepo to void NPE
    properties.put("zeppelin.interpreter.localRepo", ".");

    // create interpreter
    server.createInterpreter("group_1", "session_1",
        "org.apache.zeppelin.interpreter.EchoInterpreter", properties, "user_1");

    InterpreterGroup interpreterGroup = server.getInterpreterGroup();
    assertEquals("group_1", interpreterGroup.getId());
    assertEquals(properties, interpreterGroup.getProperty());
    assertTrue(interpreterGroup.containsKey("session_1"));
    assertEquals(1, interpreterGroup.get("session_1").size());
    assertEquals(LazyOpenInterpreter.class, interpreterGroup.get("session_1").get(0).getClass());

    EchoInterpreter echoInterpreter = (EchoInterpreter)
        ((LazyOpenInterpreter) interpreterGroup.get("session_1").get(0)).getInnerInterpreter();
    assertEquals(properties, echoInterpreter.getProperty());
    assertEquals("user_1", echoInterpreter.getUserName());
    // open is not called until interpreter method is called.
    assertFalse(echoInterpreter.openCalled);

    // call interpret method
    RemoteInterpreterContext interpreterContext = new RemoteInterpreterContext();
    interpreterContext.setRunners("[]");
    interpreterContext.setGui(new GUI().toJson());

    RemoteInterpreterResult result = server.interpret("session_1",
        "org.apache.zeppelin.interpreter.EchoInterpreter", "dummy", interpreterContext);
    assertEquals(InterpreterResult.Code.SUCCESS.toString(), result.code);
    assertEquals("dummy", result.getMsg().get(0).getData());
    assertTrue(echoInterpreter.openCalled);

    try {
      server.interpret("not_existed_session",
          "org.apache.zeppelin.interpreter.EchoInterpreter", "dummy", interpreterContext);
      fail("should fail due to not existed session");
    } catch (TException e) {
      assertTrue(e.getMessage().contains(
          "Interpreter org.apache.zeppelin.interpreter.EchoInterpreter not initialized"));
    }

    // check other interpreter methods
    assertEquals(Interpreter.FormType.NATIVE, echoInterpreter.getFormType());
    assertEquals(10,
        server.getProgress("session_1", "org.apache.zeppelin.interpreter.EchoInterpreter",
            interpreterContext));
    server.cancel("session_1", "org.apache.zeppelin.interpreter.EchoInterpreter",
        interpreterContext);
    assertTrue(echoInterpreter.canceledCalled);

    // close interpreter
    server.close("session_1", "org.apache.zeppelin.interpreter.EchoInterpreter");
    assertTrue(echoInterpreter.closeCalled);
    assertEquals(0, interpreterGroup.get("session_1").size());
  }

}
