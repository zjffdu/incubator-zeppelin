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
package org.apache.zeppelin.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 *
 */
public class ExecutorFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorFactory.class);
  private static ExecutorFactory instance;

  private Map<String, ExecutorService> executor = new HashMap<>();

  private ExecutorFactory() {

  }

  public static ExecutorFactory singleton() {
    if (instance == null) {
      synchronized (ExecutorFactory.class) {
        if (instance == null) {
          instance = new ExecutorFactory();
        }
      }
    }
    return instance;
  }

  public ExecutorService getOrCreate(String name) {
    return getOrCreate(name, 100);
  }

  public ExecutorService getOrCreate(String name, int numThread) {
    synchronized (executor) {
      if (!executor.containsKey(name)) {
        executor.put(name, Executors.newScheduledThreadPool(numThread));
      }
      return executor.get(name);
    }
  }

  public void shutdown(String name) {
    synchronized (executor) {
      if (executor.containsKey(name)) {
        ExecutorService e = executor.get(name);
        e.shutdown();
        executor.remove(name);
      } else {
        LOGGER.warn("Try to shutdown an unknown executor: " + name);
      }
    }
  }


  public void shutdownAll() {
    synchronized (executor) {
      for (String name : executor.keySet()) {
        shutdown(name);
      }
    }
  }
}
