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

import org.apache.flink.api.common.JobID;


import org.apache.flink.api.scala.ExecutionEnvironment;

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class JobManager {

  private static Logger LOGGER = LoggerFactory.getLogger(JobManager.class);

  private Map<String, JobID> jobs = new HashMap<>();
  private Map<String, String> savePointMap = new HashMap<>();

  private ExecutionEnvironment env;
  private StreamExecutionEnvironment senv;
  private FlinkZeppelinContext z;

  public JobManager(ExecutionEnvironment env,
                    StreamExecutionEnvironment senv,
                    FlinkZeppelinContext z) {
    this.env = env;
    this.senv = senv;
    this.z = z;
  }

  public void addJob(String paragraphId, JobID jobId) {
    JobID previousJobId = this.jobs.put(paragraphId, jobId);
    if (previousJobId != null) {
      LOGGER.warn("There's another Job {} that is associated with paragraph {}",
              jobId, paragraphId);
    }
  }

  public void remoteJob(String paragraphId) {
    this.jobs.remove(paragraphId);
  }

  public void cancelJob(InterpreterContext context) {
    JobID jobId = this.jobs.remove(context.getParagraphId());
    if (jobId == null) {
      LOGGER.warn("Unable to remove Job from paragraph {}", context.getParagraphId());
      return;
    }

    if (Boolean.parseBoolean(
            context.getLocalProperties().getOrDefault("enableSavePoint", "false"))) {
      try {
        String savePointPath = this.senv.cancelWithSavepoint(jobId.toString(), null);
        this.savePointMap.put(context.getParagraphId(), savePointPath);
        Map<String, String> config = new HashMap<>();
        config.put("savepointPath", savePointPath);
        z.updateParagraphConfig(context.getNoteId(), context.getParagraphId(), config);
      } catch (Exception e) {
        LOGGER.warn(String.format("Fail to cancel job %s that is associated with paragraph %s",
                jobId, context.getParagraphId()), e);
      }
    } else {
      try {
        this.env.cancel(jobId);
      } catch (Exception e) {
        LOGGER.warn(String.format("Fail to cancel job %s that is associated with paragraph %s",
                jobId, context.getParagraphId()), e);
      }
    }
  }

  public String getSavePointPath(String paragraphId) {
    return savePointMap.get(paragraphId);
  }
}
