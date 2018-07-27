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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This thread sends paragraph's append-data periodically. It handles append-data
 * for all paragraphs across all notebooks.
 */
public class AppendOutputThread extends Thread {

  private static final Logger LOGGER = LoggerFactory.getLogger(AppendOutputThread.class);
  public static final Long BUFFER_TIME_MS = new Long(100);

  private final BlockingQueue<AppendOutputBuffer> queue = new LinkedBlockingQueue<>();
  private final RemoteInterpreterProcessListener listener;

  public AppendOutputThread(RemoteInterpreterProcessListener listener) {
    this.listener = listener;
  }

  @Override
  public void run() {
    while(!Thread.interrupted()) {
      try {
        AppendOutputBuffer buffer = queue.take();
        listener.onOutputAppend(buffer.getNoteId(), buffer.getParagraphId(), buffer.getIndex(),
            buffer.getData());
      } catch (InterruptedException e) {
        LOGGER.warn("AppendOutputThread is interrupted", e);
        break;
      }
    }
  }

  public void appendBuffer(String noteId, String paragraphId, int index, String outputToAppend) {
    queue.offer(new AppendOutputBuffer(noteId, paragraphId, index, outputToAppend));
  }

}
