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

import org.apache.thrift.TException;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.thrift.ParagraphInfo;
import org.apache.zeppelin.interpreter.thrift.ServiceException;
import org.apache.zeppelin.user.AuthenticationInfo;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Event from RemoteInterpreterProcess
 */
public interface RemoteInterpreterProcessListener {
  /**
   * Called when output is appended from RemoteInterpreterProcess. This is used for streaming output.
   *
   * @param noteId
   * @param paragraphId
   * @param index
   * @param output
   */
  void onOutputAppend(String noteId, String paragraphId, int index, String output);

  /**
   *
   * @param noteId
   * @param paragraphId
   * @param index
   * @param type
   * @param output
   */
  void onOutputUpdated(
      String noteId, String paragraphId, int index, InterpreterResult.Type type, String output);

  /**
   * Clear paragraph output of frontend
   * @param noteId
   * @param paragraphId
   */
  void onOutputClear(String noteId, String paragraphId);

  /**
   * It is used by ZeppelinContext to run paragraphs pragmatically. Either paragraphIndices
   * or paragraphIds is empty.
   *
   * @param noteId
   * @param paragraphIndices
   * @param paragraphIds
   * @param curParagraphId
   * @throws IOException
   */
  void runParagraphs(String noteId, List<Integer> paragraphIndices, List<String> paragraphIds,
                     String curParagraphId)
      throws IOException;

  /**
   * This is called by interpreter implementation to send meta info to frontend, such as spark job url
   * for spark interpreter.
   *
   * @param noteId
   * @param paragraphId
   * @param interpreterSettingId
   * @param metaInfos
   */
  void onParaInfosReceived(String noteId, String paragraphId,
                                  String interpreterSettingId, Map<String, String> metaInfos);

  /**
   * This is called by interpreter implementation to get paragraph info.
   *
   * @param user
   * @param noteId
   * @return
   * @throws TException
   * @throws ServiceException
   */
  List<ParagraphInfo> getParagraphList(String user, String noteId) throws TException, ServiceException;
}
