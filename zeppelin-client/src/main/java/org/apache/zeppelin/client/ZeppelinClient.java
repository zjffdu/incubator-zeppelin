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


package org.apache.zeppelin.client;

import kong.unirest.JsonNode;
import kong.unirest.Unirest;
import kong.unirest.json.JSONArray;
import kong.unirest.json.JSONObject;
import org.apache.commons.text.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Low level api for interacting with Zeppelin. Underneath, it use the zeppelin rest api.
 */
public class ZeppelinClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(ZeppelinClient.class);

  private ClientConfig clientConfig;

  public ZeppelinClient(ClientConfig clientConfig) {
    this.clientConfig = clientConfig;
  }

  /**
   * Get Zeppelin version.
   *
   * @return
   * @throws Exception
   */
  public String getVersion() throws Exception {
    JsonNode jsonNode = Unirest.get(clientConfig.getZeppelinRestUrl() + "/api/version").asJson().getBody();
    if (! "OK".equalsIgnoreCase(jsonNode.getObject().getString("status"))) {
      throw new Exception(StringEscapeUtils.unescapeJava(jsonNode.getObject().getString("message")));
    }
    return jsonNode.getObject().getJSONObject("body").getString("version");
  }

  /**
   * Request a new session id. It doesn't create session (interpreter process) in zeppelin server side, but just
   * create a unique session id.
   *
   * @param interpreter
   * @return
   * @throws Exception
   */
  public String newSession(String interpreter) throws Exception {
    JsonNode jsonNode = Unirest.post(clientConfig.getZeppelinRestUrl() + "/api/session/" + interpreter).asJson().getBody();
    if (! "OK".equalsIgnoreCase(jsonNode.getObject().getString("status"))) {
      throw new Exception(StringEscapeUtils.unescapeJava(jsonNode.getObject().getString("message")));
    }
    return jsonNode.getObject().getString("message");
  }

  /**
   * Stop the session(interpreter process) in zeppelin server.
   *
   * @param interpreter
   * @param sessionId
   * @throws Exception
   */
  public void stopSession(String interpreter, String sessionId) throws Exception {
    JsonNode jsonNode = Unirest.delete(
            clientConfig.getZeppelinRestUrl() + "/api/session/" + interpreter + "/" + sessionId)
            .asJson().getBody();
    if (! "OK".equalsIgnoreCase(jsonNode.getObject().getString("status"))) {
      throw new Exception(StringEscapeUtils.unescapeJava(jsonNode.getObject().getString("message")));
    }
  }

  /**
   * Get the session weburl.
   *
   * @param sessionId
   * @throws Exception
   */
  public String getSessionWebUrl(String sessionId) throws Exception {
    JsonNode jsonNode = Unirest.get(
            clientConfig.getZeppelinRestUrl() + "/api/session/" + sessionId)
            .asJson().getBody();
    if (! "OK".equalsIgnoreCase(jsonNode.getObject().getString("status"))) {
      throw new Exception(StringEscapeUtils.unescapeJava(jsonNode.getObject().getString("message")));
    }

    JSONObject bodyObject = jsonNode.getObject().getJSONObject("body");
    if (bodyObject.has("weburl")) {
      return bodyObject.getString("weburl");
    } else {
      return null;
    }
  }

  /**
   * Create a new empty note with specified notePath.
   *
   * @param notePath
   * @return
   * @throws Exception
   */
  public String createNote(String notePath) throws Exception {
    JSONObject bodyObject = new JSONObject();
    bodyObject.put("name", notePath);
    JsonNode jsonNode = Unirest.post(clientConfig.getZeppelinRestUrl() + "/api/notebook")
            .body(bodyObject.toString())
            .asJson()
            .getBody();
    if (! "OK".equalsIgnoreCase(jsonNode.getObject().getString("status"))) {
      throw new Exception(StringEscapeUtils.unescapeJava(jsonNode.getObject().getString("message")));
    }

    return jsonNode.getObject().getString("body");
  }

  /**
   * Delete note with provided noteId.
   *
   * @param noteId
   * @throws Exception
   */
  public void deleteNote(String noteId) throws Exception {
    JsonNode jsonNode = Unirest.delete(clientConfig.getZeppelinRestUrl() + "/api/notebook/" + noteId)
            .asJson()
            .getBody();
    if (! "OK".equalsIgnoreCase(jsonNode.getObject().getString("status"))) {
      throw new Exception(StringEscapeUtils.unescapeJava(jsonNode.getObject().getString("message")));
    }
  }

  /**
   * Query {@link NoteResult} with provided noteId.
   *
   * @param noteId
   * @return
   * @throws Exception
   */
  public NoteResult queryNoteResult(String noteId) throws Exception {
    JsonNode jsonNode = Unirest.get(clientConfig.getZeppelinRestUrl() + "/api/notebook/" + noteId)
            .asJson()
            .getBody();
    if (! "OK".equalsIgnoreCase(jsonNode.getObject().getString("status"))) {
      throw new Exception(StringEscapeUtils.unescapeJava(jsonNode.getObject().getString("message")));
    }

    JSONObject noteJsonObject = jsonNode.getObject().getJSONObject("body");
    boolean isRunning = false;
    if (noteJsonObject.has("info")) {
      JSONObject infoJsonObject = noteJsonObject.getJSONObject("info");
      if (infoJsonObject.has("isRunning")) {
        isRunning = Boolean.parseBoolean(infoJsonObject.getString("isRunning"));
      }
    }

    List<ParagraphResult> paragraphResultList = new ArrayList<>();
    if (noteJsonObject.has("paragraphs")) {
      JSONArray paragraphJsonArray = noteJsonObject.getJSONArray("paragraphs");
      for (int i = 0; i< paragraphJsonArray.length(); ++i) {
        paragraphResultList.add(new ParagraphResult(paragraphJsonArray.getJSONObject(i)));
      }
    }

    return new NoteResult(noteId, isRunning, paragraphResultList);
  }

  /**
   * Execute note with provided noteId, return until note execution is completed.
   *
   * @param noteId
   * @return
   * @throws Exception
   */
  public NoteResult executeNote(String noteId) throws Exception {
    return executeNote(noteId, new HashMap<>());
  }

  /**
   * Execute note with provided noteId and parameters, return until note execution is completed.
   *
   * @param noteId
   * @param parameters
   * @return
   * @throws Exception
   */
  public NoteResult executeNote(String noteId, Map<String, String> parameters) throws Exception {
    JSONObject bodyObject = new JSONObject();
    bodyObject.put("params", parameters);
    JsonNode jsonNode = Unirest
            .post(clientConfig.getZeppelinRestUrl() + "/api/notebook/job/" + noteId + "?blocking=true&isolated=true")
            .body(bodyObject.toString())
            .asJson()
            .getBody();
    if (! "OK".equalsIgnoreCase(jsonNode.getObject().getString("status"))) {
      throw new Exception(StringEscapeUtils.unescapeJava(jsonNode.getObject().getString("message")));
    }

    return queryNoteResult(noteId);
  }

  /**
   * Submit note to execution with provided noteId, return at once the submission is completed.
   * You need to query {@link NoteResult} by yourself afterwards until note execution is completed.
   *
   * @param noteId
   * @return
   * @throws Exception
   */
  public NoteResult submitNote(String noteId) throws Exception  {
    return submitNote(noteId, new HashMap<>());
  }

  /**
   * Submit note to execution with provided noteId and parameters, return at once the submission is completed.
   * You need to query {@link NoteResult} by yourself afterwards until note execution is completed.
   *
   * @param noteId
   * @param parameters
   * @return
   * @throws Exception
   */
  public NoteResult submitNote(String noteId, Map<String, String> parameters) throws Exception  {
    JSONObject bodyObject = new JSONObject();
    bodyObject.put("params", parameters);
    // run note in non-blocking and isolated way.
    JsonNode jsonNode = Unirest.post(clientConfig.getZeppelinRestUrl() +
            "/api/notebook/job/" + noteId + "?blocking=false&isolated=true")
            .body(bodyObject)
            .asJson()
            .getBody();
    LOGGER.info("Start to run note: " + noteId);
    String status = jsonNode.getObject().getString("status");
    if (!"OK".equalsIgnoreCase(status)) {
      throw new Exception(StringEscapeUtils.unescapeJava(jsonNode.getObject().getString("message")));
    }
    return queryNoteResult(noteId);
  }

  /**
   * Block there until note execution is completed.
   *
   * @param noteId
   * @return
   * @throws Exception
   */
  public NoteResult waitUntilNoteFinished(String noteId) throws Exception {
    while (true) {
      NoteResult noteResult = queryNoteResult(noteId);
      if (!noteResult.isRunning()) {
        return noteResult;
      }
      Thread.sleep(clientConfig.getQueryInterval());
    }
  }

  /**
   * Block there until note execution is completed, and throw exception if note execution is not completed
   * in <code>timeoutInMills</code>.
   *
   * @param noteId
   * @param timeoutInMills
   * @return
   * @throws Exception
   */
  public NoteResult waitUntilNoteFinished(String noteId, long timeoutInMills) throws Exception {
    long start = System.currentTimeMillis();
    while (true && (System.currentTimeMillis() - start) < timeoutInMills) {
      NoteResult noteResult = queryNoteResult(noteId);
      if (!noteResult.isRunning()) {
        return noteResult;
      }
      Thread.sleep(clientConfig.getQueryInterval());
    }
    throw new Exception("Note is not finished in " + timeoutInMills / 1000 + " seconds");
  }

  /**
   * Add paragraph to note with provided title and text.
   *
   * @param noteId
   * @param title
   * @param text
   * @return
   * @throws Exception
   */
  public String addParagraph(String noteId, String title, String text) throws Exception {
    JSONObject bodyObject = new JSONObject();
    bodyObject.put("title", title);
    bodyObject.put("text", text);
    JsonNode jsonNode = Unirest.post(clientConfig.getZeppelinRestUrl() + "/api/notebook/" + noteId + "/paragraph")
            .body(bodyObject.toString())
            .asJson()
            .getBody();
    if (! "OK".equalsIgnoreCase(jsonNode.getObject().getString("status"))) {
      throw new Exception(StringEscapeUtils.unescapeJava(jsonNode.getObject().getString("message")));
    }

    return jsonNode.getObject().getString("body");
  }

  /**
   * Update paragraph with specified title and text.
   *
   * @param noteId
   * @param paragraphId
   * @param title
   * @param text
   * @throws Exception
   */
  public void updateParagraph(String noteId, String paragraphId, String title, String text) throws Exception {
    JSONObject bodyObject = new JSONObject();
    bodyObject.put("title", title);
    bodyObject.put("text", text);
    JsonNode jsonNode = Unirest.put(clientConfig.getZeppelinRestUrl() + "/api/notebook/" + noteId + "/paragraph/" + paragraphId)
            .body(bodyObject.toString())
            .asJson()
            .getBody();
    if (! "OK".equalsIgnoreCase(jsonNode.getObject().getString("status"))) {
      throw new Exception(StringEscapeUtils.unescapeJava(jsonNode.getObject().getString("message")));
    }
  }

  /**
   * Execute paragraph with parameters in specified session. If sessionId is null or empty string, then it depends on
   * the interpreter binding mode of Note(e.g. isolated per note), otherwise it will run in the specified session.
   *
   * @param noteId
   * @param paragraphId
   * @param sessionId
   * @param parameters
   * @return
   * @throws Exception
   */
  public ParagraphResult executeParagraph(String noteId, String paragraphId, String sessionId, Map<String, String> parameters) throws Exception {
    JSONObject bodyObject = new JSONObject();
    bodyObject.put("params", parameters);
    JsonNode jsonNode = Unirest
            .post(clientConfig.getZeppelinRestUrl() + "/api/notebook/run/" + noteId + "/" + paragraphId)
            .queryString("sessionId", sessionId)
            .body(bodyObject.toString())
            .asJson()
            .getBody();
    if (! "OK".equalsIgnoreCase(jsonNode.getObject().getString("status"))) {
      throw new Exception(StringEscapeUtils.unescapeJava(jsonNode.getObject().getString("message")));
    }
    jsonNode = Unirest.get(
            clientConfig.getZeppelinRestUrl() + "/api/notebook/" + noteId + "/paragraph/" + paragraphId)
            .asJson()
            .getBody();
    if (! "OK".equalsIgnoreCase(jsonNode.getObject().getString("status"))) {
      throw new Exception("Fail to get paragraph: " + jsonNode.toPrettyString());
    }

    JSONObject paragraphJson = jsonNode.getObject().getJSONObject("body");
    return new ParagraphResult(paragraphJson);
  }

  /**
   * Execute paragraph with parameters.
   *
   * @param noteId
   * @param paragraphId
   * @param parameters
   * @return
   * @throws Exception
   */
  public ParagraphResult executeParagraph(String noteId, String paragraphId, Map<String, String> parameters) throws Exception {
    return executeParagraph(noteId, paragraphId, "", parameters);
  }

  /**
   * Execute paragraph in specified session. If sessionId is null or empty string, then it depends on
   * the interpreter binding mode of Note(e.g. isolated per note), otherwise it will run in the specified session.
   *
   * @param noteId
   * @param paragraphId
   * @param sessionId
   * @return
   * @throws Exception
   */
  public ParagraphResult executeParagraph(String noteId, String paragraphId, String sessionId) throws Exception {
    return executeParagraph(noteId, paragraphId, sessionId, new HashMap<>());
  }

  /**
   * Execute paragraph.
   *
   * @param noteId
   * @param paragraphId
   * @return
   * @throws Exception
   */
  public ParagraphResult executeParagraph(String noteId, String paragraphId) throws Exception {
    return executeParagraph(noteId, paragraphId, "", new HashMap<>());
  }

  /**
   *
   * @param noteId
   * @param paragraphId
   * @param sessionId
   * @param parameters
   * @return
   * @throws Exception
   */
  public ParagraphResult submitParagraph(String noteId, String paragraphId, String sessionId, Map<String, String> parameters) throws Exception {
    JSONObject bodyObject = new JSONObject();
    bodyObject.put("params", parameters);
    JsonNode jsonNode = Unirest.post(
            clientConfig.getZeppelinRestUrl() + "/api/notebook/job/" + noteId + "/" + paragraphId)
            .queryString("sessionId", sessionId)
            .body(bodyObject.toString())
            .asJson()
            .getBody();
    if (! "OK".equalsIgnoreCase(jsonNode.getObject().getString("status"))) {
      throw new Exception(StringEscapeUtils.unescapeJava(jsonNode.getObject().getString("message")));
    }
    return queryParagraphResult(noteId, paragraphId);
  }

  /**
   *
   * @param noteId
   * @param paragraphId
   * @param sessionId
   * @return
   * @throws Exception
   */
  public ParagraphResult submitParagraph(String noteId, String paragraphId, String sessionId) throws Exception {
    return submitParagraph(noteId, paragraphId, sessionId, new HashMap<>());
  }

  /**
   *
   * @param noteId
   * @param paragraphId
   * @param parameters
   * @return
   * @throws Exception
   */
  public ParagraphResult submitParagraph(String noteId, String paragraphId, Map<String, String> parameters) throws Exception {
    return submitParagraph(noteId, paragraphId, "", parameters);
  }

  /**
   *
   * @param noteId
   * @param paragraphId
   * @return
   * @throws Exception
   */
  public ParagraphResult submitParagraph(String noteId, String paragraphId) throws Exception {
    return submitParagraph(noteId, paragraphId, "", new HashMap<>());
  }

  /**
   * This used by {@link ZSession} for creating or reusing a paragraph for executing another piece of code.
   *
   * @param noteId
   * @param maxParagraph
   * @return
   * @throws Exception
   */
  public String nextSessionParagraph(String noteId, int maxParagraph) throws Exception {
    JsonNode jsonNode = Unirest
            .post(clientConfig.getZeppelinRestUrl() + "/api/notebook/" + noteId + "/paragraph/next")
            .queryString("maxParagraph", maxParagraph)
            .asJson()
            .getBody();
    if (! "OK".equalsIgnoreCase(jsonNode.getObject().getString("status"))) {
      throw new Exception(StringEscapeUtils.unescapeJava(jsonNode.getObject().getString("message")));
    }

    return jsonNode.getObject().getString("message");
  }

  /**
   * Cancel a running paragraph.
   *
   * @param noteId
   * @param paragraphId
   * @throws Exception
   */
  public void cancelParagraph(String noteId, String paragraphId) throws Exception {
    JsonNode jsonNode = Unirest.delete(
            clientConfig.getZeppelinRestUrl() + "/api/notebook/job/" + noteId + "/" + paragraphId)
            .asJson()
            .getBody();
    if (! "OK".equalsIgnoreCase(jsonNode.getObject().getString("status"))) {
      throw new Exception(StringEscapeUtils.unescapeJava(jsonNode.getObject().getString("message")));
    }
  }

  /**
   *
   * @param noteId
   * @param paragraphId
   * @return
   * @throws Exception
   */
  public ParagraphResult queryParagraphResult(String noteId, String paragraphId) throws Exception {
    JsonNode jsonNode = Unirest
            .get(clientConfig.getZeppelinRestUrl() + "/api/notebook/" + noteId + "/paragraph/" + paragraphId)
            .asJson()
            .getBody();
    if (! "OK".equalsIgnoreCase(jsonNode.getObject().getString("status"))) {
      throw new Exception(StringEscapeUtils.unescapeJava(jsonNode.getObject().getString("message")));
    }

    JSONObject paragraphJson = jsonNode.getObject().getJSONObject("body");
    return new ParagraphResult(paragraphJson);
  }

  /**
   *
   * @param noteId
   * @param paragraphId
   * @return
   * @throws Exception
   */
  public ParagraphResult waitUtilParagraphFinish(String noteId, String paragraphId) throws Exception {
    while (true) {
      ParagraphResult paragraphResult = queryParagraphResult(noteId, paragraphId);
      if (paragraphResult.getStatus().isCompleted()) {
        return paragraphResult;
      }
      Thread.sleep(clientConfig.getQueryInterval());
    }
  }

  /**
   *
   * @param noteId
   * @param paragraphId
   * @param timeoutInMills
   * @return
   * @throws Exception
   */
  public ParagraphResult waitUtilParagraphFinish(String noteId, String paragraphId, long timeoutInMills) throws Exception {
    long start = System.currentTimeMillis();
    while (true && (System.currentTimeMillis() - start) < timeoutInMills) {
      ParagraphResult paragraphResult = queryParagraphResult(noteId, paragraphId);
      if (paragraphResult.getStatus().isCompleted()) {
        return paragraphResult;
      }
      Thread.sleep(clientConfig.getQueryInterval());
    }
    throw new Exception("Paragraph is not finished in " + timeoutInMills / 1000 + " seconds");
  }

  /**
   *
   * @param noteId
   * @param paragraphId
   * @return
   * @throws Exception
   */
  public ParagraphResult waitUtilParagraphRunning(String noteId, String paragraphId) throws Exception {
    while (true) {
      ParagraphResult paragraphResult = queryParagraphResult(noteId, paragraphId);
      if (paragraphResult.getStatus().isRunning()) {
        return paragraphResult;
      }
      Thread.sleep(clientConfig.getQueryInterval());
    }
  }

  public static void main(String[] args) throws Exception {
    ClientConfig clientConfig = new ClientConfig("http://localhost:18086");
    ZeppelinClient zeppelinClient = new ZeppelinClient(clientConfig);
    String version = zeppelinClient.getVersion();
  }
}
