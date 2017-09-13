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


package org.apache.zeppelin.storage;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.io.IOUtils;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.interpreter.InterpreterInfoSaving;
import org.apache.zeppelin.interpreter.InterpreterSetting;
import org.apache.zeppelin.notebook.NotebookAuthorizationInfoSaving;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;


/**
 * Store configuration in local file system
 *
 */
public class LocalConfigStorage extends ConfigStorage {

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalConfigStorage.class);
  private static final Gson gson =  new GsonBuilder().setPrettyPrinting().create();

  private String interpreterSettingPath;
  private String authorizationPath;

  public LocalConfigStorage(ZeppelinConfiguration zConf) {
    super(zConf);
    this.interpreterSettingPath = zConf.getInterpreterSettingPath();
    this.authorizationPath = zConf.getNotebookAuthorizationPath();
  }

  @Override
  public void save(InterpreterInfoSaving settingInfos) throws IOException {
    LOGGER.info("Save Interpreter Settings to " + interpreterSettingPath);
    String json = gson.toJson(settingInfos);
    FileWriter writer = new FileWriter(interpreterSettingPath);
    IOUtils.copy(new StringReader(json), writer);
    writer.close();
  }

  @Override
  public InterpreterInfoSaving loadInterpreterSettings() throws IOException {
    LOGGER.info("Load interpreter setting from file: " + interpreterSettingPath);
    if (!new File(interpreterSettingPath).exists()) {
      return null;
    }
    String json = IOUtils.toString(new FileReader(interpreterSettingPath));
    //TODO(zjffdu) This kind of post processing is ugly.
    JsonParser jsonParser = new JsonParser();
    JsonObject jsonObject = jsonParser.parse(json).getAsJsonObject();
    InterpreterInfoSaving infoSaving = gson.fromJson(json, InterpreterInfoSaving.class);
    for (InterpreterSetting interpreterSetting : infoSaving.interpreterSettings.values()) {
      // Always use separate interpreter process
      // While we decided to turn this feature on always (without providing
      // enable/disable option on GUI).
      // previously created setting should turn this feature on here.
      interpreterSetting.getOption().setRemote(true);
    }
    return infoSaving;
  }

  public void save(NotebookAuthorizationInfoSaving authorizationInfoSaving) throws IOException {
    LOGGER.info("Save notebook authorization to file: " + authorizationPath);
    String json = gson.toJson(authorizationInfoSaving);
    FileWriter writer = new FileWriter(authorizationPath);
    IOUtils.copy(new StringReader(json), writer);
    writer.close();

  }

  @Override
  public NotebookAuthorizationInfoSaving loadNotebookAuthorization() throws IOException {
    LOGGER.info("Load notebook authorization from file: " + authorizationPath);
    if (!new File(authorizationPath).exists()) {
      return null;
    }
    String json = IOUtils.toString(new FileReader(authorizationPath));
    return gson.fromJson(json, NotebookAuthorizationInfoSaving.class);
  }
}
