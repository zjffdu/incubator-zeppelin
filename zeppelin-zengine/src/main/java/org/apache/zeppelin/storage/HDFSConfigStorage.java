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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.interpreter.InterpreterInfoSaving;
import org.apache.zeppelin.interpreter.InterpreterSetting;
import org.apache.zeppelin.notebook.NotebookAuthorizationInfoSaving;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * It could be used either local file system or hadoop distributed file system,
 * because FileSystem support both local file system and hdfs.
 *
 */
public class HDFSConfigStorage extends ConfigStorage {

  private static final Logger LOGGER = LoggerFactory.getLogger(HDFSConfigStorage.class);
  private static final Gson gson =  new GsonBuilder().setPrettyPrinting().create();

  private Configuration hadoopConf;
  private FileSystem fs;
  private Path rootConfFolder;

  private Path interpreterSettingPath;
  private Path authorizationPath;

  public HDFSConfigStorage(ZeppelinConfiguration zConf) throws IOException {
    super(zConf);
    this.hadoopConf = new Configuration();
    this.fs = FileSystem.get(hadoopConf);
    this.rootConfFolder = fs.makeQualified(new Path(zConf.getConfDir()));
    this.interpreterSettingPath = fs.makeQualified(new Path(zConf.getInterpreterSettingPath()));
    this.authorizationPath = fs.makeQualified(new Path(zConf.getNotebookAuthorizationPath()));
  }

  @Override
  public void save(InterpreterInfoSaving settingInfos) throws IOException {
    LOGGER.info("Save Interpreter Settings to " + interpreterSettingPath);
    String json = gson.toJson(settingInfos);
    InputStream in = new ByteArrayInputStream(json.getBytes(
        zConf.getString(ZeppelinConfiguration.ConfVars.ZEPPELIN_ENCODING)));
    IOUtils.copyBytes(in, fs.create(interpreterSettingPath, true), hadoopConf);
  }

  @Override
  public InterpreterInfoSaving loadInterpreterSettings() throws IOException {
    if (!fs.exists(interpreterSettingPath)) {
      LOGGER.warn("Interpreter Setting file {} is not existed", interpreterSettingPath);
      return null;
    }
    LOGGER.info("Load Interpreter Setting from file: " + interpreterSettingPath);
    ByteArrayOutputStream jsonBytes = new ByteArrayOutputStream();
    IOUtils.copyBytes(fs.open(interpreterSettingPath), jsonBytes, hadoopConf);
    String json = new String(jsonBytes.toString(
        zConf.getString(ZeppelinConfiguration.ConfVars.ZEPPELIN_ENCODING)));
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
    InputStream in = new ByteArrayInputStream(json.getBytes(
        zConf.getString(ZeppelinConfiguration.ConfVars.ZEPPELIN_ENCODING)));
    IOUtils.copyBytes(in, fs.create(authorizationPath, true), hadoopConf);

  }

  @Override
  public NotebookAuthorizationInfoSaving loadNotebookAuthorization() throws IOException {
    if (!fs.exists(authorizationPath)) {
      LOGGER.warn("Interpreter Setting file {} is not existed", authorizationPath);
      return null;
    }
    LOGGER.info("Load notebook authorization from file: " + authorizationPath);
    ByteArrayOutputStream jsonBytes = new ByteArrayOutputStream();
    IOUtils.copyBytes(fs.open(authorizationPath), jsonBytes, hadoopConf);
    String json = new String(jsonBytes.toString(
        zConf.getString(ZeppelinConfiguration.ConfVars.ZEPPELIN_ENCODING)));
    return gson.fromJson(json, NotebookAuthorizationInfoSaving.class);
  }

}
