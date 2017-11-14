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

package org.apache.zeppelin.interpreter.recovery;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.interpreter.InterpreterSetting;
import org.apache.zeppelin.interpreter.InterpreterSettingManager;
import org.apache.zeppelin.interpreter.ManagedInterpreterGroup;
import org.apache.zeppelin.interpreter.launcher.InterpreterClient;
import org.apache.zeppelin.interpreter.remote.RemoteInterpreterProcess;
import org.apache.zeppelin.interpreter.remote.RemoteInterpreterRunningProcess;
import org.apache.zeppelin.notebook.repo.FileSystemNotebookRepo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 *
 */
public class FileSystemRecoveryStorage extends RecoveryStorage {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileSystemNotebookRepo.class);

  private InterpreterSettingManager interpreterSettingManager;
  private Configuration hadoopConf;
  private ZeppelinConfiguration zConf;
  private boolean isSecurityEnabled = false;
  private FileSystem fs;
  private Path recoveryDir;

  public FileSystemRecoveryStorage(ZeppelinConfiguration zConf,
                                   InterpreterSettingManager interpreterSettingManager)
      throws IOException {

    this.zConf = zConf;
    this.interpreterSettingManager = interpreterSettingManager;
    this.hadoopConf = new Configuration();
    this.isSecurityEnabled = UserGroupInformation.isSecurityEnabled();
    if (isSecurityEnabled) {
      String keytab = zConf.getString(
          ZeppelinConfiguration.ConfVars.ZEPPELIN_SERVER_KERBEROS_KEYTAB);
      String principal = zConf.getString(
          ZeppelinConfiguration.ConfVars.ZEPPELIN_SERVER_KERBEROS_PRINCIPAL);
      if (StringUtils.isBlank(keytab) || StringUtils.isBlank(principal)) {
        throw new IOException("keytab and principal can not be empty, keytab: " + keytab
            + ", principal: " + principal);
      }
      UserGroupInformation.loginUserFromKeytab(principal, keytab);
    }

    try {
      this.fs = FileSystem.get(new URI(zConf.getRecoveryDir()), new Configuration());
      LOGGER.info("Creating FileSystem: " + this.fs.getClass().getCanonicalName());
      this.recoveryDir = fs.makeQualified(new Path(zConf.getRecoveryDir()));
      LOGGER.info("Using folder {} to store recovery data", recoveryDir);
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
    if (!fs.exists(recoveryDir)) {
      fs.mkdirs(recoveryDir);
      LOGGER.info("Create recovery dir {} in hdfs", recoveryDir.toString());
    }
    if (fs.isFile(recoveryDir)) {
      throw new IOException("recoveryDir {} is file instead of directory, please remove it or " +
          "specify another directory");
    }
  }

  @Override
  public void onInterpreterClientStart(InterpreterClient client) throws IOException {
    save(client.getInterpreterSettingName());
  }

  @Override
  public void onInterpreterClientStop(InterpreterClient client) throws IOException {
    save(client.getInterpreterSettingName());
  }

  private void save(String interpreterSettingName) throws IOException {
    InterpreterSetting interpreterSetting =
        interpreterSettingManager.getInterpreterSettingByName(interpreterSettingName);
    Path tmpFile = new Path(recoveryDir, interpreterSettingName + ".tmp");
    List<String> recoveryContent = new ArrayList<>();
    for (ManagedInterpreterGroup interpreterGroup : interpreterSetting.getAllInterpreterGroups()) {
      RemoteInterpreterProcess interpreterProcess = interpreterGroup.getInterpreterProcess();
      if (interpreterProcess != null) {
        recoveryContent.add(interpreterGroup.getId() + "\t" + interpreterProcess.getHost() + ":" +
            interpreterProcess.getPort());
      }
    }
    LOGGER.debug("Updating recovery data for interpreterSetting: " + interpreterSettingName);
    LOGGER.debug("Recovery Data: " + StringUtils.join(recoveryContent, "\n"));
    IOUtils.copyBytes(new ByteArrayInputStream(StringUtils.join(recoveryContent, "\n").getBytes()),
        fs.create(tmpFile), hadoopConf);
    Path recoveryFile = new Path(recoveryDir, interpreterSettingName + ".recovery");
    fs.delete(recoveryFile, true);
    fs.rename(tmpFile, recoveryFile);
  }

  @Override
  public Map<String, InterpreterClient> restore() throws IOException {
    Map<String, InterpreterClient> clients = new HashMap<>();
    FileStatus[] recoveryFiles = fs.listStatus(recoveryDir, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().endsWith(".recovery");
      }
    });

    for (FileStatus fileStatus : recoveryFiles) {
      String fileName = fileStatus.getPath().getName();
      String interpreterSettingName = fileName.substring(0,
          fileName.length() - ".recovery".length());
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      IOUtils.copyBytes(fs.open(fileStatus.getPath()), bytes, hadoopConf);
      String recoveryContent = bytes.toString();
      if (!StringUtils.isBlank(recoveryContent)) {
        for (String line : recoveryContent.split("\n")) {
          String[] tokens = line.split("\t");
          String groupId = tokens[0];
          String[] hostPort = tokens[1].split(":");
          int connectTimeout =
              zConf.getInt(ZeppelinConfiguration.ConfVars.ZEPPELIN_INTERPRETER_CONNECT_TIMEOUT);
          InterpreterClient client = new RemoteInterpreterRunningProcess(interpreterSettingName,
              connectTimeout, hostPort[0], Integer.parseInt(hostPort[1]));
          clients.put(groupId, client);
        }
      }
    }

    return clients;
  }
}
