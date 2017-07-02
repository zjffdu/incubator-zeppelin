/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*  http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.zeppelin.python;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteResultHandler;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.LogOutputStream;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.exec.environment.EnvironmentUtils;
import org.apache.commons.io.FileUtils;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.remote.RemoteInterpreterUtils;
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * IPython Interpreter for Zeppelin
 */
public class IPythonInterpreter extends Interpreter implements ExecuteResultHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(IPythonInterpreter.class);
  private static final Pattern PYTHON2_PATTERN = Pattern.compile("Python 2\\.");
  private static final Pattern PYTHON3_PATTERN = Pattern.compile("Python 3\\.");

  ExecuteWatchdog watchDog;
  private IPythonClient ipythonClient;
  private String pythonExecutable;
  private String pythonVersion;

  public IPythonInterpreter(Properties properties) {
    super(properties);
  }

  @Override
  public void open() {
    try {
      pythonExecutable = getProperty().getProperty("zeppelin.python", "python");
      pythonVersion = extractPythonVersion(pythonExecutable);
      int ipythonPort = RemoteInterpreterUtils.findRandomAvailablePortOnAllLocalInterfaces();
      LOGGER.info("Launching IPython Kernel at port: " + ipythonPort);
      launchIPythonKernel(ipythonPort);
      ipythonClient = new IPythonClient("localhost", ipythonPort);
    } catch (IOException e) {
      e.printStackTrace();
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
  }

  private String extractPythonVersion(String pythonExecutable) throws IOException {
    DefaultExecutor executor = new DefaultExecutor();
    CommandLine cmd = new CommandLine(pythonExecutable);
    cmd.addArgument("-V");
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    executor.setStreamHandler(new PumpStreamHandler(outputStream));
    executor.execute(cmd, EnvironmentUtils.getProcEnvironment());
    String output = outputStream.toString();
    if (output.contains("Python 2")) {
      return "2";
    } else if (output.contains("Python 3")) {
      return "3";
    } else {
      throw new IOException("UnSupported Python version: " + output);
    }
  }

  private void launchIPythonKernel(int ipythonPort) throws IOException, URISyntaxException {
    // Run IPython kernel
    File tmpPythonScriptFolder = Files.createTempDirectory("zeppelin_ipython").toFile();
    String[] ipythonScripts = {"ipython_server.py", "ipython_pb2.py", "ipython_pb2_grpc.py"};
    for (String ipythonScript : ipythonScripts) {
      URL url = getClass().getClassLoader().getResource("grpc/python" + pythonVersion
          + "/" + ipythonScript);
      FileUtils.copyURLToFile(url, new File(tmpPythonScriptFolder, ipythonScript));
    }

    CommandLine cmd = CommandLine.parse(pythonExecutable);
    cmd.addArgument(tmpPythonScriptFolder.getAbsolutePath() + "/ipython_server.py");
    cmd.addArgument(ipythonPort + "");
    DefaultExecutor executor = new DefaultExecutor();
    ProcessLogOutputStream processOutput = new ProcessLogOutputStream(LOGGER);
    executor.setStreamHandler(new PumpStreamHandler(processOutput));
    watchDog = new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT);
    executor.setWatchdog(watchDog);
    executor.execute(cmd, EnvironmentUtils.getProcEnvironment(), this);

    try {
      LOGGER.debug("Sleep 3 seconds");
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void close() {
    if (watchDog != null) {
      ipythonClient.stop(StopRequest.newBuilder().build());
      watchDog.destroyProcess();
      LOGGER.debug("Kill IPython Process");
    }
  }

  @Override
  public InterpreterResult interpret(String st, InterpreterContext context) {
    ExecuteResponse response =
        ipythonClient.execute(ExecuteRequest.newBuilder().setCode(st).build());
    return new InterpreterResult(InterpreterResult.Code.valueOf(response.getStatus().name()),
        response.getResult());
  }

  @Override
  public void cancel(InterpreterContext context) {

  }

  @Override
  public FormType getFormType() {
    return FormType.SIMPLE;
  }

  @Override
  public int getProgress(InterpreterContext context) {
    return 0;
  }

  @Override
  public List<InterpreterCompletion> completion(String buf, int cursor,
                                                InterpreterContext interpreterContext) {
    List<InterpreterCompletion> completions = new ArrayList<>();
    CompleteResponse response =
        ipythonClient.complete(
            CompleteRequest.getDefaultInstance().newBuilder().setCode(buf).build());
    for (int i = 0; i < response.getMatchesCount(); i++) {
      completions.add(new InterpreterCompletion(
          response.getMatches(i), response.getMatches(i), ""));
    }
    return completions;
  }

  public String getPythonVersion() {
    return pythonVersion;
  }

  @Override
  public void onProcessComplete(int exitValue) {
    LOGGER.warn("Python Process is completed with exitValue: " + exitValue);
  }

  @Override
  public void onProcessFailed(ExecuteException e) {
    LOGGER.warn("Exception happens in Python Process", e);
  }

  private static class ProcessLogOutputStream extends LogOutputStream {

    private Logger logger;
    OutputStream out;

    public ProcessLogOutputStream(Logger logger) {
      this.logger = logger;
    }

    @Override
    protected void processLine(String s, int i) {
      this.logger.debug("Process Output:" + s);
    }

    @Override
    public void write(byte [] b) throws IOException {
      super.write(b);

      if (out != null) {
        synchronized (this) {
          if (out != null) {
            out.write(b);
          }
        }
      }
    }

    @Override
    public void write(byte [] b, int offset, int len) throws IOException {
      super.write(b, offset, len);

      if (out != null) {
        synchronized (this) {
          if (out != null) {
            out.write(b, offset, len);
          }
        }
      }
    }

    public void setOutputStream(OutputStream out) {
      synchronized (this) {
        this.out = out;
      }
    }
  }
}
