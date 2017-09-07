package org.apache.zeppelin.interpreter;

import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.display.AngularObjectRegistryListener;
import org.apache.zeppelin.helium.ApplicationEventListener;
import org.apache.zeppelin.interpreter.remote.RemoteInterpreterProcessListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

import static org.mockito.Mockito.mock;

public class MiniZeppelin {

  protected static final Logger LOGGER = LoggerFactory.getLogger(MiniZeppelin.class);
  private static final String INTERPRETER_SCRIPT =
      System.getProperty("os.name").startsWith("Windows") ?
          "../bin/interpreter.cmd" :
          "../bin/interpreter.sh";

  protected InterpreterSettingManager interpreterSettingManager;
  protected InterpreterFactory interpreterFactory;
  protected File zeppelinHome;
  protected ZeppelinConfiguration conf;

  public void start() throws IOException {
    zeppelinHome = new File(System.getenv("ZEPPELIN_HOME"));
    conf = new ZeppelinConfiguration();
    interpreterSettingManager = new InterpreterSettingManager(conf,
        mock(AngularObjectRegistryListener.class), mock(RemoteInterpreterProcessListener.class), mock(ApplicationEventListener.class));
    interpreterFactory = new InterpreterFactory(interpreterSettingManager);
  }

  public void stop() throws IOException {
    interpreterSettingManager.close();
  }

  public File getZeppelinHome() {
    return zeppelinHome;
  }

  public InterpreterFactory getInterpreterFactory() {
    return interpreterFactory;
  }

  public InterpreterSettingManager getInterpreterSettingManager() {
    return interpreterSettingManager;
  }
}
