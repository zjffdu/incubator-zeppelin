package org.apache.zeppelin.interpreter;

import org.apache.commons.io.FileUtils;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Created by jzhang on 6/6/17.
 */
public class AbstractInterpreterTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractInterpreterTest.class);

  protected InterpreterSettingManager interpreterSettingManager;
  protected File rootFolder;
  protected ZeppelinConfiguration conf;

  @Before
  public void setUp() throws Exception {
    rootFolder = new File(System.getProperty("java.io.tmpdir") + "/ZeppelinLTest_" + System.currentTimeMillis());
    rootFolder.mkdirs();
    LOGGER.info("Create tmp directory: {} as root folder of ZEPPELIN_INTERPRETER_DIR & ZEPPELIN_CONF_DIR", rootFolder.getAbsolutePath());
    String interpreterDir = rootFolder + "/interpreter";
    String confDir = rootFolder + "/conf";
    FileUtils.copyDirectory(new File("src/test/resources/interpreter"), new File(interpreterDir));
    FileUtils.copyDirectory(new File("src/test/resources/conf"), new File(confDir));

    System.setProperty(ZeppelinConfiguration.ConfVars.ZEPPELIN_CONF_DIR.getVarName(), confDir);
    System.setProperty(ZeppelinConfiguration.ConfVars.ZEPPELIN_INTERPRETER_DIR.getVarName(), interpreterDir);
    System.setProperty(ZeppelinConfiguration.ConfVars.ZEPPELIN_INTERPRETER_GROUP_ORDER.getVarName(), "echo");
    interpreterSettingManager = new InterpreterSettingManager(
        new ZeppelinConfiguration(), null, null, null);
  }


}
