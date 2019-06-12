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


package org.apache.zeppelin.spark;

import org.apache.spark.ml.classification.GBTClassificationModel;
import org.apache.spark.ml.classification.GBTClassifier;
import org.apache.zeppelin.interpreter.AbstractInterpreter;
import org.apache.zeppelin.interpreter.BaseZeppelinContext;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterException;
import org.apache.zeppelin.interpreter.InterpreterResult;

import java.util.Properties;

public class SparkMLGBTInterpreter extends AbstractInterpreter {


  public SparkMLGBTInterpreter(Properties properties) {
    super(properties);
  }

  @Override
  public BaseZeppelinContext getZeppelinContext() {
    return null;
  }

  @Override
  protected InterpreterResult internalInterpret(String st, InterpreterContext context) throws InterpreterException {
    return null;
  }

  private void train(InterpreterContext context) {
    String labelCol = (String) context.getAngularObjectRegistry()
            .get("labelCol", context.getNoteId(), context.getParagraphId()).get();
    String featureCol = (String) context.getAngularObjectRegistry()
            .get("featureCol", context.getNoteId(), context.getParagraphId()).get();
    GBTClassifier gbt = new GBTClassifier()
            .setLabelCol(labelCol)
            .setFeaturesCol(featureCol)
            .setMaxIter(10);
    GBTClassificationModel model = gbt.fit(null);
    model.
  }

  @Override
  public void open() throws InterpreterException {

  }

  @Override
  public void close() throws InterpreterException {

  }

  @Override
  public void cancel(InterpreterContext context) throws InterpreterException {

  }

  @Override
  public FormType getFormType() throws InterpreterException {
    return null;
  }

  @Override
  public int getProgress(InterpreterContext context) throws InterpreterException {
    return 0;
  }
}
