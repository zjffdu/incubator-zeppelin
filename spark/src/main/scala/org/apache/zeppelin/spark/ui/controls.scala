package org.apache.zeppelin.spark.ui

import org.apache.zeppelin.spark.SparkZeppelinContext
import org.apache.zeppelin.display.angular.notebookscope._
import AngularElem._

trait BaseDynamicForm {
  val z = SparkZeppelinContext.get()
}

class TextBox(name: String, defaultValue: String = "", size:Int = 30, config: Map[String, String] = Map.empty) extends BaseDynamicForm {

  <input type="text" size={size + ""}></input>.model("name", defaultValue).display

  def value: String = AngularModel("name")().toString()

}

class CheckBox(name: String, options: Seq[Tuple2[Object, String]]) extends BaseDynamicForm {
  z.checkbox(name, options)

  def checkedOptions: Seq[Object] = z.checkbox(name, options).toSeq
}

class Select(name: String, options: Seq[Tuple2[Object, String]]) extends BaseDynamicForm {

  <div class="form-group">
    <label for="name">{ name }</label>
    <select class="form-control" id={name}>
      {  options.map(option => <option>{option._2}</option>) }
    </select>.model(name, "")
  </div>.display

  def selected: Object = AngularModel(name)().toString
}

class Button(displayName: String, onClick: () => Unit) extends BaseDynamicForm {
  val variableName = "run"//"var_" + UUID.randomUUID().toString.substring(0, 8)
  println(s"%angular <button ng-click='$variableName=$variableName+1'>" +
    s"<p hidden> {{ $variableName }} </p>" +
    s"$displayName</button>")
  z.angularUnbind(variableName)
  z.angularBind(variableName, 0)
  z.angularWatch(variableName, (before, after) => onClick())
}

