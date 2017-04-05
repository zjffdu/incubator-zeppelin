package org.apache.zeppelin.spark

import java.util.UUID


trait BaseDynamicForm {
  val z = SparkZeppelinContext.get()
}

class TextBox(name: String, defaultValue: String = "") extends BaseDynamicForm {
  z.textbox(name, defaultValue)

  def value: String = z.textbox(name).toString

}

class CheckBox(name: String, options: Seq[Tuple2[Object, String]]) extends BaseDynamicForm {
  z.checkbox(name, options)

  def checkedOptions: Seq[Object] = z.checkbox(name, options).toSeq
}

class Select(name: String, options: Seq[Tuple2[Object, String]]) extends BaseDynamicForm {
  z.select(name, options)

  def selected: Object = z.select(name, options)
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

