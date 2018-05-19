package ca.lif.sparklauncher.gui

import java.lang.ProcessBuilder.Redirect

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.reflect.io.File

object ProcessHelper {

  private var process: Process = _

  def start(optionsAsString: String, mainClass: String, arguments: Array[String]): Unit = {
    if (process == null) {
      val processBuilder = createProcess(optionsAsString, mainClass, arguments)
      process = processBuilder.start

      ConsoleView.copyInputStream(process.getInputStream)
    }
    else {
      println("Cannot launch two spark processes")
    }
  }

  private def createProcess(optionsAsString: String, mainClass: String, arguments: Array[String]) = {
    val jvm = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java"
    val classpath = System.getProperty("java.class.path")

    val options = optionsAsString.split(" ")
    val command = ListBuffer.empty[String]
    command += jvm
    command ++= options
    command += mainClass
    command ++= arguments

    val processBuilder = new ProcessBuilder(command.asJava)

    val environment = processBuilder.environment
    environment.put("CLASSPATH", classpath)

    processBuilder.redirectErrorStream(true)
    processBuilder.redirectOutput(Redirect.PIPE)

    processBuilder
  }

  def kill(): Unit = {
    if (process != null) {
      process.destroy()
      process = null
    }
  }
}
