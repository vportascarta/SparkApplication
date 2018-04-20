package ca.lif.sparklauncher.model

import ca.lif.sparklauncher.gui.Gui
import ca.lif.sparklauncher.main.Application
import org.apache.spark.HypergraphProgram

import scala.reflect.io.{File, Path}
import scala.swing.Swing

class HypergraphParameters(
                            var version: Int = 1,
                            var loops: Int = 1,
                            var partitions: Int = -1,
                            // File input
                            var filepath: String = "",
                            // Generate input
                            var n: Int = 3,
                            var t: Int = 2,
                            var v: Int = 2
                          ) extends Executable {

  override def verify(): Boolean = {
    // Check version
    if (version < 1 || version > 2) {
      return false
    }

    // Check loops
    if (loops < 1) {
      return false
    }

    // Check partitions
    if (partitions < 1 && partitions != -1) {
      return false
    }

    // Check if filepath is not empty and if the file exist
    if (filepath.nonEmpty && !File(Path(filepath)).exists) {
      return false
    }

    // Check if values for generator is correct
    if (filepath.isEmpty && (t < 2 || v < 2 || n < 2 || n < t)) {
      return false
    }

    return true
  }

  override def execute(): Unit = {
    if (verify()) {
      if (filepath.nonEmpty) {
        new Thread {
          override def run(): Unit = {
            HypergraphProgram.launchFile(version, filepath, partitions, loops)
            return
            //resetView()
          }
        }.start()
      }
      else {
        new Thread {
          override def run(): Unit = {
            HypergraphProgram.launchGenerated(version, t, n, v, partitions, loops)
            return
            //resetView()
          }
        }.start()
      }
    }
  }

  def resetView(): Unit = {
    // Update view if launched
    if (Application.GUI_LAUNCHED) {
      Swing.onEDTWait {
        Gui.resetTitle()
      }
      println("ok")
    }
  }

  override def toString: String = {
    s"""Hypergraph parameters :
       | version : $version
       | loops : $loops
       | partitions : $partitions
       | file path : $filepath
       | n : $n
       | t : $t
       | v : $v
       |
       |Validate : ${verify()}
     """.stripMargin
  }

}

object HypergraphParameters {
  def parse(map_parameters: Map[String, String]): Option[HypergraphParameters] = {
    // s looks like this : --<parameters name> <parameter value> ...

    try {
      val return_value = new HypergraphParameters()

      // We fill the value
      return_value.version = map_parameters("version").toInt

      if (map_parameters.contains("loops"))
        return_value.loops = map_parameters("loops").toInt

      if (map_parameters.contains("partitions"))
        return_value.partitions = map_parameters("partitions").toInt

      if (map_parameters("input").equals("file")) {
        return_value.filepath = map_parameters("path").replaceAll("\"", "")
      }

      if (map_parameters("input").equals("generated")) {
        return_value.n = map_parameters("n").toInt
        return_value.t = map_parameters("t").toInt
        return_value.v = map_parameters("v").toInt
      }

      // Return the object
      Some(return_value)
    }

    catch {
      case e: Exception => {
        println("Parsing error, please verify the command\nType --help for more info")
        None
      }
    }

  }
}