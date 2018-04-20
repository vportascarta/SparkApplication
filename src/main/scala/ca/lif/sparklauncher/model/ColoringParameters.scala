package ca.lif.sparklauncher.model

import org.apache.spark.graphx.ColoringProgram

import scala.reflect.io.{File, Path}

class ColoringParameters(
                          var version: Int = 1,
                          var loops: Int = 1,
                          var partitions: Int = -1,
                          var maxIterations: Int = 500,
                          var checkpointInterval: Int = -1,
                          // File input
                          var filepath: String = "",
                          var isGraphviz: Boolean = true,
                          // Generate input
                          var n: Int = 3,
                          var t: Int = 2,
                          var v: Int = 2
                        ) extends Executable {

  override def verify(): Boolean = {
    // Check version
    if (version < 1 || version > 3) {
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

    // Check maxIterations
    if (maxIterations < 1) {
      return false
    }

    // Check checkpointInterval
    if (checkpointInterval < 1 && checkpointInterval != -1) {
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
            ColoringProgram.launch(filepath, isGraphviz, t, n, v, partitions, version, checkpointInterval, loops, maxIterations)
          }
        }.start()
      }
      else {
        new Thread {
          override def run(): Unit = {
            ColoringProgram.launch("", isGraphviz, t, n, v, partitions, version, checkpointInterval, loops, maxIterations)
          }
        }.start()
      }
    }
  }

  override def toString: String = {
    s"""Coloring parameters :
       | version : $version
       | loops : $loops
       | partitions : $partitions
       | max iteration : $maxIterations
       | checkpoint interval : $checkpointInterval
       | file path : $filepath
       | is graphviz : $isGraphviz
       | n : $n
       | t : $t
       | v : $v
       |
       |Validate : ${verify()}
     """.stripMargin
  }
}

object ColoringParameters {
  def parse(map_parameters: Map[String, String]): Option[ColoringParameters] = {
    // s looks like this : --<parameters name> <parameter value> ...

    try {
      val return_value = new ColoringParameters()

      // We fill the value
      return_value.version = map_parameters("version").toInt

      if (map_parameters.contains("loops"))
        return_value.loops = map_parameters("loops").toInt

      if (map_parameters.contains("partitions"))
        return_value.partitions = map_parameters("partitions").toInt

      if (map_parameters.contains("max_iterations"))
        return_value.maxIterations = map_parameters("max_iterations").toInt

      if (map_parameters.contains("checkpoint_interval"))
        return_value.checkpointInterval = map_parameters("checkpoint_interval").toInt

      if (map_parameters("input").equals("file")) {
        return_value.filepath = map_parameters("path").replaceAll("\"", "")
        return_value.isGraphviz = map_parameters("is_graphviz").equals("true")
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