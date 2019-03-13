package ca.lif.sparklauncher.model
import org.apache.spark.graphx.{ColoringProgram, runColoringTests}

import scala.reflect.io.{File, Path}

class ColoringParameters(
                          var algo: Int = 1,
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
    if (algo < 1 || algo > 3) {
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
        ColoringProgram.launch(filepath, isGraphviz, t, n, v, partitions, algo, checkpointInterval, loops, maxIterations)
      }
      else {
        ColoringProgram.launch("", isGraphviz, t, n, v, partitions, algo, checkpointInterval, loops, maxIterations)
      }
    }
  }

  override def toString: String = {
    s"""Coloring parameters :
       | algo : $algo
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

  def toCommandlineString: Array[String] = {
    val res = s"--type coloring " +
      s"--algo $algo " +
      s"--loops $loops " +
      s"--partitions $partitions " +
      s"--max_iterations $maxIterations " +
      s"--checkpoint_interval $checkpointInterval " +
      s"--input ${if (filepath.nonEmpty) "file" else "generated"} " +
      s"""--path \"$filepath\" """ +
      s"--isGraphviz $isGraphviz " +
      s"--n $n " +
      s"--t $t " +
      s"--v $v"
    res.split(" ")
  }
}

object ColoringParameters {
  def parse(map_parameters: Map[String, String]): Unit = {
    // s looks like this : --<parameters name> <parameter value> ...

    try {

      var newmap = scala.collection.mutable.Map[String,String]()
      newmap = newmap ++ map_parameters

      if (!newmap.contains("loops")) newmap("loops") = "1"
      if (!newmap.contains("t"))  newmap("t") = "2"
      if (!newmap.contains("n"))  newmap("n") = "3"
      if (!newmap.contains("v"))  newmap("v") = "2"

      if (!newmap.contains("tMax"))  newmap("tMax") =  newmap("t")
      if (!newmap.contains("nMax"))  newmap("nMax") =  newmap("n")
      if (!newmap.contains("vMax"))  newmap("vMax") =  newmap("v")

      if (!newmap.contains("print")) newmap("print") = "false"

      runColoringTests.run(newmap)

    }

    catch {
      case e: Exception => {
        println("Parsing error, please verify the command")
        println("Type --help for more info")
        None
      }
    }

  }
}