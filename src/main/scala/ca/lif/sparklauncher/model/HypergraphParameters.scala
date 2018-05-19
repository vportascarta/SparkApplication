package ca.lif.sparklauncher.model

import org.apache.spark.HypergraphProgram

import scala.reflect.io.{File, Path}

class HypergraphParameters(
                            var algo: Int = 1,
                            var loops: Int = 1,
                            var partitions: Int = -1,
                            // File input
                            var filepath: String = "",
                            // Generate input
                            var n: Int = 3,
                            var t: Int = 2,
                            var v: Int = 2
                          ) extends Executable with Serializable {

  override def verify(): Boolean = {
    // Check version
    if (algo < 1 || algo > 2) {
      return false
    }

    // Check loops
    if (loops < 1) {
      return false
    }

    // Check partitions
    if (partitions < 0 && partitions != -1) {
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
        HypergraphProgram.launchFile(algo, filepath, partitions, loops)
      }
      else {
        HypergraphProgram.launchGenerated(algo, t, n, v, partitions, loops)
      }
    }
  }

  override def toString: String = {
    s"""Hypergraph parameters :
       | algo : $algo
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

  def toCommandlineString: Array[String] = {
    val res = s"--type hypergraph " +
      s"--algo $algo " +
      s"--loops $loops " +
      s"--partitions $partitions " +
      s"--input ${if (filepath.nonEmpty) "file" else "generated"} " +
      s"""--path \"$filepath\" """ +
      s"--n $n " +
      s"--t $t " +
      s"--v $v"
    res.split(" ")
  }
}

object HypergraphParameters {
  def parse(map_parameters: Map[String, String]): Option[HypergraphParameters] = {
    // s looks like this : --<parameters name> <parameter value> ...

    try {
      val return_value = new HypergraphParameters()

      // We fill the value
      return_value.algo = map_parameters("algo").toInt

      if (map_parameters.contains("loops"))
        return_value.loops = map_parameters("loops").toInt

      if (map_parameters.contains("partitions")) {
        return_value.partitions = map_parameters("partitions").toInt
        if (return_value.partitions == 0) {
          return_value.partitions = Runtime.getRuntime.availableProcessors
        }
      }

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
        println("Parsing error, please verify the command")
        println("Type --help for more info")
        None
      }
    }

  }
}