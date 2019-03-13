package ca.lif.sparklauncher.model

import org.apache.spark.{HypergraphProgram, runHyperGraphTests}

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
  def parse(map_parameters: Map[String, String]): Unit = {
    // s looks like this : --<parameters name> <parameter value> ...

    try {
      //val return_value = new HypergraphParameters()

//        val loops = map_parameters("loops").toInt
//        val n = map_parameters("n").toInt
//        val t = map_parameters("t").toInt
//        val v = map_parameters("v").toInt
//
//        val tMax = map_parameters("tMax").toInt
//        val nMax = map_parameters("nMax").toInt
//        val vMax = map_parameters("vMax").toInt
//
//        val print = map_parameters("print").toInt

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

        runHyperGraphTests.run(newmap)

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