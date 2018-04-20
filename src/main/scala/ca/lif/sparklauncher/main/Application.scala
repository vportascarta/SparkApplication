package ca.lif.sparklauncher.main

import ca.lif.sparklauncher.gui.Gui
import ca.lif.sparklauncher.model.{ColoringParameters, HypergraphParameters}

object Application extends App {
  val VERSION = "1.0.0"
  val HEADER = s"Spark Launcher $VERSION (c) 2018 Edmond LA CHANCE & Vincent PORTA-SCARTA"
  val FOOTER = "For all other tricks, consult he documentation"

  if (args.isEmpty) {
    Gui.main(args)
  }

  else if (args.contains("--help")) {
    showConsoleHelp()
    System.exit(0)
  }

  else if (args.contains("--version")) {
    showVersion()
    System.exit(0)
  }

  else {
    val map_parameters = {
      if (args(0).startsWith("@")) {
        println("Read config file")
        val argsFile = configFile2args(args(0).replace("@", ""))
        args2maps(argsFile)
      }
      else {
        args2maps(args)
      }
    }

    val algo_type = map_parameters.getOrElse("type", "nothing")

    algo_type match {
      case "coloring" =>
        val execution = ColoringParameters.parse(map_parameters)
        if (execution.nonEmpty) {
          execution.get.execute()
        }

      case "hypergraph" =>
        val execution = HypergraphParameters.parse(map_parameters)
        if (execution.nonEmpty) {
          execution.get.execute()
        }

      case _ => println("Algorithm type unknown, please check --type parameter")
    }

    println("End of program")
  }

  def showConsoleHelp(): Unit = {
    println(HEADER)
    // TODO Help
    println(FOOTER)
  }

  def showVersion(): Unit = {
    println(HEADER)
    println(
      """Version :
        |  Graph Coloring version 1.0
        |  Hypergraph Solver version 1.2
        |  Spark version 2.3.0
      """.stripMargin
    )
  }

  def configFile2args(path: String): Array[String] = {
    ???
  }

  def args2maps(args_array: Array[String]): Map[String, String] = {
    // Input array : [--<parameter name>, <value>, --<parameter name>, <value>,...]

    val args_sanitize = args_array.map { arg =>
      val arg_sanitize = arg.replaceAll("--", "")
      arg_sanitize.trim()
    }
    val args_pairs = args_sanitize.grouped(2)

    // Then we extract the key, value to create a map
    args_pairs.map { case Array(k, v) => k -> v }.toMap
  }
}
