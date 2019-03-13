package ca.lif.sparklauncher.console

import ca.lif.sparklauncher.model.{ColoringParameters, HypergraphParameters}

import scala.io.Source
import scala.reflect.io.{File, Path}

object MainConsole {
  val VERSION = "1.0.0"
  val HEADER = s"TSPARK $VERSION (c) 2019 Edmond LA CHANCE & Sylvain HALLÉ & Vincent PORTA-SCARTA"
  val FOOTER = "See results_tspark.txt file. For all other tricks, consult the documentation"

  def main(args: Array[String]): Unit = {

    if (args.length == 0) {
      showConsoleHelp()
      System.exit(0)
    }

    if (args.contains("--help")) {
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

          if (argsFile.isDefined) {
            args2maps(argsFile.get)
          }
          else {
            println("Config file does not exists, please check path")
            System.exit(-1)
            null
          }

        }
        else {
          args2maps(args)
        }
      }

      val algo_type = map_parameters("type")

      algo_type match {
        case "coloring" =>
          val execution = ColoringParameters.parse(map_parameters)
          if (execution.nonEmpty) {
            execution.get.execute()
          }

        case "hypergraph" =>
          HypergraphParameters.parse(map_parameters)


        case _ => println("Algorithm type unknown, please check --type parameter")
      }
    }
  }

  //Offrir également une fonctionnalité de Graph coloring? Peut etre c'est mieux de juste faire un autre programme.
  def showConsoleHelp(): Unit = {
    println(HEADER)
    println(
      """Help :
        |
        |--type coloring|hypergraph choose your type of algorithm. Default is hypergraph
        |--loops N    number of loops run by spark on the same data. Default is 1
        |--t N    interaction strength. Default is 2
        |--n N    number of variables. Default is 3
        |--v N    number of values per variable. Default is 2
        |--print  true|false (prints the results). Default is false
        |
        |Optional
        |Specify max values to generate a range of tests.
        |Default values = t, n and v values.
        |--tMax N
        |--nMax N
        |--vMax N
        |
        |The results will be appended to a file named results_tspark.txt
        |
        |Tip :
        |To increase the memory available for spark put the java parameter -Xmx before any other parameter
        |For exemple to use 4Go of RAM, type : java -Xmx4G -jar <jar path> ...
        |
      """.stripMargin)
    println(FOOTER)
  }

  def showVersion(): Unit = {
    println(HEADER)
    println(
      """Version :
        |  Graph Coloring : (Knights and Peasants algorithm)
        |  Hypergraph : Greedy Set Cover algorithm with Integer compression
        |  Spark version 2.4.0
      """.stripMargin
    )
  }

  def configFile2args(path: String): Option[Array[String]] = {
    if (File(Path(path)).exists) {
      val input = Source.fromFile(path).getLines()
      val res = input.flatMap(_.trim().split(" "))
      Some(res.toArray)
    }
    else {
      None
    }
  }

  def args2maps(args_array: Array[String]): Map[String, String] = {
    // Input array : [--<parameter name>, <value>, --<parameter name>, <value>,...]

    val args_sanitize = args_array.map { arg =>
      val arg_sanitize = arg.replaceAll("--", "")
      arg_sanitize.trim()
    }
    val args_pairs = args_sanitize.grouped(2)

    // Then we extract the key, value to create a map
   val toto =  args_pairs.map { case Array(k, v) => k -> v }.toMap
    toto
  }
}

object testConsole extends App
{
 // MainConsole.main(["--type hypergraph","--loops 1","--t 2", --n 3 --v 2 --print false --tMax 2 --nMax 3 --vMax 2"
  //var argggs = ["--type hypergraph", "--loops 1", "--t 2"]

  var argggg = Array[String]("--type","hypergraph","--loops","1","--print","true")
  MainConsole.main(argggg)


}




//Old help

//"""Help :
//|
//|--type {coloring | hypergraph} choose your type of algorithm. Default is hypergraph
//|
//|if type = coloring
//|    --algo {1,2,3}    choose your version
//|    --loops N    number of loops run by spark on the same data
//|    --input {file | generated}  choose where your input data come from. Default is generated.
//|
//|    if input = file
//|        --path "<path to the file>"    path to your file
//|        --isGraphviz {true | false}    is your file formatted with the graphviz format
//|
//|    if input = generated
//|        --t N    interaction strength
//|        --n N    number of variables
//|        --v N    number of values per variable
//|
//|if type = hypergraph
//|    --algo {1,2}    choose your version
//|    --loops N    number of loops run by spark on the same data
//|    --partitions N    number of partitions for each RDD (0 = auto)
//|    --input {file | generated}    choose where your input data come from
//|
//|    if input = file
//|       --path "<path to the file>"    path to your file
//|    if input = generated
//|       --n N    number of variables
//|        --t N    number of variables on each group
//|        --v N    number of value for one variable
//|
//|Your can put all this parameters in one config file (each parameter on one line) and call it with @"<path to the config file>"
//|
//|Tip :
//|To increase the memory available for spark put the java parameter -Xmx before any other parameter
//|For exemple to use 4Go of RAM, type : java -Xmx4G -jar <jar path> ...