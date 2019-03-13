package org.apache.spark

import ca.lif.sparklauncher.app.CustomLogger
import org.apache.spark.Models.Hypergraph
import org.apache.spark.sql.DataFrame

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object HypergraphProgram {

  def launchFile(algo: Int,
                 filename: String,
                 partitions: Int,
                 loops: Int): Unit = {

    // Logging
    CustomLogger.setLogger()
    CustomLogger.logger.info(s"Hypergraph - Beginning - Algo $algo")
    CustomLogger.logger.info(s"Parameters : partitions = $partitions")
    CustomLogger.logger.info(s"File : $filename")

    // Parsing
    //val hypergraph = Parser.parseFile(filename)

 //   launch(algo, hypergraph, partitions, loops)

  }

  def launchGenerated(algo: Int,
                      t: Int,
                      n: Int,
                      v: Int,
                      partitions: Int,
                      loops: Int): Unit = {

    // Logging
    CustomLogger.setLogger()
    CustomLogger.logger.info(s"Hypergraph - Beginning - Algo $algo")
    CustomLogger.logger.info(s"Parameters : partitions = $partitions")
    CustomLogger.logger.info(s"Config : T = $t / N = $n / V = $v")


    //Force system to use algo 2 all the time
    var myalgo = 2

    def get_hypergraph: Option[ArrayBuffer[Array[Int]]] = {


    //  if (myalgo == 1) {
   //     Some(Generator.generateHypergraph(t, n, v))
   //   }
    //  else
      if (myalgo == 2) {
        Some(Generator2.generateHypergraph(t, n, v))
      }
      else None
    }

    val hypergraph = get_hypergraph.get

    launch(myalgo, hypergraph, partitions, loops)

  }

  def launch(algo: Int,
             hypergraph: ArrayBuffer[Array[Int]],
             partitions: Int,
             loops: Int): Unit = {
    // Spark Config
    val conf = new SparkConf().setAppName("Hypergraph greedy").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    sc.hadoopConfiguration.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
    sc.setCheckpointDir("./")
    sc.setLogLevel("ERROR")

    val hypergraphRDD = sc.parallelize(hypergraph, partitions)

    var myAlgo = 2

    // Looping the algo
    for (i <- 1 to loops) {
      CustomLogger.logger.info(s"Test n $i/$loops")

      def get_result: Option[ArrayBuffer[Long]] = {
//        if (myAlgo == 1) {
//          Some(Algorithm.greedy_algorithm(sc, hypergraphRDD))
//        }

         if (myAlgo == 2) {
          Some(Algorithm2.greedy_algorithm(sc, hypergraphRDD))
        }
        else None
      }

      val hyperedge_choisies = get_result.getOrElse(ArrayBuffer[Long]())

      var result = s"L'algorithme greedy a choisi ${hyperedge_choisies.size} hyperedges "

      CustomLogger.logger.info(result)
    }

    sc.stop()
  }

}


object runHyperGraphTests extends App
{

  def run(map_parameters : scala.collection.mutable.Map[String, String] = null ) : Unit = {

    val conf = new SparkConf()
      .setAppName("every hypergraph test is here")
      .setMaster("local[*]")
      .set("spark.local.dir", "/media/data/") //The 4TB hard drive can be used for shuffle files
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    /* Syntax is :

    2;3;2;COLORING_SPARK;2.559;4;
    t n v
     */

    // PrintWriter
    //ouvrir en mode append
    import java.io._
    val pw = new PrintWriter(new FileOutputStream("results_tspark.txt", true))

    var numberOfloops = 1

    //Initial should be 2 everywhere
    var initialT = 2
    var initialN = 3
    var initialV = 2

    var maxT = 2
    var maxN = 3
    var maxV = 2

    var print = "false"

    if (map_parameters != null) {
        numberOfloops = map_parameters("loops").toInt
        initialT = map_parameters("t").toInt
        initialN = map_parameters("n").toInt
        initialV = map_parameters("v").toInt
        maxT = map_parameters("tMax").toInt
        maxN = map_parameters("nMax").toInt
        maxV = map_parameters("vMax").toInt
        print = map_parameters("print")
    }


    //T
    for (t <- initialT to maxT)
    {
      //vary n
      for (n <- initialN to maxN )
      {
        //vary v
        for (v <- initialV to maxV )
        {
          gen(t,n,v)
        }
      }
    }

    def gen(t : Int, n : Int, v : Int) =
    {

      val t1gen = System.nanoTime()

      val hypergraph = Generator2.generateHypergraph(t, n, v)

      val t2gen = System.nanoTime()
      val time_elapsed =  (t2gen - t1gen).toDouble  / 1000000000
      println(s"Time elapsed : $time_elapsed seconds")

      val hypergraphRDD = sc.parallelize(hypergraph)

      // For implicit conversions from RDDs to DataFrames


      //val numhyperedges = hypergraph.size
      //val elementsPerHyperedge = hypergraph.head.size

      for (i <- 0 until numberOfloops)
      {
        println(s"Config : T = $t / N = $n / V = $v")
        println(s"Algorithm : Set Cover greedy algorithm (random equals) + integer compression")
        println(s"Test n $i/$numberOfloops")
        //println(s"Size of problem : $numhyperedges hyperedges and $elementsPerHyperedge elements per hyperedge on average.")
        //println(s"There should be around ${numhyperedges * elementsPerHyperedge} elements in the RDD")

        val t1 = System.nanoTime()
        val chosen_hyperedges = Algorithm2.greedy_algorithm(sc, hypergraphRDD)
        val numcolors = chosen_hyperedges.size
        println(s"Greedy algorithm found ${numcolors} tests")

        val t2 = System.nanoTime()
        val time_elapsed =  (t2 - t1).toDouble  / 1000000000

        println(s"Time elapsed : $time_elapsed seconds")

        if (print == "true")
          chosen_hyperedges.foreach(println)

        //Write the results to our file
        pw.append(s"$t;$n;$v;HYPERGRAPH_SETCOVER;$time_elapsed;$numcolors\n")
        pw.flush()

        System.gc()

      }

    }

    //Close file
    pw.close

} //end of run method

  run()

}


//.set("spark.executor.memory","32g")
//.set("spark.shuffle.file.buffer","3200k")
// .set("spark.dynamicAllocation.enabled", "true")
//.set("spark.dynamicAllocation.maxExecutors", "1")
// .set("spark.dynamicAllocation.minExecutors", "1")

//Pour Tungsten et Dataset
//.set("spark.memory.offHeap.enabled", "true")
//.set("spark.memory.offHeap.size", "34359738368")