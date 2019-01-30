package org.apache.spark

import ca.lif.sparklauncher.app.CustomLogger
import org.apache.spark.Models.Hypergraph

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
    val hypergraph = Parser.parseFile(filename)

    launch(algo, hypergraph, partitions, loops)

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

    def get_hypergraph: Option[Hypergraph] = {


      if (myalgo == 1) {
        Some(Generator.generateHypergraph(t, n, v))
      }
      else if (myalgo == 2) {
        Some(Generator2.generateHypergraph(t, n, v))
      }
      else None
    }

    val hypergraph = get_hypergraph.get

    launch(myalgo, hypergraph, partitions, loops)

  }

  def launch(algo: Int,
             hypergraph: Hypergraph,
             partitions: Int,
             loops: Int): Unit = {
    // Spark Config
    val conf = new SparkConf().setAppName("Hypergraph greedy").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    sc.hadoopConfiguration.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
    sc.setCheckpointDir("./")
    sc.setLogLevel("ERROR")

    val hypergraphRDD = sc.parallelize(hypergraph.toSeq, partitions)

    // Looping the algo
    for (i <- 1 to loops) {
      CustomLogger.logger.info(s"Test n $i/$loops")

      def get_result: Option[ArrayBuffer[Long]] = {
        if (algo == 1) {
          Some(Algorithm.greedy_algorithm(sc, hypergraphRDD))
        }
        else if (algo == 2) {
          Some(Algorithm2.greedy_algorithm(sc, hypergraphRDD))
        }
        else None
      }

      val hyperedge_choisies = get_result.getOrElse(ArrayBuffer[Long]())

      var result = s"L'algorithme greedy a choisi ${hyperedge_choisies.size} hyperedges. Les voici : "
      hyperedge_choisies.foreach(he => result += he + " ")

      CustomLogger.logger.info(result)
    }

    sc.stop()
  }

}
