package org.apache.spark

import ca.lif.sparklauncher.app.CustomLogger
import org.apache.spark.Models.{Hyperedge, Hypergraph}

import scala.collection.mutable.{ArrayBuffer, Set}
import scala.io.Source


import me.lemire.integercompression._

object Parser {
  //Read a hypergraph file
  //This is the file format. Each line is a hyperedge, and the content is the vertexIds in the set
  // The first line is the number of hyperedges. We should skip it.
  //  23
  //  0 1 2 3 6 7 8 9 12 13 14 15 18 19 20 21
  //  0 1 2 3 4 12 13 14 15 16

  def parseFile(filename: String): Hypergraph = {
    var edgeCounter: Long = 0L
    var hypergraph = Set[Hyperedge]()

    def treatLine(line: String): Option[Hyperedge] = {
      //check pour les commentaires
      if (line(0) == '#') {
        return None
      }

      val tokens = line.split(" ")
      val verticesOnHyperedge = Set[Long]()
      //Add the token to the mutable set
      for (i <- tokens) {
        verticesOnHyperedge += i.toLong
      }

      edgeCounter += 1
      val hyperedge_val = Hyperedge(verticesOnHyperedge, edgeCounter)
      Some(hyperedge_val)
    }

    var lastPrint = System.currentTimeMillis()
    CustomLogger.logger.info("BEGINNING OF PARSING")

    var isFirst = true
    //For all the lines in the file
    for (line <- Source.fromFile(filename).getLines()) {
      if (!isFirst) {
        val result = treatLine(line)
        if (result.nonEmpty) {
          hypergraph += result.get
        }
      }
      isFirst = false

      if (System.currentTimeMillis() - lastPrint > 1000) {
        CustomLogger.logger.finest(s"CREATING HYPERGRAPH : n $edgeCounter")
        lastPrint = System.currentTimeMillis()
      }
    }

    CustomLogger.logger.info("END OF PARSING")

    hypergraph
  }


  //We take a string that contains the hypergraph and we generate
  def parseString(str: String): ArrayBuffer[Array[Int]] = {

    import me.lemire.integercompression.differential.IntegratedIntCompressor
    val iic = new IntegratedIntCompressor

    var edgeCounter: Long = 0L
    var hypergraph = ArrayBuffer[Array[Int]]()

    def treatLine(line: String): Option[Array[Int]] = {
      //check pour les commentaires
      if (line(0) == '#') {
        return None
      }

      val tokens = line.split(" ")
      val verticesOnHyperedge = ArrayBuffer[Int]()
      //Add the token to the mutable set
      for (i <- tokens) {
        verticesOnHyperedge += i.toInt
      }

      edgeCounter += 1
      Some(verticesOnHyperedge.toArray)
    }

    var lastPrint = System.currentTimeMillis()
    CustomLogger.logger.info("BEGIN PARSING")

    val lines = str.split(System.getProperty("line.separator"))
    var isFirst = true
    //For all the lines in the file
    for (line <- lines) {
      if (!isFirst) {
        val result = treatLine(line)
        if (result.nonEmpty) {

          //Compress this
          val compressed: Array[Int] = iic.compress(result.get)
          //val recov: Array[Int] = iic.uncompress(compressed)
          hypergraph += compressed
        }
      }
      isFirst = false

      if (System.currentTimeMillis() - lastPrint > 1000) {
        CustomLogger.logger.finest(s"CREATING HYPERGRAPH : n $edgeCounter")
        lastPrint = System.currentTimeMillis()
      }
    }

    CustomLogger.logger.info("END OF PARSING")
    val sizehyper = hypergraph.size

    CustomLogger.logger.info(s"Number of hyperedges : $sizehyper")

    hypergraph
  }
}
