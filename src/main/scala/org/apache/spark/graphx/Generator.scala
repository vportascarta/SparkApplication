package org.apache.spark.graphx

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import java.util.{ArrayList, List}

import ca.lif.sparklauncher.app.CustomLogger
import ca.uqac.lif.testing.tway._
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Models.node
import org.apache.spark.rdd.RDD

import scala.collection.mutable


object Generator {
  def generateGraph(t: Int, n: Int, v: Int, sc: SparkContext, partitions: Int): Graph[node, String] = {
    CustomLogger.logger.info("BEGIN GENERATION")

    val domains = FrontEnd.createDomains(n, v)
    val var_names: List[String] = new ArrayList[String](domains.size)
    var_names.addAll(domains.keySet)

    val twp = new DotGraphGenerator(t, var_names)

    val baos = new ByteArrayOutputStream()
    val ps = new PrintStream(baos, true, "utf-8")

    twp.setOutput(ps)
    twp.addDomain(domains)
    twp.generateTWayEdges()

    val content = new String(baos.toByteArray, StandardCharsets.UTF_8)

    CustomLogger.logger.info("GENERATION COMPLETE")
    Parser.readGraphVizString(content, sc, partitions)
  }


  def generate_nodes_and_edges( t: Int, n: Int, v: Int, sc: SparkContext, partitions: Int  ) :
      Tuple2[Array[(Long, node_data)],Vector[edge_data]] = {

    CustomLogger.logger.info("BEGIN GENERATION")

    val domains = FrontEnd.createDomains(n, v)
    val var_names: List[String] = new ArrayList[String](domains.size)
    var_names.addAll(domains.keySet)

    val twp = new DotGraphGenerator(t, var_names)

    val baos = new ByteArrayOutputStream()
    val ps = new PrintStream(baos, true, "utf-8")

    twp.setOutput(ps)
    twp.addDomain(domains)
    twp.generateTWayEdges()

    val content = new String(baos.toByteArray, StandardCharsets.UTF_8)

    CustomLogger.logger.info("GENERATION COMPLETE")

    //Il faut mmaintenant parser "content"

    var edgesVector: Vector[edge_data] = Vector.empty[edge_data]
    //Comment inferer le return??
    def treatLine(line: String): Option[edge_data] = {
      //les sommets commencent a 1
      val values = line.split(" ") //Split with space separator

      //Ligne genre graph ou commentaire
      if (values(0)(0) > '9' || values(0)(0) < '0') {
        return Option(null)
      }

      //Gerer le label (pas besoin)
      if (values(1)(0) == '[') return Option(null)

      //Faire une edge
      val src = values(0).toLong + 1
      val dst = values(2).replace(";", "").toLong + 1

      val e = edge_data(src, dst)
      Option(e)

    }
    //For all the lines in the file
    for (line <- content.split(System.getProperty("line.separator"))) {
      val e = treatLine(line)
      if (e.nonEmpty)
        edgesVector = edgesVector :+ e.get
    }

    var vertices: mutable.Map[VertexId, node_data] = collection.mutable.Map[Long, node_data]()

    //Fill the map
    edgesVector.foreach( e => {
      if (!vertices.isDefinedAt(e.src))
        vertices(e.src) = node_data()

      if (!vertices.isDefinedAt(e.dst))
        vertices(e.dst) = node_data()
    })


   // println("Print le resultat de la generation")
    //println("Les edges : ")
   // edgesVector.foreach(println)

    //println("La map")
    //vertices.toArray.sortBy(_._1).foreach( println)


    (vertices.toArray, edgesVector)

  }



}
