package org.apache.spark

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import java.util.{ArrayList, List}

import ca.lif.sparklauncher.app.CustomLogger
import ca.uqac.lif.testing.tway.{FrontEnd, VertexListGenerator}
import org.apache.spark.Models.Hypergraph

object Generator2 {
  def generateHypergraph(t: Int, n: Int, v: Int, output_to_file: Boolean = false): Hypergraph = {
    CustomLogger.logger.info("BEGIN GENERATION")

    val domains = FrontEnd.createDomains(n, v)
    val var_names: List[String] = new ArrayList[String](domains.size)
    var_names.addAll(domains.keySet)

    val twp = new VertexListGenerator(t, var_names)

    val baos = new ByteArrayOutputStream()
    val ps = new PrintStream(baos, true, "utf-8")

    twp.setOutput(ps)
    twp.addDomain(domains)
    twp.generateTWayEdges()

    val content = new String(baos.toByteArray, StandardCharsets.UTF_8)

    CustomLogger.logger.info("GENERATION COMPLETE")
    Parser.parseString(content)
  }
}
