package org.apache.spark
import ca.lif.sparklauncher.app.CustomLogger
import com.google.common.hash.Hashing
import org.apache.spark.Models.{Hyperedge, Hypergraph, TPair, Variable}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer, Map, Set}

object Generator {

  val hashing = Hashing.sha1()
  //A problem is an array of variables, and their values
  val problem = ArrayBuffer[Variable]()
  //This is used to print, sort, and renumber things
  val allpairs = Map[Long, String]()
  // This is the result
  val hypergraph = Set[Hyperedge]()
  var edge_index = 1

  /* n : the number of chars to grab off a list */
  //https://www.scala-lang.org/api/2.12.3/scala/collection/IndexedSeqOptimized.html
  //IndexOutOfBoundsException si on depasse
  def grabTCharacters(list: ListBuffer[String], n: Int): Option[ListBuffer[String]] = {
    val ee = list.slice(0, n)
    if (ee.lengthCompare(n) == 0) {
      Some(ee)
    }
    else
      None
  }

  /*
  This function takes a value and a list of other values and generates all combinations
  for a given t. t=2 = pairs t=3 means triples and so on
  ListBuffer : https://www.safaribooksonline.com/library/view/scala-cookbook/9781449340292/ch11s03.html
 */
  def grabTPairs(first: String, list: ListBuffer[String], t: Int): ListBuffer[TPair] = {
    var results = new ListBuffer[TPair]()
    //Notre liste mutable
    var l = list

    var firstList = new ListBuffer[String]()
    firstList += first

    //Do this for all t-pairs
    //While the list is still big enough
    while (l.lengthCompare(t - 1) >= 0) {
      val r = grabTCharacters(l, t - 1)
      // we store it.
      val temp = TPair(firstList)
      temp.add(r.get)
      results += temp
      //Delete the first one
      l = l.drop(1)
      //Start again
    }

    results
  }

  //Given a list of values for variables and a T, we return all T pairs from that list
  //Return an Option to handle Nulls
  /*
   This function grabs the T pairs for a given sequence and a starting variable value
   This function needs to be called in a loop for all variables
  Example with t=3 :
   abc
   def
   ghi
   jkl
   We get t=3, and a list of values that are adgj
   With adgj we can do :
   adg (pair)
   delete d
   agj (pair)
   delete g
   aj <3 stop
   Return (adg, agj)
   (See Example)
   */
  def generate_combos(t: Int): Unit = {
    var values = new ListBuffer[String]()

    //Combinations for this hyperedge
    var combinations = new ListBuffer[TPair]

    //First get all the variable values, and the variable name
    //FIX : We prefix the value with the variable name. This fixes many problems
    //It fixes the problem of many variables having the same values. Binary problems for instance
    //Have only 0,1. We need to be able to differentiate them.
    //Of course, for this we also need unique variable names. But that's not too much to ask I think :)
    for (i <- problem) {
      values += i.name + ":" + i.currentValue
    }

    //For each value. Get a T-uple
    for (i <- 0 to values.size) {
      val listt = values.drop(i + 1)
      if (listt.lengthCompare(t - 1) >= 0) {
        val r = grabTPairs(values(i), listt, t)
        combinations = combinations ++ r
      }
    }

    //Print all combinations for one variable
    /*for (i <- combinations) {
      print(i + " ")
    }
    println("")*/

    // Cast to an Hypergraph
    hypergraph += Hyperedge(mutable.Set(combinations.map { e =>
      val str = e.toString
      val hash = hashing.hashString(str).asLong()
      //      if(!allpairs.contains(hash)) allpairs.+=((hash,str))
      //      else if (allpairs.getOrElse(hash,"") != str) println("ERROR")
      hash
    }: _*), edge_index)
    edge_index += 1

  }

  //Now we generate all possible hyperedges
  //pos means, which variable we are working with right now
  //We always start at position 0
  //This is a recursive function
  def generate(pos: Int = 0, t: Int): Unit = {
    //We try all different values of this variable
    for (i <- problem(pos).values) {
      problem(pos).currentValue = i

      //Do we still have more variables?
      if (pos != problem.size - 1) {
        generate(pos + 1, t)
      }

      //Or maybe we are at the right place
      //Now we can generate the hyperedge
      else if (pos == problem.size - 1) {
        //Create new hyperedge and add it to the data structure
        //And we can just print it directly to a file (to save ram)
        //Or we could create it, and join it with an existing RDD.

        //Generate the t-pairs that this hyperedge visits
        generate_combos(t)
      }

      //If we come here, that means that we have generated all hyperedges for this value of this current
      //variable
    }
  }

  def generateHypergraph(t: Int, n: Int, v: Int, output_to_file: Boolean = false): Hypergraph = {
    CustomLogger.logger.info("BEGIN GENERATION")
    // Build all variables
    for (n_iter <- 1 to n) {
      val var_name = s"var$n_iter"
      val values = ArrayBuffer[String]()

      for (v_iter <- 1 to v) {
        values += v_iter.toString
      }

      problem += Variable(var_name, values)
    }

    // Generate
    generate(pos = 0, t)

    // Output if we need
    if (output_to_file) {
      import java.io._
      val pw = new PrintWriter(new File(s"$t-$n-$v.hgph"))

      for (edge <- hypergraph) {
        edge.listVertices.foreach(s => pw.write(s + " "))
        pw.write(System.getProperty("line.separator"))
      }

      pw.close()
    }

    // Return
    CustomLogger.logger.info("GENERATION COMPLETE")

    hypergraph
  }


}


object testGenerator extends App
{
    val res = Generator.generateHypergraph(3,2,2,false)
   res.foreach( println)


}
