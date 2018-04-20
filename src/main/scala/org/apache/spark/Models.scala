package org.apache.spark

import scala.collection.mutable.{ArrayBuffer, ListBuffer, Set}

object Models {

  type Hypergraph = Set[Hyperedge]

  case class Hyperedge(listVertices: Set[Long], edgeID: Long)

  //A tpair is a group of values. Can be a pair if t=2 or more if t is 3,4,5 etc
  //The initial list has only one character in it
  case class TPair(var list: ListBuffer[String]) {
    def add(i: ListBuffer[String]): Unit = {
      list = list ++ i
    }

    override def toString: String = {
      var output = "|"
      for (i <- list) {
        output += i + "|"
      }
      output
    }
  }

  //A variable is simply a name and a list of possible values.
  //We also have a currentValue variable. We use this during the generation
  case class Variable(name: String, values: ArrayBuffer[String]) {
    var currentValue: String = ""
    //current value of this variable. Used by the generator

    def add(elem: String): Unit = {
      values += elem
    }

    override def toString: String = {
      var output = ""
      for (i <- values) {
        output += i + " "
      }
      output
    }
  }

}