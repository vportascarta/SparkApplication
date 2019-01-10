package org.apache.spark.graphx

import scala.collection.mutable.ArrayBuffer

object Models {

  //Having this inside GraphColoring caused so many problems omg
  //ids are randomized
  case class node(id: Long = 0, color: Int = 1, knighthood: Int = 0, tiebreakvalue: Long = 1L) {

    //Utile pour imprimer le graphe après
    override def toString: String = s"id : $id color : $color knighthood : $knighthood tiebreaker : $tiebreakvalue"
  }

  // to modify according to input graph
  // type Message = Map[Int, VertexId]
  // type used for messages in Pregel algorithm

  case class Message(var color: Int, var tiebreaker: VertexId) extends Serializable {
    override def toString: String = {
      s"color : $color tiebreaker : $tiebreaker"
    }
  }

  type Messages = ArrayBuffer[Message]
}
