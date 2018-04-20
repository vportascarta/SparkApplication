package org.apache.spark.graphx

import scala.collection.mutable.ArrayBuffer

object Models {

  //Having this inside GraphColoring caused so many problems omg
  //ids are randomized
  case class Node(id: Int = 0, color: Int = 1, knighthood: Boolean = false, tiebreakingValue: VertexId = 1L) {

    //Utile pour imprimer le graphe après
    override def toString: String = s"id : $tiebreakingValue color : $color knighthood : $knighthood"
  }

  // to modify according to input graph
  // type Message = Map[Int, VertexId]
  // type used for messages in Pregel algorithm

  case class Message(var color: Int, var id: VertexId) extends Serializable {
    override def toString: String = {
      s"color : $color id : $id"
    }
  }

  type Messages = ArrayBuffer[Message]
}
