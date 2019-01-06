package org.apache.spark.graphx

import ca.lif.sparklauncher.app.CustomLogger
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Models.{Message, Messages, Node}
import org.apache.spark.graphx.util.PeriodicGraphCheckpointer
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.util.PeriodicRDDCheckpointer
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable.ArrayBuffer

class AlgorithmFC3(checkpointInterval: Int = 4) extends Algorithm {
  var NB_COULEUR_MAX = 0

  //Envoyer les messages des deux côtés
  def s1[A](ctx: EdgeContext[Node, String, Messages]): Unit = {
    val srcinfo = new Messages()
    srcinfo.append(Message(ctx.srcAttr.color, ctx.srcAttr.tiebreakingValue))
    val dstinfo = new Messages()
    dstinfo.append(Message(ctx.dstAttr.color, ctx.dstAttr.tiebreakingValue))

    //If SRC is a knight, don't send him messages
  //  if (!ctx.srcAttr.knighthood) {
      ctx.sendToSrc(dstinfo)
  //  }
    //If DST is a knight, don't send him messages
   // if (!ctx.dstAttr.knighthood) {
      ctx.sendToDst(srcinfo)
   // }
  }

  /**
    * Trouver la plus petite couleur disponible de 1 jusqu'au nombre chromatique du graph.
    *
    * @param  couleurCourante Couleur du sommet courant sur lequel on souhaite appliquer la fonction
    * @param  couleurs        Les couleurs des voisins
    * @return La couleur qui va être appliqué au sommet
    **/
  def trouverPlusPetiteCouleur(couleurCourante: Int, couleurs: Messages): Int = {
    var couleur = couleurCourante
    var i = 1 //couleur courante qu'on check
    var j = 0 //iterateur du vecteur de couleurs
    //Cas ou le vecteur est vide (devrait pas arriver)
    if (couleurs.isEmpty) return couleur

    def loop(): Int = {
      while (true) {
        //Fin du vecteur?
        if (j == couleurs.length) {
          if (i <= NB_COULEUR_MAX) { //on peut prendre la couleur si il en reste
            return i
          }
          else return couleur
        }

        //i est une couleur qu'on peut prendre. On return, on a la trouve la couleur
        if (i < couleurs(j).color) {
          return i
        }
        //la couleur i est deja presente, on itere un peu plus
        else {
          i += 1 //augmente la couleur
          j += 1 //progression dans le vector
        }
      }
      couleur
    }

    couleur = loop()
    couleur
  }


  /**
    * Fonction appelé par vprog dans l'algorithme de pregel.
    * Choisit une nouvelle couleur (ou non) pour le sommet;
    *
    * @param  vertexid      ID du sommet courant
    * @param  sommetCourant etat actuel du sommet
    * @param  voisins       Nombre maximum de couleur disponible pour le graph
    * @return La couleur qui va être appliqué au sommet
    **/
  def choisirCouleur(vertexid: VertexId, sommetCourant: Node, voisins: Messages, accum: LongAccumulator): Node = {

    //Knight, on return tout de suite
   // if (sommetCourant.knighthood) return sommetCourant
    //Aller chercher la couleur actuelle du sommet

    val couleurCourante = sommetCourant.color

    //Verifier si la couleur actuelle du sommet existe dans les couleurs des voisins
    //1 Trouver la couleur du sommet
    //On regarde si on trouve le msg qui contient notre propre couleur. On recupere aussi le vertex id
    //Si on trouve l'element dans les messages, alors on a un conflit. On essaie de voir si notre sommet a le plus
    //petit vertexid, comme ça il ne change pas de couleur
    val f = voisins.find(_.color == couleurCourante)
    if (f.nonEmpty) {
      val tb_sommet = sommetCourant.tiebreakingValue
      val tb_voisin = f.get.tiebreaker
      //On peut garder notre couleur pour cette itération si notre vertex id est plus petit que celui du voisin
      if (tb_sommet < tb_voisin) {
        return Node(sommetCourant.id, color = sommetCourant.color, knighthood = true, tiebreakingValue = sommetCourant.tiebreakingValue)
      }
    }
    //Sinon, on n'avait pas le tiebreaker le plus petit, nous on change de couleur.
    //Ou alors, la couleur n'existait pas dans les couleurs que les voisins envoient.
    //On choisit quand même une nouevlle couleur minimum (Il faut qu'elle soit mieux que notre couleur courante)
    val c = trouverPlusPetiteCouleur(couleurCourante, voisins)

    //La couleur change
    if (couleurCourante != c) {
      accum.add(1) //on ajoute a l'accumulateur du cluster
      return Node(sommetCourant.id, color = c, tiebreakingValue = sommetCourant.tiebreakingValue)
    }

    //jamasi execute
    Node(sommetCourant.id, color = sommetCourant.color, knighthood = true, tiebreakingValue = sommetCourant.tiebreakingValue)
  }

  //Merges two vectors of messages together. We keep the lowest tiebreaker for a given color
  //https://en.wikipedia.org/wiki/Merge_sort (parallel)
  //O(n)
  def mergeMessages(a: Messages, b: Messages): Messages = {
    val new_vector: Messages = ArrayBuffer()
    var i = 0 //index de la map a
    var j = 0 //index de la map b

    def func(): Unit = {
      while (true) {
        if (i == a.length || a.isEmpty) return
        if (j == b.length || b.isEmpty) return

        //Comparer a et b
        val color_a = a(i).color
        val color_b = b(j).color
        val tb_a = a(i).tiebreaker
        val tb_b = b(j).tiebreaker

        //A = [1,2,4]  B=[2,3,4]  (Juste un vector de couleurs)
        //Cas 1 : Le vecteur A, a la position courante, a une plus petite couleur. On l'ajoute direct
        if (color_a < color_b) {
          //Ajouter la couleur dans le new vector
          new_vector.append(a(i))
          i += 1
        }

        //Cas 2 : Même couleur, il faut garder le plus petit vertexid
        else if (color_a == color_b) {
          val biggestId = if (tb_a < tb_b) tb_a else tb_b
          new_vector.append(Message(color_a, biggestId))
          j += 1
          i += 1
        }

        //Case 3 : B a la plus petite couleur. Donc on copie b au complet
        else {
          new_vector.append(b(j))
          j += 1
        }
      }
    } //fin func

    func()

    //Deverser le restant de A dans newvector
    while (i != a.length) {
      new_vector.append(a(i))
      i += 1
    }

    //Deverser le restant de B dans newvector
    while (j != b.length) {
      new_vector.append(b(j))
      j += 1
    }
    //Return le resultat
    new_vector
  }

  //This is a helper function to execute aggregateMessages over a number of iterations on a certain graph
  def execute(graph: Graph[Node, String], maxIterations: Int, sc: SparkContext): Graph[Node, String] = {
    var myGraph = randomize_ids(graph, sc)

    //printGraphProper(myGraph)

    NB_COULEUR_MAX = myGraph.vertices.count().toInt
    var counter = 0

    //Gestion du GraphX Checkpointer et RDD checkpointer
    val graphCheckpointer = new PeriodicGraphCheckpointer[Node, String](checkpointInterval, sc)
    graphCheckpointer.update(myGraph)

    // compute the messages
    val messageCheckpointer = new PeriodicRDDCheckpointer[(Messages)](checkpointInterval, sc)

    def loop1(): Unit = {
      while (true) {

        counter += 1

        val accum = sc.longAccumulator("changes")
        CustomLogger.logger.info("ITERATION NUMERO : " + counter)

        //On genere les messages. On met le RDD en cache car on va en avoir besoin.
        val vertice_and_messages = myGraph.aggregateMessages[Messages](
          ctx => s1(ctx),
          (a, b) => mergeMessages(a, b)
        )
        messageCheckpointer.update(vertice_and_messages.asInstanceOf[RDD[Messages]])
        //vertice_and_messages.cache()

        //Join les resultats des messages avec choisirCouleur
        myGraph = myGraph.joinVertices(vertice_and_messages)((vid, sommet, messages) => choisirCouleur(vid, sommet, messages, accum))
        graphCheckpointer.update(myGraph)

        //Trigger une action (nécessaire pour les accumulateurs)
        myGraph.vertices.take(1)

        //Si l'accumulateur a bien fonctionne
        if (accum.value == 0 || counter == maxIterations) return

        // Loop cleanup
        vertice_and_messages.unpersist(blocking = false)


        //We make the graph smaller
//        myGraph = myGraph.subgraph(
//          et => {
//            if (et.srcAttr.knighthood == true || et.dstAttr.knighthood == true)
//              false
//            else true
//          }, (vid, node_data) =>  {
//            if (node_data.knighthood == true) false
//            else true
//          }
//        )

        //Remove the edge from the graph when there is a knight on it. This way we can remove a ton of edges from the graph.
//        myGraph = myGraph.subgraph(
//          et => {
//            if (et.srcAttr.knighthood == true || et.dstAttr.knighthood == true)
//              false
//            else true
//          }
//        )



      }
    }

    loop1() //execute loop
    //Cleanup
    //messageCheckpointer.unpersistDataSet()
    //graphCheckpointer.unpersistDataSet()
    //graphCheckpointer.deleteAllCheckpointsButLast()
    //messageCheckpointer.deleteAllCheckpointsButLast()
    myGraph //return the result graph
  }
}
