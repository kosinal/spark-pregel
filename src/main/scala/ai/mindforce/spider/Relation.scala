package ai.mindforce.spider

import ai.mindforce.spider.Relation.NodeRelation
import ai.mindforce.spider.RelationArguments.RelationConfig
import org.apache.spark.graphx._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Compute direct relation between all nodes connected by edges in graph
  */
object Relation {

  type NodeRelation = scala.Double

  /**
    * Send arguments to parser and start calculation
    *
    * @param args command line arguments
    */
  def main(args: Array[String]): Unit = {
    RelationArguments.parse(calculateAndSave, args)
  }

  /**
    * Compute all relations in graph and save them according configuration set
    *
    * @param config configuration of calculation
    * @param spark  spark session for computation
    */
  def calculateAndSave(config: RelationConfig, spark: SparkSession): Unit = {
    val result = calculateRelationFrame(config, spark)
    config.dataSource.save(
      result,
      config.saveMode
    )
    config.afterSave()
  }

  /**
    * Convert input data defined in configuration into graph and run pregel algorithm co
    * compute indirect relations between nodes.
    *
    * @param config configuration of application
    * @param spark  spark session for computation
    * @return DataFrame containing direct relation between nodes (all indirect relations are converted into direct)
    */
  def calculateRelationFrame(config: RelationConfig, spark: SparkSession): DataFrame = {
    import spark.implicits._

    val data = config.dataSource.load(spark).persist()
    val graph = createGraph(config, data, spark)

    graph.pregel(Message(Seq.empty), activeDirection = EdgeDirection.In)(
      receiveMessage,
      sendMessage,
      mergeMessages
    ).vertices
      .flatMap {
        case (sourceId, node) => node.ownershipMap.map {
          case (targetId, relation) => (sourceId, targetId, relation)
        }
      }.toDF(config.parentCol, config.childCol, config.relationCol)
  }

  /**
    * Convert input data into graph representation
    *
    * @param config configuration of application
    * @param data   data loaded for processing
    * @param spark  spark used for calculation
    * @return graph representation of data
    */
  // ToDo: Need testing with more data and caching. Current results are inconclusive if caching is needed for graph or not
  private def createGraph(config: RelationConfig, data: DataFrame, spark: SparkSession): Graph[Node, Double] = {
    val nodePrimaryId = "node_id"

    import spark.implicits._

    val vertices = data
      .select($"${config.parentCol}".as(nodePrimaryId))
      .union(data.select($"${config.childCol}"))
      .distinct()
      .rdd
      .map(r => (r.getAs[Int](0).toLong, Node()))

    val edges = data
      .rdd
      .map {
        r =>
          Edge(
            r.getAs[Int](config.parentCol).toLong,
            r.getAs[Int](config.childCol).toLong,
            r.getAs[Double](config.relationCol)
          )
      }

    Graph(vertices, edges)
  }

  /**
    * Receive the incoming message to the node and compute accorging algorithm if the message should be counted into
    * node and passed to parents, or not (eg. self loop, message already seen)
    *
    * @param nodeId  identification number of note
    * @param node    current node with incoming messages
    * @param message incoming message
    * @return node with adjusted values and processed message
    */
  def receiveMessage(nodeId: VertexId, node: Node, message: Message): Node = {
    /**
      * Check if the relation info is cycle ==> target is the same as our node
      *
      * @param relationInfo incoming relation info
      * @return true if it is cycle or false otherwise
      */
    def isNotCycle(relationInfo: RelationInfo): Boolean = relationInfo.targetNode != nodeId

    /**
      * Check if the relation info message has been seen in previous step.
      *
      * @param relationInfo incoming relation info
      * @return true if message has been seen, otherwise false
      */
    def hasReceivedMessage(relationInfo: RelationInfo): Boolean =
      node.visitMap(relationInfo.targetNode).contains(relationInfo.originalNode)

    /**
      * Convert the relation info to local context. If the relation info is already about current node, then
      * pass it. If not, look what is the relation between current node and incoming message node and
      * convert it into local context
      *
      * @param relationInfo incoming relation info
      * @return converted relation info to local context
      */
    def convertToLocalRelation(relationInfo: RelationInfo): RelationInfo = relationInfo match {
      case RelationInfo(sourceNode, _, _, _) if sourceNode == nodeId => relationInfo
      case RelationInfo(sourceNode, targetNode, originalNode, ownershipValue) =>
        RelationInfo(
          nodeId,
          targetNode,
          originalNode,
          ownershipValue * node.ownershipMap(sourceNode)
        )
    }

    val rels = message.relations

    if (rels.isEmpty) {
      Node(initialVal = true)
    } else {
      val openMessages = rels
        .filter {
          isNotCycle
        }
        .filterNot {
          hasReceivedMessage
        }
        .map {
          convertToLocalRelation
        }

      val visitMap = openMessages.foldLeft(node.visitMap) {
        case (map, RelationInfo(_, targetNode, originalNode, _)) =>
          map.updated(targetNode, map(targetNode) + originalNode)
      }

      val ownershipMap = openMessages.foldLeft(node.ownershipMap withDefaultValue 0.0) {
        case (map, RelationInfo(_, targetNode, _, ownershipValue)) =>
          map.updated(targetNode, map(targetNode) + ownershipValue)
      }

      Node(
        visitMap,
        ownershipMap,
        openMessages
      )
    }
  }

  /**
    * Send message in opposite edge direction. The content of the message is:
    * 1) the relation itself, if the node has initial flag set tu true (first iteration)
    * 2) the content of open messages if the node is not in initial mode
    *
    * @param triplet triplet containing two nodes and relation between them
    * @return message that should be sent for next iteration
    */
  def sendMessage(triplet: EdgeTriplet[Node, NodeRelation]): Iterator[(VertexId, Message)] = {
    val node = triplet.dstAttr
    val sourceId = triplet.srcId
    val targetId = triplet.dstId
    // Initial message sent
    val relations = if (node.initialVal) {
      Seq(RelationInfo(sourceId, targetId, sourceId, triplet.attr))
    } else {
      node.openMessages
    }
    if (relations.isEmpty) {
      Iterator.empty
    } else {
      Iterator((sourceId, Message(relations)))
    }
  }

  /**
    * Join two messages together
    *
    * @param first  one message
    * @param second second message
    * @return joined messages
    */
  def mergeMessages(first: Message, second: Message): Message = first ++ second
}

case class Node(visitMap: Map[VertexId, Set[VertexId]] = Map.empty withDefaultValue Set.empty,
                ownershipMap: Map[VertexId, NodeRelation] = Map.empty,
                openMessages: Seq[RelationInfo] = Seq.empty,
                initialVal: Boolean = false)

case class RelationInfo(sourceNode: VertexId, targetNode: VertexId, originalNode: VertexId, ownershipValue: Double)

case class Message(relations: Seq[RelationInfo]) {
  /**
    * Join this message with another message according following rules:
    * If both messages are null, than result null as well
    * If one of the messages is null, return the message, that is not null
    * If neither of messages is null, return new message with joined relations in messages
    *
    * @param that another message that should be joined
    * @return joined messages
    */
  def ++(that: Message): Message = (this, that) match {
    case (null, null) => null
    case (x, null) => x
    case (null, x) => x
    case (Message(first), Message(second)) => Message(first ++ second)
  }
}