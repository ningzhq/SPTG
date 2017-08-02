package com.huawei.readertest

import java.io.File
import java.security.PrivilegedAction
import java.util.Iterator

import com.google.common.base.Preconditions
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery
import com.thinkaurelius.titan.diskstorage.util.{EntryArrayList, StaticArrayBuffer, StaticArrayEntry}
import com.thinkaurelius.titan.diskstorage.{Entry, EntryList, StaticBuffer}
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx
import com.thinkaurelius.titan.hadoop.config.{ModifiableHadoopConfiguration, TitanHadoopConfiguration}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, VertexId}
import org.apache.spark.rdd.HBaseScanRDD
import org.apache.tinkerpop.gremlin.structure.Direction
import org.visallo.tools.export.filters.{EdgeFilter, PropertyFilter, VertexFilter}
import org.visallo.tools.export.properties.{EdgeProperties, Properties, VertexProperties}
import org.visallo.tools.export.utils.GraphConf

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object GraphBaseReader2 {

  var graph : StandardTitanGraph = null
  var vertexFilter : VertexFilter = new VertexFilter()
  var edgeFilter : EdgeFilter = null
  var propertyFilter : PropertyFilter = null

  def initiate(): Unit ={
    graph = GraphUtils.getTitanGraph
    val tx = graph.getCurrentThreadTx.asInstanceOf[StandardTitanTx]
    edgeFilter = new EdgeFilter(tx)
    propertyFilter = new PropertyFilter(tx)
    tx.close()
    filterMetaData()
  }

  def getGraphX(sc : SparkContext,resourcePath : String): org.apache.spark.graphx.Graph[Properties, Properties] = {
    initiate()
    val sc = SparkContext.getOrCreate()
    var hadoopConf = sc.hadoopConfiguration

    // Declare the information of the table to be queried.
    val titanmrConf: ModifiableHadoopConfiguration = ModifiableHadoopConfiguration.of(TitanHadoopConfiguration.MAPRED_NS, hadoopConf)
    titanmrConf.set(TitanHadoopConfiguration.COLUMN_FAMILY_NAME, GraphUtils.EDGESTORE_FAMILY_NAME)
    val localbc = graph.getConfiguration.getLocalConfiguration
    val vertexFilterHook = sc.broadcast(vertexFilter)
    graph.close()
    localbc.clearProperty(org.apache.tinkerpop.gremlin.structure.Graph.GRAPH)
    GraphConf.copyInputKeys(hadoopConf, localbc)
    val hbConf = HBaseConfiguration.create(hadoopConf)
    println(hbConf.toString)
    val scan = new Scan()
    scan.addFamily(Bytes.toBytes(GraphUtils.SHORT_EDGESTORE_FAMILY_NAME))
    val proto = ProtobufUtil.toScan(scan)
    val scanToString = Base64.encodeBytes(proto.toByteArray)
    hbConf.set("hbase.mapreduce.inputtable", localbc.getString("storage.hbase.table"))
    hbConf.set("hbase.mapreduce.scan", scanToString)
    val inputCFBytes = Bytes.toBytes(GraphUtils.SHORT_EDGESTORE_FAMILY_NAME)

    val byteRDD = new HBaseScanRDD(sc,hbConf)

    //vertex
    val vertexRDD = byteRDD.mapPartitions(iter => {
      def parseVertex():scala.Iterator[(VertexId, Properties)]={
        val resourcePath = System.getProperty("user.dir") + File.separator + "src"
        val vertexList = new ListBuffer[(org.apache.spark.graphx.VertexId, Properties)]
        val graph = GraphUtils.getTitanGraph
        val tx = graph.getCurrentThreadTx.asInstanceOf[StandardTitanTx]
        while (iter.hasNext) {
          val it = iter.next()
          //key
          val sKey = StaticArrayBuffer.of(it._1.copyBytes)
          val vertexID = graph.getIDManager.getKeyID(sKey)
          //value
          val properties = new VertexProperties
          val relationIter = it._2.getMap.get(inputCFBytes)
          relationIter.entrySet().asScala.foreach(e => {
            val col = e.getKey
            val value = e.getValue.lastEntry().getValue
            val entry = StaticArrayEntry.of(new StaticArrayBuffer(col), new StaticArrayBuffer(value))
            val relation = tx.getEdgeSerializer.parseRelation(entry, true, tx)

            val relationType = tx.getExistingRelationType(relation.typeId)
            if (relationType.isPropertyKey){
              properties.put(relation.typeId,relation.getValue)
            }
          })
          val conceptTypeValue = properties.get(GraphUtils.CONCEPT_TYPE)
          if (conceptTypeValue!=null && !conceptTypeValue.toString.trim.startsWith(GraphUtils.START_WITH_WEAVER)){
            vertexList.append((vertexID,properties))
          }
        }
        graph.close()
        vertexList.iterator
      }

      UGIUtil.login().doAs(new PrivilegedAction[scala.Iterator[(VertexId, Properties)]] {
        override def run(): scala.Iterator[(VertexId, Properties)] = parseVertex()
      })

    })
    //edge
    val edgeRDD = byteRDD.mapPartitions(iter => {
      def parseEdge():scala.Iterator[Edge[Properties]]={
        val edgeList = new ListBuffer[Edge[Properties]]
        val graph = GraphUtils.getTitanGraph
        val tx = graph.getCurrentThreadTx.asInstanceOf[StandardTitanTx]
        while (iter.hasNext) {
          val it = iter.next()
          //key
          val sKey = StaticArrayBuffer.of(it._1.copyBytes)
          val vertexID = graph.getIDManager.getKeyID(sKey)
          //value
          val properties = new EdgeProperties
          val relationIter = it._2.getMap.get(inputCFBytes)
          relationIter.entrySet().asScala.foreach(e => {
            val col = e.getKey
            val value = e.getValue.lastEntry().getValue
            val entry = StaticArrayEntry.of(new StaticArrayBuffer(col), new StaticArrayBuffer(value))
            val edge = tx.getEdgeSerializer.parseRelation(entry, true, tx)

            val relationType = tx.getExistingRelationType(edge.typeId)
            if (relationType.isEdgeLabel){
              if (!edge.direction.equals(Direction.IN)){
                properties.setLabel(relationType.name())
                if (edge.hasProperties){
                  val edgePropertyIter = edge.propertyIterator()
                  while (edgePropertyIter.hasNext){
                    val edgeProperty = edgePropertyIter.next()
                    properties.put(edgeProperty.key,edgeProperty.value)
                  }
                }
                edgeList.append(new Edge(vertexID,edge.getOtherVertexId,properties))
              }
            }
          })
        }
        graph.close()
        edgeList.iterator
      }

      UGIUtil.login().doAs(new PrivilegedAction[scala.Iterator[Edge[Properties]]] {
        override def run(): scala.Iterator[Edge[Properties]] = parseEdge()
      })

    })

    println("Filter:"+vertexFilterHook.value.toString)
    val grapx = org.apache.spark.graphx.Graph(vertexRDD,edgeRDD, new VertexProperties)
    grapx.subgraph(triplet => {
      vertexFilterHook.value.evaluate(triplet.srcAttr) && vertexFilterHook.value.evaluate(triplet.dstAttr)
    }, (vid,prop)=> {

      def filter():Boolean={
        vertexFilterHook.value.evaluate(prop)
      }

      UGIUtil.login().doAs(new PrivilegedAction[Boolean] {
        override def run(): Boolean = filter()
      })
    } )
  }

  def getVertexFilter: VertexFilter = vertexFilter

  def getEdgeFilter: EdgeFilter = edgeFilter

  def getPropertyFilter: PropertyFilter = propertyFilter

  private def filterMetaData(): Unit = {
    vertexFilter.has(GraphUtils.CONCEPT_TYPE)
    vertexFilter.hasNot(GraphUtils.SCHEMA_CONCEPT_TYPE)
    //vertexFilter.notPrefix(GraphUtils.CONCEPT_TYPE, GraphUtils.START_WITH_WEAVER)
  }

  private def findEntriesMatchingQuery(query: SliceQuery, sortedEntries: EntryList): EntryList = {
    var lowestStartMatch: Int = sortedEntries.size // Inclusive
    var highestEndMatch: Int = -1 // Inclusive
    val queryStart: StaticBuffer = query.getSliceStart
    val queryEnd: StaticBuffer = query.getSliceEnd
    // Find the lowest matchStart s.t. query.getSliceStart <= sortedEntries.get(matchStart)
    var low: Int = 0
    var high: Int = sortedEntries.size - 1
    var lowestStartMatched : Boolean = false
    while (low <= high && !lowestStartMatched) {
      val mid: Int = (low + high) >>> 1
      val midVal: Entry = sortedEntries.get(mid)
      val cmpStart: Int = queryStart.compareTo(midVal.getColumn)
      if (0 < cmpStart) {
        // query lower bound exceeds entry (no match)
        if (lowestStartMatch == mid + 1) {
          // lowestStartMatch located
          lowestStartMatched = true //todo: break is not supported
        } else {
          // Move to higher list index
          low = mid + 1
        }
      }
      else /* (0 >= cmpStart) */ {
        // entry equals or exceeds query lower bound (match, but not necessarily lowest match)
        if (mid < lowestStartMatch) lowestStartMatch = mid
        // Move to a lower list index
        high = mid - 1
      }
    }
    // If lowestStartMatch is beyond the end of our list parameter, there cannot possibly be any matches,
    // so we can bypass the highestEndMatch search and just return an empty result.
    if (sortedEntries.size == lowestStartMatch) return EntryList.EMPTY_LIST
    // Find the highest matchEnd s.t. sortedEntries.get(matchEnd) < query.getSliceEnd
    low = 0
    high = sortedEntries.size - 1
    var highestEndMatched : Boolean = false
    while (low <= high && !highestEndMatched) {
      val mid: Int = (low + high) >>> 1
      val midVal: Entry = sortedEntries.get(mid)
      val cmpEnd: Int = queryEnd.compareTo(midVal.getColumn)
      if (0 < cmpEnd) {
        // query upper bound exceeds entry (match, not necessarily highest)
        if (mid > highestEndMatch) highestEndMatch = mid
        // Move to higher list index
        low = mid + 1
      }
      else /* (0 >= cmpEnd) */ {
        // entry equals or exceeds query upper bound (no match)
        if (highestEndMatch == mid - 1) {
          // highestEndMatch located
          highestEndMatched = true //todo: break is not supported
        } else {
          // Move to a lower list index
          high = mid - 1
        }
      }
    }
    if (0 <= highestEndMatch - lowestStartMatch) {
      // Return sublist between indices (inclusive at both indices)
      var endIndex: Int = highestEndMatch + 1 // This will be passed into subList, which interprets it exclusively
      if (query.hasLimit) endIndex = Math.min(endIndex, query.getLimit + lowestStartMatch)
      // TODO avoid unnecessary copy here
      EntryArrayList.of(sortedEntries.subList(lowestStartMatch, endIndex /* exclusive */))
    }
    else EntryList.EMPTY_LIST
  }
}

//util
private[this] class HBaseMapIterable(val columnValues: java.util.NavigableMap[Array[Byte], java.util.NavigableMap[java.lang.Long, Array[Byte]]]) extends java.lang.Iterable[Entry] {
  Preconditions.checkNotNull(columnValues)

  def iterator: java.util.Iterator[Entry] = new HBaseMapIterator(columnValues.entrySet.iterator)
}

private[this] class HBaseMapIterator(val iterator: java.util.Iterator[java.util.Map.Entry[Array[Byte], java.util.NavigableMap[java.lang.Long, Array[Byte]]]]) extends Iterator[Entry] {
  def hasNext: Boolean = iterator.hasNext

  def next: Entry = {
    val entry: java.util.Map.Entry[Array[Byte], java.util.NavigableMap[java.lang.Long, Array[Byte]]] = iterator.next
    val col: Array[Byte] = entry.getKey
    val `val`: Array[Byte] = entry.getValue.lastEntry.getValue
    StaticArrayEntry.of(new StaticArrayBuffer(col), new StaticArrayBuffer(`val`))
  }

  override def remove() {
    throw new UnsupportedOperationException
  }
}
