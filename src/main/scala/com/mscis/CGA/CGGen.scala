package com.mscis.CGA

import com.mscis.CGA.CGUtils._

import org.apache.spark.{SparkConf, SparkContext, graphx}
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._

import org.apache.log4j.{Level, LogManager}
//import org.apache.spark.sql._


object CGGen {
  def main(args: Array[String]) {

    val log = LogManager.getRootLogger
    log.setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("CGGen")
    val sc = new SparkContext(conf)
    val inputSM = "/home/alx/dpl/input"
    val output = "/home/alx/dpl/output"
    val output3 = "/home/alx/dpl/output3"
    log.info("got Info!")

    val inputdata = sc.parallelize(getContext(inputSM))
    log.info("serialized Info!")


    case class InfoData(classFQName: (String, String), declaredMethods: List[CGUtils.declData], invokedMethods: List[String])

    val inform = inputdata.map(x => {

      val fullName = x._1
      val name = fullName.drop(fullName.lastIndexOf("/"))
      val sourceInfo = getInfo(x._2)
      val mDeclarations = getDeclInfo(sourceInfo.lift(1).toString.split("-----").toList)
      val mInvocations = sourceInfo.lift(2).toString.split("-----").toList
      InfoData((fullName,name), mDeclarations, mInvocations)

    })

    val classVertices:RDD[(graphx.VertexId, (String,String))] = inform.map(x => (vertexHash(x.classFQName._1), (x.classFQName._2, "class")))

    val methodVerticesList = inform.flatMap(x => x.declaredMethods)

    val methodVertices:RDD[(graphx.VertexId, (String,String))] = methodVerticesList.map(x => (vertexHash(x.mMods+x.mType+x.mName+x.mPar), (x.toString,"declaration")))

    val invocVerticesList = inform.flatMap(x => x.invokedMethods)

    val invocVertices:RDD[(graphx.VertexId, (String,String))] = invocVerticesList.map(x => (vertexHash(x), (x,"invocation")))

    val cmVertices:RDD[(graphx.VertexId, (String,String))]  = classVertices.union(methodVertices)

    val allVertices:RDD[(graphx.VertexId, (String,String))]  = cmVertices.union(invocVertices)

    val edgesInv: RDD[Edge[String]] = inform.flatMap { x =>
      val srcVid = vertexHash(x.classFQName._1)
      x.invokedMethods.map({ iMeth =>
        val dstVid = vertexHash(iMeth)
        Edge(srcVid, dstVid, "invokes method")
      })
    }

    val edgesNew: RDD[Edge[String]] = inform.flatMap{ x=>
      x.invokedMethods.flatMap{ iMeth =>
        val srcVid = vertexHash(iMeth)
        val pairss = x.declaredMethods.filter(p => p.mName.contains(iMeth))
        pairss.map(c => {
          val dstVid = vertexHash(c.mMods+c.mType+c.mName+c.mPar)
          Edge(srcVid, dstVid, "declared")
        })
      }
    }

//    classVertices.filter(x => x._2._2=="class")

    val edges = edgesNew.union(edgesInv)

    val defaultCon = ("a","A")

    val finalGraph:Graph[(String,String), String] = Graph(allVertices, edges, defaultCon)
    finalGraph.triplets.saveAsTextFile(output)

    val test = finalGraph.subgraph(vpred = (id, attr) => attr._1=="/TableCellLayoutManager")

    val test2 = test.mapVertices( (id, attr) => )



    val dotVert =finalGraph.aggregateMessages[String](
      triplet =>  {triplet.sendToDst(triplet.srcAttr._1)},
         (a,b) => (a + " -> " + b)
    )
    dotVert.saveAsTextFile(output3)




    /*val initialMsg = "digraph G {\n"

    def vprog(vertexId: VertexId, value:(String, String), message: String): String = {
      if (value._2=="class"){
        message + value._1 + " -> "
      } else if (value._2=="invocation") {
          if(message != initialMsg) {
            message + value._1 + " -> "
          } else
            value._1 +"->"
      }

    }

    def sendMsg() = {

    }

    def mergeMsg() = {

    }

    val sssp = finalGraph.pregel(initialMsg)(vprog, sendMsg, mergeMsg)*/


    /*val graph1 = Graph.fromEdges(edgesInv, "a")

    val graph2 = Graph.fromEdges(edgesNew, "a")

    */




    /*
    results in the form
    ((-2147467271,/home/alx/dpl/input/src/java/org/apache/fop/pdf/PDFXMode),(-1960642174,public static PDFXMode getValueOf(String)),declares method)
    ((-2147467271,/home/alx/dpl/input/src/java/org/apache/fop/pdf/PDFXMode),(-881418252,Some(public String getName()),declares method)
    ((-2147467271,/home/alx/dpl/input/src/java/org/apache/fop/pdf/PDFXMode),(-293337209,public String toString()),declares method)
    ((-2141090230,/home/alx/dpl/input/src/java/org/apache/fop/area/inline/InlineParent),(102230,get),invokes method)
    ((-2141090230,/home/alx/dpl/input/src/java/org/apache/fop/area/inline/InlineParent),(107876,max),invokes method)
    ((-2141090230,/home/alx/dpl/input/src/java/org/apache/fop/area/inline/InlineParent),(108114,min),invokes method)
     */

    /*
    ********** DEBUG **************
    val results = finalGraph.edges.count.toString + "\n" + finalGraph.edges.collect.take(5).mkString("\n")

    println(finalGraph.edges.count)
    println(finalGraph.edges.collect.take(5).mkString("\n"))


    println(results)
    */
  }
}