package com.mscis.CGA

import com.github.javaparser.ast.body.MethodDeclaration
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

//    val hdfsServ = "172.16.10.34" //lab hadoop server
    val hdfsServ = "192.168.1.9"  //home hadoop server
    val hdfsPath = "hdfs://" + hdfsServ + ":9000/user/alx/"
    val output = hdfsPath + "output"

//    val output = "/home/alx/dpl/output"
//    val output3 = "/home/alx/dpl/output3"
    log.info("got Info!")

    val inputdata = sc.parallelize(getContext(inputSM))
    log.info("serialized Info!")


    case class InfoData(classFQName: (String, String), declaredMethods: List[CGUtils.declData], invokedMethods: List[CGUtils.invocData])//, declarations: List[MethodDeclaration])

    val inform = inputdata.map(x => {

      val fullName = x._1
      val name = fullName.drop(fullName.lastIndexOf("/"))
      val sourceInfo = getInfo(x._2)
      val mDeclarations = getDeclInfo(sourceInfo.lift(1).toString.stripSuffix(")").split("-----").toList)
      val mInvocations = getInvocInfo(sourceInfo.lift(2).toString.stripSuffix(")").split("-----").toList)
      InfoData((fullName,name), mDeclarations, mInvocations)

      //      val declarations  = getDecls(x._2)
//      InfoData((fullName,name), mDeclarations, mInvocations, declarations)

    })

    val classVertices:RDD[(graphx.VertexId, (String,String))] = inform.map(x => (vertexHash(x.classFQName._1), (x.classFQName._2, "class")))

    val methodVerticesList = inform.flatMap(x => x.declaredMethods)

    val collectedMethodVertices = methodVerticesList.collect

//    val methodObjVertices = inform.flatMap(x => x.declarations)

//    val test: RDD[(graphx.VertexId, MethodDeclaration)]= methodObjVertices.map(x => (vertexHash(x.getName.toString), x))

    val methodVertices:RDD[(graphx.VertexId, (String,String))] = methodVerticesList.map(x => (vertexHash(x.mMods+x.mType+x.mName+x.mPar+x.mPos), (x.mName,"declaration")))

    val invocVerticesList = inform.flatMap(x => x.invokedMethods)

    val invocVertices:RDD[(graphx.VertexId, (String,String))] = invocVerticesList.map(x => (vertexHash(x.iName+x.iScope+x.argsNum+x.pos), (x.iName,"invocation")))

    val cmVertices:RDD[(graphx.VertexId, (String,String))]  = classVertices.union(methodVertices)

    val allVertices:RDD[(graphx.VertexId, (String,String))]  = cmVertices.union(invocVertices)

    val edgesInv: RDD[Edge[String]] = inform.flatMap { x =>
      val srcVid = vertexHash(x.classFQName._1)
      x.invokedMethods.map({ iMeth =>
        val dstVid = vertexHash(iMeth.iName+iMeth.iScope+iMeth.argsNum+iMeth.pos)
        Edge(srcVid, dstVid, "invokes method")
      })
    }

    val edgesDecl: RDD[Edge[String]] = inform.flatMap{ x=>
      x.invokedMethods.flatMap{ iMeth =>
        val srcVid: graphx.VertexId = vertexHash(iMeth.iName+iMeth.iScope+iMeth.argsNum+iMeth.pos)
        val pairss = collectedMethodVertices.filter(p => {
          val nameCheck = p.mName == iMeth.iName
          val parNumCheck = p.mParNum == iMeth.argsNum
          nameCheck && parNumCheck
        })
        pairss.map(c => {
          val dstVid = vertexHash(c.mMods+c.mType+c.mName+c.mPar+c.mPos)
          Edge(srcVid, dstVid, "declared")
        })
      }
    }
    //log.info("edges Decl count: "+edgesDecl.count)

    val edges = edgesDecl.union(edgesInv)

    val defaultCon = ("a","A")

    val finalGraph:Graph[(String,String), String] = Graph(allVertices, edges, defaultCon)
    val fx = finalGraph.triplets
    fx.saveAsTextFile(output+"/graph")

    val pdfPagesEdges = fx.filter(x => x.srcId == 2139746722)
    val dstVids = pdfPagesEdges.map(x => x.dstId.toLong).collect()

    //DEBUG
    /*
    println("\n\n\nAbout to print dstVids")
    dstVids.foreach(println(_))
    println("Just printed dstVids\n\n\n")
    */

    val checkGraph = fx.filter(x => dstVids.contains(x.srcId))

    // TODO: checkgraph.count

    val findataset : RDD[EdgeTriplet[(String,String), String]] = pdfPagesEdges.union(checkGraph)
    val dotFile = findataset.map(x => x.srcAttr._1 + " -> " + x.dstAttr._1 + "\n")
    dotFile.saveAsTextFile(output + "/digraph")
    findataset.saveAsTextFile(output + "/test")

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