package com.mscis.CGA

import com.github.javaparser.ast.body.{ClassOrInterfaceDeclaration, MethodDeclaration}
import com.github.javaparser.ast.expr.{MethodCallExpr, VariableDeclarationExpr}
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


    case class InfoData(classFQName: (String, String), declaredMethods: List[MethodDeclaration],
                        invokedMethods: List[MethodCallExpr], declaredVars: List[VariableDeclarationExpr])

    val inform = inputdata.map(x => {

      val fullName = x._1
      val name = fullName.drop(fullName.lastIndexOf("/"))
      val sourceInfo = getInfo(x._2)
      val mDeclarations = sourceInfo.declNodes
      val mInvocations = sourceInfo.invocNodes
      val mFields = sourceInfo.varNodes
      InfoData((fullName,name), mDeclarations, mInvocations, mFields)

      //      val declarations  = getDecls(x._2)
//      InfoData((fullName,name), mDeclarations, mInvocations, declarations)

    })

    val classVertices:RDD[(graphx.VertexId, (String,String))] = inform.map(x => (vertexHash(x.classFQName._1), (x.classFQName._2, "class")))

    val methodVerticesList:RDD[MethodDeclaration] = inform.flatMap(x => x.declaredMethods)

    val collectedMethodVertices = methodVerticesList.map(x => {
      val hash = declVertexHash(x)
      val name = x.getNameAsString
      val num = x.getParameters.size
      var parent = ""
      x.getParentNode.get() match {
        case t : ClassOrInterfaceDeclaration =>
          parent = t.getNameAsString
        case _ =>
      }

      (name, num, parent, hash)

    }).collect

//    val methodObjVertices = inform.flatMap(x => x.declarations)

//    val test: RDD[(graphx.VertexId, MethodDeclaration)]= methodObjVertices.map(x => (vertexHash(x.getName.toString), x))

    val methodVertices:RDD[(graphx.VertexId, (String,String))] = methodVerticesList.map(x => (declVertexHash(x), (x.getNameAsString,"declaration")))

    val invocVerticesList = inform.flatMap(x => x.invokedMethods)

    val invocVertices:RDD[(graphx.VertexId, (String,String))] = invocVerticesList.map(x => (invocVertexHash(x), (x.getNameAsString,"invocation")))

    val cmVertices:RDD[(graphx.VertexId, (String,String))]  = classVertices.union(methodVertices)

    val allVertices:RDD[(graphx.VertexId, (String,String))]  = cmVertices.union(invocVertices)

    val edgesInv: RDD[Edge[String]] = inform.flatMap { x =>
      val srcVid = vertexHash(x.classFQName._1)
      x.invokedMethods.map({ iMeth =>
        val dstVid = invocVertexHash(iMeth)
        Edge(srcVid, dstVid, "invokes method")
      })
    }



    val edgesDecl: RDD[Edge[String]] = inform.flatMap{ x =>

      x.invokedMethods.flatMap{ iMeth =>
        val srcVid: graphx.VertexId = invocVertexHash(iMeth)
        val pairss = collectedMethodVertices.filter(p => checkConnections(iMeth, p, x.declaredVars))
        pairss.map(c => {
          val dstVid = c._4
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