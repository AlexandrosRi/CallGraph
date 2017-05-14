package com.mscis.CGA

import com.github.javaparser.JavaParser
import com.github.javaparser.ast.CompilationUnit
import com.mscis.CGA.CGUtils._
import org.apache.spark.{SparkConf, SparkContext, graphx}
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
//import org.apache.spark.sql._
//this is a comment

object CGGen {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("CGGen")
    val sc = new SparkContext(conf)

    val hdfsServ = "192.168.1.9"
    val hdfspath = "hdfs://" + hdfsServ + ":9000/user/hduser/"
    val inputSM = "/home/alx/dpl/input"
    val outputSM = hdfspath +  "output"

    def getInfo(javaContent: String): List[String] = {

      val cu: CompilationUnit = JavaParser.parse(javaContent)

      getInfoOutOfCU(cu)
    }

    val inputdata = sc.parallelize(getContext(inputSM))

    case class InfoData(classFQName: String, declaredMethods: List[String], invokedMethods: List[String])

    val inform = inputdata.map(x => {
      val a = x._1
      val b = getInfo(x._2).lift(1).toString.split("-----").toList
      val c = getInfo(x._2).lift(2).toString.split("-----").toList
      InfoData(a,b,c)
    })

    println(inform.count())

    val classVertices = inform.map(x => (vertexHash(x.classFQName), x.classFQName))

    val methodVerticesList = inform.flatMap(x => x.declaredMethods)
    val methodVertices:RDD[(graphx.VertexId, String)] = methodVerticesList.map(x => (vertexHash(x), x))

    val invocVerticesList = inform.flatMap(x => x.invokedMethods)
    val invocVertices:RDD[(graphx.VertexId, String)] = invocVerticesList.map(x => (vertexHash(x), x))

    val cmVertices = classVertices.union(methodVertices)
    val allVertices = cmVertices.union(invocVertices)


    val edgesDecl: RDD[Edge[String]] = inform.flatMap { x =>
      val srcVid = vertexHash(x.classFQName)
      x.declaredMethods.map { dMeth =>
        val dstVid = vertexHash(dMeth)
        Edge(srcVid, dstVid, "declares method")
      }
    }

    val edgesInv: RDD[Edge[String]] = inform.flatMap { x =>
      val srcVid = vertexHash(x.classFQName)
      x.invokedMethods.map({ iMeth =>
        val dstVid = vertexHash(iMeth)
        Edge(srcVid, dstVid, "invokes method")
      })
    }

    val edges = edgesDecl.union(edgesInv)

    val defaultCon = "a"



    val finalGraph = Graph(allVertices, edges, defaultCon)


    finalGraph.triplets.saveAsTextFile(outputSM)

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