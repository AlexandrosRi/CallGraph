package com.mscis.CGA

import com.github.javaparser.JavaParser
import com.github.javaparser.ast.CompilationUnit
import com.mscis.CGA.CGUtils._
import org.apache.spark.{SparkConf, SparkContext, graphx}
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.log4j.{Level, LogManager}
//import org.apache.spark.sql._
//this is a comment

object CGGen {
  def main(args: Array[String]) {

    val log = LogManager.getRootLogger
    log.setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("CGGen")
    val sc = new SparkContext(conf)
    val inputSM = "/home/alx/dpl/input"
    val outputSM = "/home/alx/dpl/output"

    def getInfo(javaContent: String): List[String] = {

      val cu: CompilationUnit = JavaParser.parse(javaContent)

      getInfoOutOfCU(cu)
    }

    log.info("got Info!")

    val inputdata = sc.parallelize(getContext(inputSM))

    log.info("serialized Info!")

    case class InfoData(classFQName: String, declaredMethods: List[String], invokedMethods: List[String])

    val inform = inputdata.map(x => {

      val a = x._1
      val helper = getInfo(x._2)
      val b = helper.lift(1).toString.split("-----").toList
      val c = helper.lift(2).toString.split("-----").toList
      InfoData(a,b,c)

    })

    val classVertices = inform.map(x => (vertexHash(x.classFQName), x.classFQName))

    val methodVerticesList = inform.flatMap(x => x.declaredMethods)

    val methodVertices:RDD[(graphx.VertexId, String)] = methodVerticesList.map(x => (vertexHash(x), x))

    val invocVerticesList = inform.flatMap(x => x.invokedMethods)

    val invocVertices:RDD[(graphx.VertexId, String)] = invocVerticesList.map(x => (vertexHash(x), x))

    val cmVertices = classVertices.union(methodVertices)

    val allVertices = cmVertices.union(invocVertices)

    val edgesInv: RDD[Edge[String]] = inform.flatMap { x =>
      val srcVid = vertexHash(x.classFQName)
      x.invokedMethods.map({ iMeth =>
        val dstVid = vertexHash(iMeth)
        Edge(srcVid, dstVid, "invokes method")

      })
    }

    val edgesNew: RDD[Edge[String]] = inform.flatMap{ x=>
      x.invokedMethods.flatMap{ iMeth =>
        val srcVid = vertexHash(iMeth)
        val pairss = x.declaredMethods.filter(p => p.contains(iMeth))
        pairss.map(c => Edge(srcVid, vertexHash(c), "declared"))

      }

    }

//    val edgesDecl: RDD[Edge[String]] = inform.flatMap { x =>
//      x.invokedMethods.map({ iMeth =>
//        val srcVid = vertexHash(iMeth)
//        val conVertices = x.declaredMethods.filter(a => a.contains(iMeth))
//        val fin:List[graphx.VertexId] = conVertices.map( b => vertexHash(b))
//     //   fin.map(c => Edge(srcVid, c, "a"))
//      })
//    }


//    case class pPair(src:Long, dst:Long)
//    val edgesNew:RDD[(graphx.VertexId,graphx.VertexId)] = invocVertices.flatMap { x =>
//      val id = x._1
//      val edges:RDD[(String)] = methodVerticesList.filter(a => a.contains(x._2))
//      val pairs:RDD[(graphx.VertexId, graphx.VertexId)] = edges.map(p => (id, vertexHash(p)))
//
//
//      List((id,2L),(id,3L))
////     methodVertices.filter(a => a._2.contains(x._2)).map(p => (x._1, p._1))
////      val edges:RDD[(graphx.VertexId, String)] = methodVertices.filter(a => a._2.contains(x._2))
////
//    }

//    val edgesDecl: RDD[Edge[String]] = edgesNew.map(ed => Edge(ed._1,ed._2,"declared method"))
    //    val edgesDecl: RDD[Edge[String]] = invocVertices.map{e =>
//      val keep = methodVertices.filter(case (i,n) => i==e._2)
//
//      /*
//      αυτό που πρέπει να κάνω είναι βρω τα pairs όχι με RDD και μετά να κάνω το RDD
//       */
//
//    }


    //    val edgesDecl: RDD[Edge[String]] = inform.flatMap { y =>
    //        val srcVid = vertexHash(y.classFQName)
    //        y.declaredMethods.map( { dMeth =>
    //          val dstVid = vertexHash(dMeth)
    //          Edge(srcVid, dstVid, "declares method")
    //        })
    //    }


//
//    val carVertices = invocVertices.cartesian(methodVertices)
////    val superVertices = new VertexRDD[String](invocVertices)
////    val joiVertices = invocVertices.innerJoin(methodVertices)
//
//    val conVertices = carVertices.filter(x => x._1._2.contains(x._2._2))
//
//    val edgesNew = conVertices.map(x => Edge(x._1._1, x._2._1 ,"declared in"))

//    val
//
//    val edgesNew:RDD[Edge[String]] = invocVertices.flatMap{ y =>
//      val srcVid = y._1
//      val filteredMeth = methodVertices.filter(c => y._2.contains(c._2))
//      val ex = filteredMeth.map{ fMeth =>
//        val dstVid = vertexHash(fMeth._2)
//        dstVid
//        //Edge(srcVid, dstVid, "declares method")
//      }
//      val res = ex.foreach(x => Edge(srcVid, x, "a"))
//    }






//    val edges = edgesDecl.union(edgesInv)

    val edges2 = edgesNew.union(edgesInv)

    val defaultCon = "a"

//        val finalGraph = Graph(allVertices, edges, defaultCon)

    val finalGraph = Graph(allVertices, edges2, defaultCon)

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