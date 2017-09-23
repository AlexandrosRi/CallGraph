package com.mscis.CGA

import com.mscis.CGA.CGUtils._
import org.apache.spark.{SparkConf, SparkContext, graphx}
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.log4j.{Level, LogManager}


object CGGen {
  def main(args: Array[String]) {

    val log = LogManager.getRootLogger
    log.setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("CGGen")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    val inputFop = "/home/alx/dpl/input/fopp"
    val inputHadoop = "/home/alx/dpl/hdp-trnk"
    val inputIntlj = "/home/alx/dpl/intlj"
    val inputSM = inputFop


    val hdfsServ = "172.16.10.34" //lab hadoop server
//    val hdfsServ = "192.168.1.9"  //home hadoop server
    val hdfsPath = "hdfs://" + hdfsServ + ":9000/user/alx/"
    val output = hdfsPath + "output"

    log.info("got Info!")

    val inputdata = sc.parallelize(getContext(inputSM), 16)
    log.info("serialized Info!")

    case class InfoData(classFQName: (String, String), declaredMethods: List[CGUtils.declData], invokedMethods: List[CGUtils.invocData])//, declarations: List[MethodDeclaration])

    val inform = inputdata.map(x => {

      val fullName = x._1
      val name = fullName.drop(fullName.lastIndexOf("/"))
      val sourceInfo = getInfo(x._2)
      val mDeclarations = getDeclInfo(fullName, sourceInfo.lift(1).toString.stripSuffix(")").split(":-----:").toList)
      val mInvocations = getInvocInfo(fullName, sourceInfo.lift(2).toString.stripSuffix(")").split(":-----:").toList)
      InfoData((fullName,name), mDeclarations, mInvocations)
    })

    val classVertices:RDD[(graphx.VertexId, (String,String))] = inform.map(x => (vertexHash(x.classFQName._1), (x.classFQName._2, "class")))

    val methodVerticesList = inform.flatMap(x => x.declaredMethods)

    val collectedMethodVertices = methodVerticesList.collect

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
          var isDeclared = true
          if (!(iMeth.iScope == "Optional.empty" || iMeth.iScope.contains("."))){
            isDeclared = p.mParent == iMeth.retType
          }

          val visCheck = (p.mMods == "[PUBLIC]") || (p.mMods == "[PRIVATE]" && (p.oCName == iMeth.oCName))

          val pkgCheck = p.mPckg == iMeth.iPckg



          nameCheck && parNumCheck && isDeclared && pkgCheck && visCheck
        })
        pairss.map(c => {
          val dstVid = vertexHash(c.mMods+c.mType+c.mName+c.mPar+c.mPos)
          Edge(srcVid, dstVid, "declared")
        })

      }
    }
    edgesDecl.map(x => x.srcId)

    val edges = edgesDecl.union(edgesInv)

    val defaultCon = ("a","A")

    val finalGraph:Graph[(String,String), String] = Graph(allVertices, edges, defaultCon)

  /*  Statistics
      val invocToDecl = finalGraph.subgraph(x => x.srcAttr._2 == "invocation")
      val invocOutDeg = invocToDecl.outDegrees
      val invocAndCount: (Int, Int) = invocOutDeg.map(x => (x._2 , 1)).reduce((a,b) => (a._1+b._1, a._2+b._2))
      log.info("invocOut: " + invocAndCount._1 + "\ncount: " + invocAndCount._2)
  */

    val fx = finalGraph.triplets
    fx.saveAsTextFile(output+"/graph")

    //DEBUG
    /*
    println("\n\n\nAbout to print dstVids")
    dstVids.foreach(println(_))
    println("Just printed dstVids\n\n\n")


    val pdfPagesEdges = fx.filter(x => x.srcId == 2139746722)
    val dstVids = pdfPagesEdges.map(x => x.dstId.toLong).collect()
    val checkGraph = fx.filter(x => dstVids.contains(x.srcId))


    val findataset : RDD[EdgeTriplet[(String,String), String]] = pdfPagesEdges.union(checkGraph)
    val dotFile = findataset.map(x => x.srcAttr._1 + " -> " + x.dstAttr._1 + "\n")
    dotFile.saveAsTextFile(output + "/digraph")
    findataset.saveAsTextFile(output + "/test")


    results in the form
    ((-2147467271,/home/alx/dpl/input/src/java/org/apache/fop/pdf/PDFXMode),(-1960642174,public static PDFXMode getValueOf(String)),declares method)
    ((-2141090230,/home/alx/dpl/input/src/java/org/apache/fop/area/inline/InlineParent),(108114,min),invokes method)
   */
  }
}