package com.mscis.CGA

import java.nio.file.{Files, Path, Paths}
import java.util.stream.Collectors

import com.github.javaparser.JavaParser
import com.github.javaparser.ast.CompilationUnit
import com.github.javaparser.ast.body.{ClassOrInterfaceDeclaration, MethodDeclaration}
import com.github.javaparser.ast.expr.MethodCallExpr
import org.apache.spark.graphx._

import scala.collection.JavaConverters._
import scala.io.Source

/**
  * Created by ralex on 27/4/2017.
  */

object CGUtils {

  def getContext(dir: String): List[(String, String)] = {
    //val files = getListOfFiles(dir)
    //val fnc:List[(String,String)] = files.map(x => (x.getName.stripSuffix(".java"), Source.fromFile(x).mkString + "\n"))

    val files = getAllSourceFiles(dir)
    val fnc: List[(String, String)] = files.map(x => (x.toString.stripSuffix(".java"), Source.fromURI(x.toUri).mkString + "\n"))

    fnc
  }

  //get all files that end with .java by walking the directory

  def getAllSourceFiles(dir: String): List[Path] = {
    val d = Paths.get(dir)

    if (Files.exists(d) && Files.isDirectory(d)) {
      val fejava = Files.walk(d).collect(Collectors.toList()).asScala
      fejava.filter(x => x.toString.endsWith(".java") && !Files.isDirectory(x)).toList
    } else {
      List[Path]()
    }
  }

  def getInfo(javaContent: String): List[String] = {

    val cu: CompilationUnit = JavaParser.parse(javaContent)

    getInfoOutOfCU(cu)
  }

  def getInfoOutOfCU(cu: CompilationUnit):List[String] = {

    val classNodes = cu.getChildNodesByType(classOf[ClassOrInterfaceDeclaration]).asScala.toList
    val methodNodes = cu.getChildNodesByType(classOf[(MethodDeclaration)]).asScala.toList
    val invocNodes = cu.getChildNodesByType(classOf[MethodCallExpr]).asScala.toList

    var clsssI = ""

    for (cl <- classNodes) {
      clsssI += cl.getName + "-----"
    }
    clsssI +=":"

    for (meth <- methodNodes){
//      clsssI += meth.getDeclarationAsString(true,false,false) + "-----"
      clsssI += meth.getName + ";;;"
      clsssI += meth.getType + ";;;"
      clsssI += meth.getModifiers.toString + ";;;"
      clsssI += meth.getParameters.toArray.mkString(",")
      clsssI.stripSuffix(",")
      clsssI += "-----"
    }
    clsssI +=":"

    for (expr <- invocNodes){
      clsssI += expr.getName + "-----"
    }

    clsssI.split(":").toList

  }

  def vertexHash(name: String): VertexId = {
    name.toLowerCase.replace(" ", "").hashCode.toLong
  }

  case class declData(mName: String, mType: String = "No Type", mMods: String = "No mods", mPar: List[String] = List("No pars"))
  def getDeclInfo(declAsString: List[String]): List[declData] = {
    val declInfo: List[declData] = declAsString.map(x => {
      x.split(";;;").length match{
        case 4 =>
          val xName = x.split(";;;")(0)
          val xType = x.split(";;;")(1)
          val xMods = x.split(";;;")(2)
          val xPars = x.split(";;;")(3).split(",").toList
          declData(xName, xType, xMods, xPars)
        case 3 =>
          val xName = x.split(";;;")(0)
          val xType = x.split(";;;")(1)
          val xMods = x.split(";;;")(2)
          declData(xName, xType, xMods)
        case 2 =>
          val xName = x.split(";;;")(0)
          val xType = x.split(";;;")(1)
          declData(xName, xType)
        case 1 =>
          val xName = x.split(";;;")(0)
          declData(xName)
      }
    })
    declInfo
  }
}
