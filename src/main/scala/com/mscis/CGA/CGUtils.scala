package com.mscis.CGA

import java.nio.file.{Files, Path, Paths}
import java.util.stream.Collectors

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
      clsssI += meth.getDeclarationAsString(true,false,false) + "-----"
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

}
