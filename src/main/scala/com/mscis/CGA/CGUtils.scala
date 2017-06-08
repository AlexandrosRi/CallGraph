package com.mscis.CGA

import java.nio.file.{Files, Path, Paths}
import java.util.stream.Collectors

import com.github.javaparser.JavaParser
import com.github.javaparser.ast.CompilationUnit
import com.github.javaparser.ast.body.{ClassOrInterfaceDeclaration, MethodDeclaration}
import com.github.javaparser.ast.expr.{MethodCallExpr, VariableDeclarationExpr}
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

  def getInfo(javaContent: String): compUnitInfo = {

    val cu: CompilationUnit = JavaParser.parse(javaContent)

    getInfoOutOfCU(cu)
  }

  def getDecls(javaContent: String): List[MethodDeclaration] = {
    val cu: CompilationUnit = JavaParser.parse(javaContent)

    cu.getChildNodesByType(classOf[MethodDeclaration]).asScala.toList
  }

  case class compUnitInfo(classNodes: List[ClassOrInterfaceDeclaration], declNodes: List[MethodDeclaration],
                          invocNodes: List[MethodCallExpr], varNodes: List[VariableDeclarationExpr])

  def getInfoOutOfCU(cu: CompilationUnit):compUnitInfo = {

    val classNodes = cu.getChildNodesByType(classOf[ClassOrInterfaceDeclaration]).asScala.toList
    val declNodes = cu.getChildNodesByType(classOf[(MethodDeclaration)]).asScala.toList
    val invocNodes = cu.getChildNodesByType(classOf[MethodCallExpr]).asScala.toList
    val fieldNodes = cu.getChildNodesByType(classOf[VariableDeclarationExpr]).asScala.toList

    compUnitInfo(classNodes, declNodes, invocNodes, fieldNodes)
  }

  def declVertexHash(decl: MethodDeclaration): VertexId = {

    val str = decl.getModifiers.toString + decl.getType.toString + decl.getName.toString +
              decl.getParameters.toArray.mkString(",").stripSuffix(",") +
              decl.getParameters.size().toString

    vertexHash(str)
  }

  def invocVertexHash(invoc: MethodCallExpr): VertexId = {

    val str = invoc.getNameAsString + invoc.getScope.toString + invoc.getArguments.size() +
              invoc.getBegin.toString

    vertexHash(str)
  }

  def vertexHash(name: String): VertexId = {
    name.toLowerCase.replace(" ", "").hashCode.toLong
  }

  def checkConnections(invoc: MethodCallExpr, decl: (String, Int, String, Long),
                       vars: List[VariableDeclarationExpr]): Boolean = {
    val nameCheck = invoc.getNameAsString == decl._1
    val parNumCheck = decl._2 == invoc.getArguments.size

    if (invoc.getScope.isPresent){



      val elemTypeCheck = checkFieldType(invoc, decl._3, vars)

      nameCheck && parNumCheck && elemTypeCheck
    }
    else nameCheck && parNumCheck

  }

  def checkFieldType(invoc: MethodCallExpr, declParent: String,
                     vars: List[VariableDeclarationExpr]): Boolean = {
    var elemTypeCheck = false
    val vDecl = vars.iterator
    while (vDecl.hasNext && !elemTypeCheck) {
      val vDec = vDecl.next()
      if (vDec.getVariable(0).getNameAsString == invoc.getScope.get().toString) {
        elemTypeCheck = declParent == vDec.getElementType.toString

      }
    }
    elemTypeCheck
  }

}
