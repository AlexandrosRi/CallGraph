package com.mscis.CGA

import java.nio.file.{Files, Path, Paths}
import java.util.NoSuchElementException
import java.util.stream.Collectors

import com.github.javaparser.JavaParser
import com.github.javaparser.ast.{CompilationUnit, PackageDeclaration}
import com.github.javaparser.ast.body.{ClassOrInterfaceDeclaration, MethodDeclaration}
import com.github.javaparser.ast.expr.{MethodCallExpr, VariableDeclarationExpr}
import org.apache.spark.graphx._

import scala.collection.JavaConverters._
import scala.io.Source


object CGUtils {

  def getContext(dir: String): List[(String, String)] = {

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

  def getDecls(javaContent: String): List[MethodDeclaration] = {
    val cu: CompilationUnit = JavaParser.parse(javaContent)

    cu.getChildNodesByType(classOf[MethodDeclaration]).asScala.toList
  }

  def getInfoOutOfCU(cu: CompilationUnit):List[String] = {

    val classNodes = cu.getChildNodesByType(classOf[ClassOrInterfaceDeclaration]).asScala.toList
    val declNodes = cu.getChildNodesByType(classOf[(MethodDeclaration)]).asScala.toList
    val invocNodes = cu.getChildNodesByType(classOf[MethodCallExpr]).asScala.toList
    val fields = cu.getChildNodesByType(classOf[VariableDeclarationExpr]).asScala.toList

    var clPckg = ""
    try {
      cu.getPackageDeclaration.get() match {
        case t: PackageDeclaration =>
          clPckg += t.getName.toString
        case _ =>
      }
    }
    catch {
      case ex: NoSuchElementException =>
      case _: Throwable =>
    }

    var clsssI = ""

    for (cl <- classNodes) {
      clsssI += cl.getName + ":-----:"
    }
    clsssI +=":-:"

    for (decl <- declNodes){
      clsssI += decl.getName + ";;;"
      clsssI += decl.getType + ";;;"
      clsssI += decl.getModifiers.toString + ";;;"
      clsssI += decl.getParameters.toArray.mkString(",")
      clsssI.stripSuffix(",")
      clsssI += ";;;" + decl.getParameters.size() + ";;;"
      clsssI += decl.getBegin.toString + ";;;"
      decl.getParentNode.get() match {
        case t: ClassOrInterfaceDeclaration =>
          clsssI += t.getNameAsString + ";;;"
        case _ =>
          clsssI += " ;;;"
      }
      clsssI += clPckg
      clsssI += ":-----:"
    }
    clsssI +=":-:"

    for (invoc <- invocNodes){
      clsssI += invoc.getName.toString + ";;;"
      clsssI += invoc.getScope.toString + ";;;"
      clsssI += invoc.getArguments.asScala.toList.size + ";;;"
      clsssI += invoc.getBegin.toString + ";;;"
      if (invoc.getScope.isPresent){
        for(field <- fields) {
          if (field.getVariable(0).getNameAsString == invoc.getScope.get().toString) {
            clsssI += field.getElementType.toString
          }
        }
        clsssI += ";;;"
      }
      else clsssI += " ;;;"
      clsssI += clPckg
      clsssI += ":-----:"
    }

    clsssI.split(":-:").toList
  }

  def vertexHash(name: String): VertexId = {
    name.toLowerCase.replace(" ", "").hashCode.toLong
  }

  case class declData(oCName: String, mName: String, mType: String = "No Type", mMods: String = "No mods",
                      mPar: List[String] = List("No pars"), mParNum: Int = 0, mPos: String = "0", mParent: String = "0",
                      mPckg: String = "No Pckg")
  def getDeclInfo(ogClName: String, declAsString: List[String]): List[declData] = {
    val declInfo: List[declData] = declAsString.map(x => {
      x.split(";;;").length match{
        case 8 =>
          val xName = x.split(";;;")(0)
          val xType = x.split(";;;")(1)
          val xMods = x.split(";;;")(2)
          val xPars = x.split(";;;")(3).split(",").toList
          val xParNum = x.split(";;;")(4).toInt
          val xPos = x.split(";;;")(5)
          val xParent = x.split(";;;")(6)
          val xPackage = x.split(";;;")(7)
          declData(ogClName, xName, xType, xMods, xPars, xParNum, xPos, xParent, xPackage)
        case 7 =>
          val xName = x.split(";;;")(0)
          val xType = x.split(";;;")(1)
          val xMods = x.split(";;;")(2)
          val xPars = x.split(";;;")(3).split(",").toList
          val xParNum = x.split(";;;")(4).toInt
          val xPos = x.split(";;;")(5)
          val xParent = x.split(";;;")(6)
          declData(ogClName, xName, xType, xMods, xPars, xParNum, xPos, xParent)
        case 6 =>
          val xName = x.split(";;;")(0)
          val xType = x.split(";;;")(1)
          val xMods = x.split(";;;")(2)
          val xPars = x.split(";;;")(3).split(",").toList
          val xParNum = x.split(";;;")(4).toInt
          val xPos = x.split(";;;")(5)
          declData(ogClName, xName, xType, xMods, xPars, xParNum, xPos)
        case 5 =>
          val xName = x.split(";;;")(0)
          val xType = x.split(";;;")(1)
          val xMods = x.split(";;;")(2)
          val xPars = x.split(";;;")(3).split(",").toList
          val xParNum = x.split(";;;")(4).toInt
          declData(ogClName, xName, xType, xMods, xPars, xParNum)
        case 4 =>
          val xName = x.split(";;;")(0)
          val xType = x.split(";;;")(1)
          val xMods = x.split(";;;")(2)
          val xPars = x.split(";;;")(3).split(",").toList
          declData(ogClName, xName, xType, xMods, xPars)
        case 3 =>
          val xName = x.split(";;;")(0)
          val xType = x.split(";;;")(1)
          val xMods = x.split(";;;")(2)
          declData(ogClName, xName, xType, xMods)
        case 2 =>
          val xName = x.split(";;;")(0)
          val xType = x.split(";;;")(1)
          declData(ogClName, xName, xType)
        case 1 =>
          val xName = x.split(";;;")(0)
          declData(ogClName, xName)
      }
    })
    declInfo
  }

  case class invocData(oCName: String, iName: String = "No Name", iScope: String ="", argsNum: Int = 0,
                       pos: String = "0", retType: String ="", iPckg: String ="No Pckg")
  def getInvocInfo(ogClName: String, invocAsString: List[String]): List[invocData] = {
    val invocInfo: List[invocData] = invocAsString.map(x => {
      x.split(";;;").length match {
        case 6 =>
          val xName = x.split (";;;")(0)
          val xScope = x.split (";;;")(1)
          val xArgs = x.split (";;;")(2).toInt
          val xPos = x.split (";;;")(3)
          val xRetType = x.split(";;;")(4)
          val xPackage = x.split(";;;")(5)
          invocData(ogClName, xName, xScope, xArgs, xPos, xRetType, xPackage)
        case 5 =>
          val xName = x.split (";;;")(0)
          val xScope = x.split (";;;")(1)
          val xArgs = x.split (";;;")(2).toInt
          val xPos = x.split (";;;")(3)
          val xRetType = x.split(";;;")(4)
          invocData(ogClName, xName, xScope, xArgs, xPos, xRetType)
        case 4 =>
          val xName = x.split (";;;")(0)
          val xScope = x.split (";;;")(1)
          val xArgs = x.split (";;;")(2).toInt
          val xPos = x.split (";;;")(3)
          invocData(ogClName, xName, xScope, xArgs, xPos)
        case 3 =>
          val xName = x.split (";;;") (0)
          val xScope = x.split (";;;") (1)
          val xArgs = x.split (";;;")(2).toInt
          invocData(ogClName, xName, xScope, xArgs)
        case 2 =>
          val xName = x.split (";;;") (0)
          val xScope = x.split (";;;") (1)
          invocData(ogClName, xName, xScope)
        case 1 =>
          val xName = x.split (";;;") (0)
          invocData(ogClName, xName)
      }
    })
    invocInfo
  }

}
