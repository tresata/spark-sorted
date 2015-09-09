import sbt._
import sbt.Keys._
import sbtsparkpackage.SparkPackagePlugin.autoImport._
import net.virtualvoid.sbt.graph.Plugin._

object ProjectBuild extends Build {
  lazy val project = Project(
    id = "root",
    base = file("."),
    settings = Project.defaultSettings ++ graphSettings ++ Seq(
      organization := "com.tresata",
      name := "spark-sorted",
      version := "0.4.0-SNAPSHOT",
      scalaVersion := "2.10.4",
      crossScalaVersions := Seq("2.10.4", "2.11.7"),
      // sbt-spark-package settings
      spName := "tresata/spark-sorted",
      sparkVersion := "1.5.0",
      sparkComponents += "core",
      spIncludeMaven := true,
      spAppendScalaVersion := true,
      // end sbt-spark-package settings
      licenses += "Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"),
      libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.5" % "test",
      libraryDependencies += "com.novocode" % "junit-interface" % "0.10" % "test",
      libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.12.4" % "test",
      testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oF"),
      testOptions += Tests.Argument(TestFrameworks.JUnit, "-v", "-a"),
      publishMavenStyle := true,
      pomIncludeRepository := { x => false },
      publishArtifact in Test := false,
      publishTo := {
        val nexus = "https://oss.sonatype.org/"
        if (isSnapshot.value)
          Some("snapshots" at nexus + "content/repositories/snapshots")
        else
          Some("releases"  at nexus + "service/local/staging/deploy/maven2")
      },
      credentials += Credentials(Path.userHome / ".m2" / "credentials_sonatype"),
      credentials += Credentials(Path.userHome / ".m2" / "credentials_spark_packages"),
      pomExtra := (
        <url>https://github.com/tresata/spark-scalding</url>
        <scm>
          <url>git@github.com:tresata/spark-scalding.git</url>
          <connection>scm:git:git@github.com:tresata/spark-scalding.git</connection>
        </scm>
        <developers>
          <developer>
            <id>koertkuipers</id>
            <name>Koert Kuipers</name>
            <url>https://github.com/koertkuipers</url>
          </developer>
        </developers>)
    )
  )
}
