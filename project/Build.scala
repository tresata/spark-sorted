import sbt._
import sbt.Keys._
import sbtsparkpackage.SparkPackagePlugin.autoImport._

object ProjectBuild extends Build {
  lazy val project = Project(
    id = "root",
    base = file(".")
  ).settings(
    Seq(
      organization := "com.tresata",
      name := "spark-sorted",
      version := "1.0.0-SNAPSHOT",
      scalaVersion := "2.11.8",
      crossScalaVersions := Seq("2.10.6", "2.11.8"),
      javacOptions ++= Seq("-Xlint:unchecked", "-source", "1.7", "-target", "1.7"),
      scalacOptions ++= Seq("-unchecked", "-deprecation", "-target:jvm-1.7", "-feature", "-language:_"),
      // sbt-spark-package settings
      spName := "tresata/spark-sorted",
      sparkVersion := "2.0.0",
      sparkComponents += "core",
      spIncludeMaven := true,
      spAppendScalaVersion := true,
      // end sbt-spark-package settings
      licenses += "Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"),
      libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test",
      libraryDependencies += "com.novocode" % "junit-interface" % "0.11" % "test",
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
        <url>https://github.com/tresata/spark-sorted</url>
        <scm>
          <url>git@github.com:tresata/spark-sorted.git</url>
          <connection>scm:git:git@github.com:tresata/spark-sorted.git</connection>
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
