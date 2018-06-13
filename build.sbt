lazy val root = (project in file(".")).settings(
  organization := "com.tresata",
  name := "spark-sorted",
  version := "1.6.0-SNAPSHOT",
  scalaVersion := "2.11.8",
  javacOptions in (Compile, compile) ++= Seq("-Xlint:unchecked", "-source", "1.8", "-target", "1.8"),
  scalacOptions ++= Seq("-unchecked", "-deprecation", "-target:jvm-1.8", "-feature", "-language:_", "-Xlint:-package-object-classes,-adapted-args,_",
    "-Ywarn-unused-import", "-Ywarn-dead-code", "-Ywarn-value-discard", "-Ywarn-unused"),
  scalacOptions in (Test, compile) := (scalacOptions in (Test, compile)).value.filter(_ != "-Ywarn-value-discard").filter(_ != "-Ywarn-unused"),
  scalacOptions in (Compile, console) := (scalacOptions in (Compile, console)).value.filter(_ != "-Ywarn-unused-import"),
  scalacOptions in (Test, console) := (scalacOptions in (Test, console)).value.filter(_ != "-Ywarn-unused-import"),
  licenses += "Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"),
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.3.1" % "provided",
    "org.apache.spark" %% "spark-sql" % "2.3.1" % "provided",
    "org.scalatest" %% "scalatest" % "3.0.5" % "test",
    "com.novocode" % "junit-interface" % "0.11" % "test",
    "org.scalacheck" %% "scalacheck" % "1.14.0" % "test"
  ),
  testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oF"),
  testOptions += Tests.Argument(TestFrameworks.JUnit, "-v", "-a"),
  publishMavenStyle := true,
  pomIncludeRepository := { x => false },
  publishArtifact in Test := false,
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value)
      Some("snapshots" at "http://server02.tresata.com:8081/artifactory/oss-libs-snapshot-local")
    else
      Some("releases"  at nexus + "service/local/staging/deploy/maven2")
  },
  credentials += Credentials(Path.userHome / ".m2" / "credentials_sonatype"),
  credentials += Credentials(Path.userHome / ".m2" / "credentials_artifactory"),
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
