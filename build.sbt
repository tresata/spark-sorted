lazy val root = (project in file(".")).settings(
  organization := "com.tresata",
  name := "spark-sorted",
  version := "2.6.0-SNAPSHOT",
  scalaVersion := "2.12.15",
  crossScalaVersions := Seq("2.12.15", "2.13.8"),
  Compile / compile / javacOptions ++= Seq("-Xlint:unchecked", "-source", "1.8", "-target", "1.8"),
  scalacOptions ++= Seq("-unchecked", "-deprecation", "-target:jvm-1.8", "-feature", "-language:_", "-Xlint:-package-object-classes,-adapted-args,_",
    "-Ywarn-unused:-imports", "-Ywarn-dead-code", "-Ywarn-value-discard", "-Ywarn-unused"),
  Test / compile / scalacOptions := (Test / compile / scalacOptions).value.filter(_ != "-Ywarn-value-discard").filter(_ != "-Ywarn-unused"),
  Compile / console / scalacOptions := (Compile / console / scalacOptions).value.filter(_ != "-Ywarn-unused-import"),
  Test / console / scalacOptions := (Test / console / scalacOptions).value.filter(_ != "-Ywarn-unused-import"),
  licenses += "Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"),
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "3.4.1" % "provided",
    "org.apache.spark" %% "spark-sql" % "3.4.1" % "provided",
    "org.scalatest" %% "scalatest-funspec" % "3.2.16" % "test",
    "org.scalatestplus" %% "scalacheck-1-17" % "3.2.16.0" % "test",
    "com.novocode" % "junit-interface" % "0.11" % "test",
    "org.scalacheck" %% "scalacheck" % "1.17.0" % "test"
  ),
  Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oF"),
  testOptions += Tests.Argument(TestFrameworks.JUnit, "-v", "-a"),
  publishMavenStyle := true,
  pomIncludeRepository := { x => false },
  Test / publishArtifact := false,
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value)
      Some("snapshots" at "https://server02.tresata.com:8084/artifactory/oss-libs-snapshot-local")
    else
      Some("releases" at nexus + "service/local/staging/deploy/maven2")
  },
  credentials += Credentials(Path.userHome / ".m2" / "credentials_sonatype"),
  credentials += Credentials(Path.userHome / ".m2" / "credentials_artifactory"),
  Global / useGpgPinentry := true,
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
