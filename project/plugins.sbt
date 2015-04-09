addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.7.4")

addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.0.0")

resolvers += "Spark Packages repo" at "https://dl.bintray.com/spark-packages/maven/"

addSbtPlugin("org.spark-packages" %% "sbt-spark-package" % "0.2.1")
