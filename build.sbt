ThisBuild / scalaVersion := "2.13.16"
ThisBuild / version := "0.1.0"
name := "spark-migration-interview"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "4.0.1"
Compile / run / fork := true
Test / fork := true
Test / parallelExecution := false
val sparkOptions = Seq(
  "-Xmx2g",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-exports=java.base/sun.security.action=ALL-UNNAMED",
  "-Dio.netty.tryReflectionSetAccessible=true"
)
Compile / run / javaOptions ++= sparkOptions
Test / javaOptions ++= sparkOptions
Compile / run / envVars += "SPARK_LOCAL_IP" -> "127.0.0.1"
Test / envVars += "SPARK_LOCAL_IP" -> "127.0.0.1"
