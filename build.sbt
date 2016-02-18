lazy val root = (project in file(".")).
  settings(
    name := "hello",
    version := "1.0",
    scalaVersion := "2.11.5",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0"
  )