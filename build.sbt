lazy val root = (project in file(".")).
  settings(
    name := "noaa",
    version := "1.0",
    scalaVersion := "2.10.6",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.1"
    javaOptions += "-Xmx8g"
  )