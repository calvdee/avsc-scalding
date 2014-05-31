name := "AVSC"

version := "1.0"

libraryDependencies ++= Seq(
  "com.twitter" % "scalding-core_2.10" % "0.10.0",
  "org.apache.hadoop" % "hadoop-core" % "1.1.2",
  "org.slf4j" % "slf4j-log4j12" % "1.6.6"
)
