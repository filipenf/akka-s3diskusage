name := "s3du-scala-akka"

version := "1.0"

scalaVersion := "2.10.4"


libraryDependencies ++= {
  val akkaVersion  = "2.3.2"
  val sprayVersion = "1.3.1"
  Seq(
    "com.typesafe.akka" %% "akka-actor" % akkaVersion
      exclude ("org.scala-lang" , "scala-library"),
    "com.typesafe.akka" %% "akka-slf4j" % akkaVersion
      exclude ("org.slf4j", "slf4j-api")
      exclude ("org.scala-lang" , "scala-library"),
    "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
    "com.amazonaws" % "aws-java-sdk-s3" % "1.9.30"
  )
}
