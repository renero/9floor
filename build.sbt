name := "konkordator"

organization in ThisBuild := "com.ninthfloor"

version in ThisBuild := "0.1.0"

scalaVersion in ThisBuild := "2.11.5"

libraryDependencies in ThisBuild ++= Seq(
  "com.github.nscala-time" %% "nscala-time" % "1.8.0",
  "org.apache.spark" %% "spark-core" % "1.3.0" % "provided",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test"
)

ivyXML in ThisBuild :=
  <dependency org="org.eclipse.jetty.orbit" name="javax.servlet" rev="2.5.0.v201103041518">
    <artifact name="javax.servlet" type="orbit" ext="jar"/>
  </dependency>

credentials in ThisBuild += Credentials(Path.userHome / ".ivy2" / ".credentials")
