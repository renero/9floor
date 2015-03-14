
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.11.2")

addSbtPlugin("com.eed3si9n" % "sbt-unidoc" % "0.3.1")

addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "0.7.5")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.5.0")

addSbtPlugin("org.scoverage" %% "sbt-scoverage" % "0.98.2")

resolvers ++= Seq(
  Classpaths.sbtPluginReleases
)
