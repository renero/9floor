import sbt._
import sbtassembly.Plugin._
import sbtassembly.Plugin.AssemblyKeys._

object Build extends sbt.Build {

  def projectId(name: String) = s"konkordator-$name"

  val KonkordatorMergeStrategy = mergeStrategy in assembly := {
    case "reference.conf" => MergeStrategy.concat
    case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
    case m if m.toLowerCase.matches("meta-inf/services/.*") => MergeStrategy.concat
    case m if m.toLowerCase.matches("meta-inf/.*\\.sf$") => MergeStrategy.discard
    case _ => MergeStrategy.first
  }

  lazy val KonkordatorAssembly = assemblySettings ++ KonkordatorMergeStrategy

  lazy val root = (
    Project(
      id = projectId("root"), base = file("."),
      settings = Defaults.defaultSettings ++ sbtassembly.Plugin.assemblySettings ++ addArtifact(
        Artifact(projectId("root"), "assembly"), sbtassembly.Plugin.AssemblyKeys.assembly)
    )
    settings(Defaults.itSettings: _*)
    settings(KonkordatorAssembly: _*)
    aggregate(indices)
    dependsOn(indices % "compile->compile;test->test")
  )

  lazy val indices = (Project(id = projectId("indices"), base = file("indices"))
    settings(Defaults.itSettings: _*)
  )
}
