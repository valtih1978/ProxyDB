scalaVersion := "2.11.7"

libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value

includeFilter in (Compile, unmanagedSources) := "*.sc"

