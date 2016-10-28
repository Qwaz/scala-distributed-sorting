package dsorting.primitive

class IODirectoryInfo(val inputFiles: List[String], val outputDirectory: String) {
  require(inputFiles.nonEmpty)
}
