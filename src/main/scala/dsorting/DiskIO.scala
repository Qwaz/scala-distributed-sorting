package dsorting

import java.io.{File, FileInputStream, FileOutputStream, FilenameFilter}

import dsorting.primitive._

class Directory(path: String) {
  private val folder = new File(path)
  require(folder.isDirectory)

  def listFilesWithPrefix(prefix: String) = {
    folder.listFiles(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = name.startsWith(prefix)
    })
  }

  def allocateTemporaryFIle() = {
    File.createTempFile(Setting.TempFilePrefix, "", folder)
  }

  def clearTemporaryFiles() = {
    val tempFiles = listFilesWithPrefix(Setting.TempFilePrefix)
    tempFiles.foreach(f => f.delete())
  }
}

object Directory {
  def apply(path: String) = {
    new Directory(path)
  }
}

trait EntryReader extends Iterator[Entry] {
  def numEntries: Long
  def remainingEntries: Integer
}

class FileEntryReader(val file: File) extends EntryReader with AutoCloseable {
  require(file.isFile)
  require(file.length % Setting.EntrySize == 0)
  private val fileStream = new FileInputStream(file)

  def numEntries = file.length / Setting.EntrySize
  def remainingEntries = fileStream.available / Setting.EntrySize

  override def next() = {
    val keyBytes = new Array[Byte](Setting.KeySize)
    fileStream.read(keyBytes, 0, Setting.KeySize)

    val valueBytes = new Array[Byte](Setting.ValueSize)
    fileStream.read(valueBytes, 0, Setting.ValueSize)

    Entry(Key(keyBytes), Value(valueBytes))
  }

  override def hasNext = remainingEntries > 0

  def close() = fileStream.close()
}

trait EntryWriter {
  def writeEntry(entry: Entry)
  def close()
}

class FileEntryWriter(val file: File) extends EntryWriter with AutoCloseable {
  require(file.isFile)
  private val fileStream = new FileOutputStream(file)

  override def writeEntry(entry: Entry) = {
    fileStream.write(entry.key.bytes)
    fileStream.write(entry.value.bytes)
    fileStream.flush()
  }

  def close() = fileStream.close()
}