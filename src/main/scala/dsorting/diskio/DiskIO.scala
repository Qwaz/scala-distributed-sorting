package dsorting.diskio

import java.io.{File, FileInputStream, FileOutputStream, FilenameFilter}
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption

import dsorting.Setting
import dsorting.primitive._

object FileUtil {
  def mergeFiles(inputFiles: Seq[File], outputFile: File): Unit = {
    val outChannel = FileChannel.open(outputFile.toPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)
    for (inputFile <- inputFiles) {
      val inChannel = FileChannel.open(inputFile.toPath, StandardOpenOption.READ)

      val inFileSize = inChannel.size()
      var position = 0L
      while (position < inFileSize) {
        position += inChannel.transferTo(position, inFileSize-position, outChannel)
      }
      inChannel.close()
    }
    outChannel.close()
  }
}

class Directory(path: String) {
  private val folder = new File(path)
  require(folder.isDirectory)

  def listFilesWithPrefix(prefix: String) = {
    folder.listFiles(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = name.startsWith(prefix)
    })
  }

  def createTemporaryFileWithPrefix(prefix: String) = {
    File.createTempFile(prefix, "", folder)
  }

  def createResultFile(slaveIndex: Int) = {
    new File(folder, s"result.$slaveIndex")
  }

  def deleteFilesWithPrefix(prefix: String) = {
    val files = listFilesWithPrefix(prefix)
    files.foreach(f => f.delete())
  }
}

object Directory {
  def apply(path: String) = {
    new Directory(path)
  }
}

trait EntryReader extends Iterator[Entry] {
  def numEntries: Long
  def remainingEntries: Int
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

trait EntryWriter extends AutoCloseable {
  def writeEntry(entry: Entry)
  def close()
}

class FileEntryWriter(val file: File) extends EntryWriter {
  require(file.isFile)

  if (!file.exists) file.createNewFile()

  private val fileStream = new FileOutputStream(file)

  override def writeEntry(entry: Entry) = {
    fileStream.write(entry.key.bytes)
    fileStream.write(entry.value.bytes)
    fileStream.flush()
  }

  def close() = fileStream.close()
}