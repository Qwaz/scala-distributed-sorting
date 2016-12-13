package dsorting.transition.slave

import java.io.File

import com.typesafe.scalalogging.Logger
import dsorting.Setting
import dsorting.diskio._
import dsorting.messaging._
import dsorting.primitive._
import dsorting.states.slave._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}

object SortingInitializer {
  def prepareSorting(prevState: ShufflingState)(targetFile: File): SortingState = {
    new TransitionFromConnected(prevState) with SortingState {
      val logger = Logger("Slave Sorting")

      private val SortingPrefix = "sorting"
      private val outputDirectory = Directory(ioDirectoryInfo.outputDirectory)
      outputDirectory.deleteFilesWithPrefix(SortingPrefix)

      private val mySlaveIndex = {
        partitionTable.identity match {
          case Master => -1
          case Slave(i) => i
        }
      }

      def run(): Future[Unit] = {
        logger.info("start running")

        val p = Promise[Unit]()

        def receiveSortingComplete(data: Array[Byte]): Unit = {
          p.success(())
        }

        listener.replaceHandler{
          (message, futurama) => {
            message.messageType match {
              case MessageType.SortingComplete => futurama.executeImmediately(receiveSortingComplete, message.data)
              case _ => Future()
            }
          }
        }

        performSorting()

        p.future
      }

      private def performSorting(): Unit = {
        val sortedFile = sortFile(0, targetFile)
        val resultFile = outputDirectory.createResultFile(mySlaveIndex)
        FileUtil.mergeFiles(Seq(sortedFile), resultFile)

        channelToMaster.sendMessage(Message.withType(MessageType.SortingDone))
        logger.info("sorting done, waiting for other slaves")
      }

      private def distributeEntriesToFiles(inputFile: File, entryWriters: IndexedSeq[EntryWriter], indexCalculator: Entry => Int): Unit = {
        val entryReader = new FileEntryReader(inputFile)
        entryReader.foreach {
          entry =>
            val index = indexCalculator(entry)
            entryWriters(index).writeEntry(entry)
        }
        entryReader.close()
      }

      private def sortFile(sortedPrefixNibble: Int, inputFile: File): File = {
        if (sortedPrefixNibble >= Setting.KeySize * 2) {
          inputFile
        } else {
          if (inputFile.length <= Setting.InMemorySortingBound) {
            inMemorySort(inputFile)
          } else {
            val files = (0 until 16).map(_ => outputDirectory.createTemporaryFileWithPrefix(SortingPrefix))
            val entryWriters = files.map(file => new FileEntryWriter(file))
            distributeEntriesToFiles(inputFile, entryWriters, (entry: Entry) => {
              val targetByte = entry.key.bytes(sortedPrefixNibble / 2)
              (if ((sortedPrefixNibble & 1) > 0) targetByte else targetByte >> 4) & 0x0F
            })
            entryWriters.foreach(_.close())

            val sortedFiles = files.map(file => sortFile(sortedPrefixNibble+1, file))

            val outputFile = outputDirectory.createTemporaryFileWithPrefix(SortingPrefix)
            FileUtil.mergeFiles(sortedFiles, outputFile)

            if (Setting.DeleteTemporarySortingFiles) {
              files.foreach(file => file.delete())
              sortedFiles.foreach(file => file.delete())
            }

            outputFile
          }
        }
      }

      private def inMemorySort(inputFile: File): File = {
        val outputFile = outputDirectory.createTemporaryFileWithPrefix(SortingPrefix)
        val entryWriter = new FileEntryWriter(outputFile)

        val entryReader = new FileEntryReader(inputFile)
        val sortedEntries = entryReader.toSeq.sortWith(_ <= _)
        sortedEntries.foreach(entryWriter.writeEntry)

        entryReader.close()
        entryWriter.close()

        outputFile
      }

      logger.info("initialized")
    }
  }
}
