/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark

import java.io._
import java.lang.management.ManagementFactory
import java.util.concurrent.TimeUnit

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap
import scala.concurrent.duration._

import scala.io.Source

/**
 * Extracts information from the process info pseudo-filesystem at /proc.
 *
 * Based on https://code.google.com/p/djmonitor/source/browse/src/parser/ProcParser.java
 */
class ProcParser(conf: SparkConf) extends Logging {
  var previousGcLogTime = 0L
  var previousGcTime = -1L

  val CPU_TOTALS_FILENAME = "/proc/stat"
  val DISK_TOTALS_FILENAME = "/proc/diskstats"

  // 0-based index in /proc/pid/stat file of the user CPU time. Not necessarily portable.
  val UTIME_INDEX = 13
  val STIME_INDEX = 14
  // This is the correct value for most linux systems, and for the default Spark AMI.
  val JIFFIES_PER_SECOND = 100

  var previousCpuLogTime = 0L
  var previousUtime = -1
  var previousStime = -1
  var previousTotalCpuTime = -1
  var previousTotalUserTime = -1
  var previousTotalNiceTime = -1
  var previousTotalSysTime = -1
  var previousTotalIdleTime = -1
  var previousTotalIowaitTime = -1
  var previousTotalIrqTime = -1
  var previousTotalSoftIrqTime = -1
  var previousTotalGuestTime = -1

  // 0-based index within the list of numbers in /proc/pid/net/dev file of the received and
  // transmitted bytes/packets. Not necessarily portable.
  val RECEIVED_BYTES_INDEX = 0
  val RECEIVED_PACKETS_INDEX = 1
  val TRANSMITTED_BYTES_INDEX = 8
  val TRANSMITTED_PACKETS_INDEX = 9

  // We need to store the bytes/packets recorded at the last time so that we can compute the delta.
  var previousNetworkLogTime = 0L
  var previousReceivedBytes = 0L
  var previousReceivedPackets = 0L
  var previousTransmittedBytes = 0L
  var previousTransmittedPackets = 0L

  // Chars read is the sm of bytes pased to read() and pread() -- so it includes tty IO,
  // for example, and overestimtes the number of bytes written to physical disk. The bytes
  // are an attempt to count the number of bytes fetched from the storage layer.
  // (See http://stackoverflow.com/questions/3633286/understanding-the-counters-in-proc-pid-io)
  val CHARS_READ_PREFIX = "rchar: "
  val CHARS_WRITTEN_PREFIX = "wchar: "
  val BYTES_READ_PREFIX = "read_bytes: "
  val BYTES_WRITTEN_PREFIX = "write_bytes: "

  var previousDiskLogTime = 0L
  var previousCharsRead = 0L
  var previousCharsWritten = 0L
  var previousBytesRead = 0L
  var previousBytesWritten = 0L

  // The disk stats read from /proc/stat don't really reflect what's happening on the disk;
  // just when the reads/writes were issued.  So, also parse the diskstats file to understand
  // disk performance.
  val SECTORS_READ_INDEX = 5
  val MILLIS_READING_INDEX = 6
  val SECTORS_WRITTEN_INDEX = 9
  val MILLIS_WRITING_INDEX = 10
  val MILLIS_TOTAL_INDEX = 12
  // Dictionaries with one entry for each block device (indexed by device name). 
  var previousSectorsRead = HashMap[String, Long]()
  var previousSectorsWritten = HashMap[String, Long]()
  Source.fromFile(DISK_TOTALS_FILENAME).getLines().foreach { line =>
    if (line.indexOf("loop") == -1) {
      val deviceName = line.split(" ").filter(!_.isEmpty())(2)
      previousSectorsRead += deviceName -> 0L
      previousSectorsWritten += deviceName -> 0L
    }
  }

  val logIntervalMillis = conf.getInt("spark.procParser.logIntervalMillis", 50)
  val LOG_INTERVAL_MILLIS = Duration(logIntervalMillis, TimeUnit.MILLISECONDS)

  // Beware that the name returned by getName() is not guaranteed to keep following the pid@X
  // format.
  val PID = ManagementFactory.getRuntimeMXBean().getName().split("@")(0)

  def start(env: SparkEnv) {  
    logInfo("Starting ProcParser CPU logging")
    import env.actorSystem.dispatcher
    env.actorSystem.scheduler.schedule(
      Duration(0, TimeUnit.MILLISECONDS),
      LOG_INTERVAL_MILLIS) {
      logGcTime()
      logCpuUsage()
      logNetworkUsage()
      logDiskUsage()
    }
  }

  def logGcTime() {
    val currentTime = System.currentTimeMillis
    val currentGcTime = ManagementFactory.getGarbageCollectorMXBeans.map(_.getCollectionTime).sum
    val percentTimeGcing = ((currentGcTime - previousGcTime) * 1.0 /
      (currentTime - previousGcLogTime))
    if (previousGcTime != -1L) {
      logInfo("%s GC total %s Fraction of last interval GCing %s"
        .format(currentTime, currentGcTime, percentTimeGcing))
    }
    previousGcLogTime = currentTime
    previousGcTime = currentGcTime
  }
  
  /**
   * Logs the CPU utilization during the period of time since the last log message.
   */
  def logCpuUsage() {
    val file = new File("/proc/%s/stat".format(PID))
    val fileReader = new FileReader(file)
    val bufferedReader = new BufferedReader(fileReader)
    val line = bufferedReader.readLine()
    val values = line.split(" ")
    val currentUtime = values(UTIME_INDEX).toInt
    val currentStime = values(STIME_INDEX).toInt

    // Read the total number of jiffies that have elapsed since the last time we read the CPU info.
    var currentTotalCpuTime = -1
    var currentTotalUserTime = -1
    var currentTotalNiceTime = -1
    var currentTotalSysTime = -1
    var currentTotalIdleTime = -1
    var currentTotalIowaitTime = -1
    var currentTotalIrqTime = -1
    var currentTotalSoftIrqTime = -1
    var currentTotalGuestTime = -1
    val currentTime = System.currentTimeMillis
    Source.fromFile(CPU_TOTALS_FILENAME).getLines().foreach { line =>
      // Need the space after "cpu" to make sure we parse the total line and not the per-cpu line!
      if (line.startsWith("cpu ")) {
        val cpuTimes = line.substring(5, line.length).split(" ").map(_.toInt)
        currentTotalUserTime = cpuTimes(0)
        currentTotalNiceTime = cpuTimes(1)
        currentTotalSysTime = cpuTimes(2)
        currentTotalIdleTime = cpuTimes(3)
        currentTotalIowaitTime = cpuTimes(4)
        currentTotalIrqTime = cpuTimes(5)
        currentTotalSoftIrqTime = cpuTimes(6)
        currentTotalGuestTime = cpuTimes(7)
        currentTotalCpuTime = cpuTimes.sum
      }
    }
    if (currentTotalCpuTime == -1) {
      logError("Couldn't find line beginning with 'cpu' in file %s".format(CPU_TOTALS_FILENAME))
      return
    }

    val elapsedCpuTime = currentTotalCpuTime - previousTotalCpuTime
    if (previousUtime != -1 && elapsedCpuTime > 0) {
      val userUtil = (currentUtime - previousUtime) * 1.0 / elapsedCpuTime
      val sysUtil = (currentStime - previousStime) * 1.0 / elapsedCpuTime
      val totalProcessUtil = userUtil + sysUtil
      val totalUtil = ((currentTotalUserTime + currentTotalNiceTime + currentTotalSysTime +
        currentTotalIrqTime + currentTotalSoftIrqTime + currentTotalGuestTime) -
        (previousTotalUserTime + previousTotalNiceTime + previousTotalSysTime
        + previousTotalIrqTime + previousTotalSoftIrqTime + previousTotalGuestTime)) * 1.0 /
        elapsedCpuTime
      if (currentTotalIowaitTime < previousTotalIowaitTime) {
        logError("Current: %s, previous: %s".format(currentTotalIowaitTime, previousTotalIowaitTime))
      }
      val ioWait = (currentTotalIowaitTime - previousTotalIowaitTime) * 1.0 / elapsedCpuTime
      val idle = (currentTotalIdleTime - previousTotalIdleTime) * 1.0 / elapsedCpuTime
      logInfo(
        ("%s CPU utilization (relative metric): user: %s sys: %s proctotal: %s total: %s " +
          "iowait: %s idle: %s")
        .format(currentTime, userUtil, sysUtil, totalProcessUtil, totalUtil, ioWait, idle))
      logInfo("Conversion: %s".format(elapsedCpuTime * 1.0 / (currentTime - previousCpuLogTime)))
    }

    // Log alternate CPU utilization metric: the CPU counters are measured in jiffies,
    // so log the elapsed jiffies / jiffies per sec / seconds since last measurement.
    val elapsedTimeMillis = currentTime - previousCpuLogTime
    if (previousCpuLogTime > 0 && elapsedTimeMillis > 0) {
      val elapsedJiffies = JIFFIES_PER_SECOND * (elapsedTimeMillis * 1.0 / 1000)
      val userUtil = (currentUtime - previousUtime) / elapsedJiffies
      val sysUtil = (currentUtime - previousUtime) / elapsedJiffies
      logInfo("%s CPU utilization (jiffie-based): user: %s sys: %s total: %s"
        .format(currentTime, userUtil, sysUtil, userUtil + sysUtil))
    }

    // Log absolute counters to make it easier to compute utilization over the entire experiment.
    logInfo("%s CPU counters: user: %s sys: %s".format(currentTime, currentUtime, currentStime)) 

    previousUtime = currentUtime
    previousStime = currentStime
    previousTotalCpuTime = currentTotalCpuTime
    previousTotalUserTime = currentTotalUserTime
    previousTotalNiceTime = currentTotalNiceTime
    previousTotalSysTime = currentTotalSysTime
    previousTotalIdleTime = currentTotalIdleTime
    previousTotalIowaitTime = currentTotalIowaitTime
    previousTotalIrqTime = currentTotalIrqTime
    previousTotalSoftIrqTime = currentTotalSoftIrqTime
    previousTotalGuestTime = currentTotalGuestTime
    previousCpuLogTime = currentTime
  }

  def logNetworkUsage() {
    val currentTime = System.currentTimeMillis
    var totalTransmittedBytes = 0L
    var totalTransmittedPackets = 0L
    var totalReceivedBytes = 0L
    var totalReceivedPackets = 0L
    Source.fromFile("/proc/%s/net/dev".format(PID)).getLines().foreach { line =>
      if (line.contains(":") && !line.contains("lo")) {
        val counts = line.split(":")(1).split(" ").filter(_.length > 0).map(_.toLong)
        totalTransmittedBytes += counts(TRANSMITTED_BYTES_INDEX)
        totalTransmittedPackets += counts(TRANSMITTED_PACKETS_INDEX)
        totalReceivedBytes += counts(RECEIVED_BYTES_INDEX)
        totalReceivedPackets += counts(RECEIVED_PACKETS_INDEX)
      }
    }
    logInfo("%s Current totals: trans: %s bytes, %s packets, recv %s bytes %s packets".format(
      currentTime,
      totalTransmittedBytes,
      totalTransmittedPackets,
      totalReceivedBytes,
      totalReceivedPackets))
    if (previousNetworkLogTime > 0) {
      val timeDeltaSeconds = (currentTime - previousNetworkLogTime) / 1000.0
      val transmittedBytesRate = ((totalTransmittedBytes - previousTransmittedBytes) * 1.0 /
        timeDeltaSeconds)
      val transmittedPacketsRate = ((totalTransmittedPackets - previousTransmittedPackets) * 1.0 /
        timeDeltaSeconds)
      val receivedBytesRate = ((totalReceivedBytes - previousReceivedBytes) * 1.0 / timeDeltaSeconds)
      val receivedPacketsRate = ((totalTransmittedPackets - previousTransmittedPackets) * 1.0 /
        timeDeltaSeconds)
      logInfo("%s trans rates: %s bytes/s, %s packets/s; Recv rates: %s bytes/s, %s packets/s"
        .format(
          currentTime,
          transmittedBytesRate,
          transmittedPacketsRate,
          receivedBytesRate,
          receivedPacketsRate))
    }
    previousReceivedBytes = totalReceivedBytes
    previousReceivedPackets = totalReceivedPackets
    previousTransmittedBytes = totalTransmittedBytes
    previousTransmittedPackets = totalTransmittedPackets
    previousNetworkLogTime = currentTime
  }

  def logDiskUsage() {
    val currentTime = System.currentTimeMillis

    var totalCharsRead = 0L
    var totalCharsWritten = 0L
    var totalBytesRead = 0L
    var totalBytesWritten = 0L
    Source.fromFile("/proc/%s/io".format(PID)).getLines().foreach { line =>
      if (line.startsWith(CHARS_READ_PREFIX)) {
        totalCharsRead = line.substring(CHARS_READ_PREFIX.length).toLong
      } else if (line.startsWith(CHARS_WRITTEN_PREFIX)) {
        totalCharsWritten = line.substring(CHARS_WRITTEN_PREFIX.length).toLong
      } else if (line.startsWith(BYTES_READ_PREFIX)) {
        totalBytesRead = line.substring(BYTES_READ_PREFIX.length).toLong
      } else if (line.startsWith(BYTES_WRITTEN_PREFIX)) {
        totalBytesWritten = line.substring(BYTES_WRITTEN_PREFIX.length).toLong
      }
    }
    logInfo("%s IO Totals: rchar %s wchar %s rbytes %s wbytes %s".format(
      currentTime, totalCharsRead, totalCharsWritten, totalBytesRead, totalBytesWritten))

    val timeDeltaSeconds = (currentTime - previousDiskLogTime) / 1000.0
    if (previousDiskLogTime > 0 && timeDeltaSeconds > 0) {
      val charsReadRate = (totalCharsRead - previousCharsRead) * 1.0 / timeDeltaSeconds
      val charsWrittenRate = (totalCharsWritten - previousCharsWritten) * 1.0 / timeDeltaSeconds
      val bytesReadRate = (totalBytesRead - previousBytesRead) * 1.0 / timeDeltaSeconds
      val bytesWrittenRate = (totalBytesWritten - previousBytesWritten) * 1.0 / timeDeltaSeconds
      logInfo("%s rchar rate: %s wchar rate: %s rbytes rate: %s wbytes rate: %s".format(
        currentTime, charsReadRate, charsWrittenRate, bytesReadRate, bytesWrittenRate))
    }
  
    Source.fromFile(DISK_TOTALS_FILENAME).getLines().foreach { line =>
      if (line.indexOf("loop") == -1) {
        val items = line.split(" ").filter(!_.isEmpty())
        val deviceName = items(2)
        val totalSectorsRead = items(SECTORS_READ_INDEX).toLong
        val sectorsReadRate = ((totalSectorsRead - previousSectorsRead(deviceName)) * 1.0 /
          timeDeltaSeconds)
        val totalSectorsWritten = items(SECTORS_WRITTEN_INDEX).toLong
        val sectorsWriteRate = ((totalSectorsWritten - previousSectorsWritten(deviceName)) * 1.0 /
          timeDeltaSeconds)
        logInfo("%s %s sectors rate read %s written %s total read %s written %s"
          .format(currentTime, deviceName, sectorsReadRate, sectorsWriteRate, totalSectorsRead,
            totalSectorsWritten))

        previousSectorsRead += deviceName -> totalSectorsRead
        previousSectorsWritten += deviceName -> totalSectorsWritten
      }
    }

    previousDiskLogTime = currentTime
    previousCharsRead = totalCharsRead
    previousCharsWritten = totalCharsWritten
    previousBytesRead = totalBytesRead
    previousBytesWritten = totalBytesWritten
  }

  def logMemoryUsage() {
  }
}
