package ca.lif.sparklauncher.main

import java.text.SimpleDateFormat
import java.util.Date
import java.util.logging._

object CustomLogger {
  val logger: Logger = Logger.getLogger("Test Log")
  logger.setUseParentHandlers(false)

  def setLogger(): Unit = {
    val fh = new FileHandler(s"test_${System.currentTimeMillis()}.log")
    val ch = new ConsoleHandler()
    fh.setFormatter(new CustomFormatter())
    ch.setFormatter(new CustomFormatter())
    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.setLevel(Level.FINEST)
  }
}

class CustomFormatter extends Formatter {
  // Create a DateFormat to format the logger timestamp
  private val df = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss.SSS a")

  override def format(record: LogRecord): String = {
    val log: StringBuilder = new StringBuilder(500)

    log.append("[").append(record.getLevel).append("] - ")
    log.append(formatMessage(record))
    log.append(" [").append(df.format(new Date(record.getMillis))).append("]")
    log.append("\r\n")

    log.toString()
  }
}

//%4$s: %5$s [%1$tb %1$td, %1$tY %1$tl:%1$tM:%1$tS.%1$tL %1$Tp]%n