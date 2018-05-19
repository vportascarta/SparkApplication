package ca.lif.sparklauncher.app

import java.text.SimpleDateFormat
import java.util.Date
import java.util.logging._

object CustomLogger {
  val logger: Logger = Logger.getLogger("Test Log")
  logger.setUseParentHandlers(false)

  def setLogger(): Unit = {
    val fh = new FileHandler(s"spark_${System.currentTimeMillis()}.log")
    fh.setFormatter(new CustomFormatter())

    val ch = new StreamHandler(System.out, new CustomFormatter()) {
      override def publish(record: LogRecord): Unit = {
        super.publish(record)
        flush()
      }
    }

    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.setLevel(Level.FINEST)
  }
}

class CustomFormatter extends Formatter {
  // Create a DateFormat to format the logger timestamp
  private val df = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss.SSS a")
  private val linesep = System.getProperty("line.separator")

  override def format(record: LogRecord): String = {
    val log: StringBuilder = new StringBuilder(500)

    log.append("[").append(record.getLevel).append("] - ")
    log.append(formatMessage(record))
    log.append(" [").append(df.format(new Date(record.getMillis))).append("]")
    log.append(linesep)

    log.toString()
  }
}

//%4$s: %5$s [%1$tb %1$td, %1$tY %1$tl:%1$tM:%1$tS.%1$tL %1$Tp]%n