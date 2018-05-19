package ca.lif.sparklauncher.gui

import java.io._
import java.util.Scanner

import scala.swing._

object ConsoleView extends Frame {

  private val console_output = new TextArea {
    editable = false
  }

  val console_stream = new PrintStream(new TextAreaOutputStream(console_output.peer))
  var copy_thread: Thread = _

  System.setOut(console_stream)
  System.setErr(console_stream)

  title = "Spark Console"
  preferredSize = new Dimension(640, 480)

  contents = new ScrollPane() {
    contents = console_output
    border = Swing.EmptyBorder(0, 5, 0, 5)
  }

  def copyInputStream(src: InputStream): Unit = {
    val t = new Thread(new Runnable {
      override def run(): Unit = {
        val sc = new Scanner(src)
        while (sc.hasNextLine) {
          console_stream.println(sc.nextLine)
        }
      }
    })
    t.start()
  }

  override def closeOperation(): Unit = {
    super.closeOperation()
    MainGui.console_button.text = "Show Console"
  }
}

