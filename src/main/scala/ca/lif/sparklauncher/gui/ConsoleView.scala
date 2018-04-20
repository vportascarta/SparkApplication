package ca.lif.sparklauncher.gui

import java.io.PrintStream

import scala.swing._

object ConsoleView extends MainFrame {

  private val console_output = new TextArea {
    editable = false
  }

  private val con = new PrintStream(new TextAreaOutputStream(console_output.peer))

  System.setOut(con)
  System.setErr(con)

  title = "Spark Console"
  preferredSize = new Dimension(640, 480)

  contents = new ScrollPane() {
    contents = console_output
    border = Swing.EmptyBorder(0, 5, 0, 5)
  }

}
