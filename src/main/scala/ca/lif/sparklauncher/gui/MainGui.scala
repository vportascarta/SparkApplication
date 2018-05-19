package ca.lif.sparklauncher.gui


import java.lang.management.ManagementFactory

import com.sun.management.OperatingSystemMXBean
import javax.swing.SpinnerNumberModel

import scala.swing._
import scala.swing.event._

object MainGui extends SimpleSwingApplication {
  var launched = false

  val max_memory = ManagementFactory.getOperatingSystemMXBean.asInstanceOf[OperatingSystemMXBean].getTotalPhysicalMemorySize / 1000000
  val memory_spinner = new CustomSpinner(new SpinnerNumberModel(2000, 500, max_memory.toInt, 100))

  val launch_button = new Button("Launch")
  val console_button = new Button("Show Console")
  val coloring_page = new TabbedPane.Page("Coloring", ColoringParametersView)
  val hypergraph_page = new TabbedPane.Page("Hypergraph", HypergraphParametersView)
  val tabs_view = new TabbedPane {
    pages += coloring_page
    pages += hypergraph_page
  }


  def top = new MainFrame {
    title = "Spark Launcher"
    preferredSize = new Dimension(420, 670)

    contents = new BoxPanel(Orientation.Vertical) {
      contents += tabs_view

      contents += new BoxPanel(Orientation.Horizontal) {
        contents += new Label("Memory allocated in Mb :  ") {
          tooltip = "Maximum memory allocated for spark"
        }
        contents += Component.wrap(memory_spinner)
      }

      contents += new BoxPanel(Orientation.Horizontal) {
        contents += console_button
        contents += launch_button
      }
      border = Swing.EmptyBorder(10, 10, 10, 10)
    }


    listenTo(launch_button, console_button)
    reactions += {
      case ButtonClicked(`launch_button`) =>
        if (!launched) {
          launched = true
          if (!ConsoleView.visible) {
            ConsoleView.visible = true
            console_button.text = "Hide Console"
          }

          val params: Array[String] = {
            if (tabs_view.selection.page == coloring_page) {
              ColoringParametersView.getLaunchParameters.toCommandlineString
            }
            else {
              HypergraphParametersView.getLaunchParameters.toCommandlineString
            }
          }

          ProcessHelper.start(s"-Xmx${memory_spinner.getValue.asInstanceOf[Int]}M",
            "ca.lif.sparklauncher.console.MainConsole", params)

          launch_button.text = "Stop"
        }
        else {
          ProcessHelper.kill()
          launch_button.text = "Launch"
          launched = false
        }

      case ButtonClicked(`console_button`) =>
        if (!ConsoleView.visible) {
          ConsoleView.visible = true
          console_button.text = "Hide Console"
        }
        else {
          ConsoleView.visible = false
          console_button.text = "Show Console"
        }

    }
  }

  def resetTitle(): Unit = {
    top.title = "Spark Launcher"
    //top.validate()
    //top.repaint()
  }
}
