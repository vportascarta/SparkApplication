package ca.lif.sparklauncher.gui


import ca.lif.sparklauncher.model.Executable

import scala.swing._
import scala.swing.event._

object Gui extends SimpleSwingApplication {
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
    preferredSize = new Dimension(400, 650)

    contents = new BoxPanel(Orientation.Vertical) {
      contents += tabs_view
      contents += new BoxPanel(Orientation.Horizontal) {
        contents += console_button
        contents += launch_button
      }
      border = Swing.EmptyBorder(10, 10, 10, 10)
    }


    listenTo(launch_button, console_button)
    reactions += {
      case ButtonClicked(`launch_button`) =>
        if (!ConsoleView.visible) {
          ConsoleView.visible = true
          console_button.text = "Hide Console"
        }

        val params: Executable = {
          if (tabs_view.selection.page == coloring_page) {
            ColoringParametersView.getLaunchParameters
          }
          else {
            HypergraphParametersView.getLaunchParameters
          }
        }

        params.execute()

        this.visible = false

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
