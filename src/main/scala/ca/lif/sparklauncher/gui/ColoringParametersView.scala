package ca.lif.sparklauncher.gui

import java.io.File

import ca.lif.sparklauncher.model.ColoringParameters
import javax.swing.filechooser.FileNameExtensionFilter
import javax.swing.{SpinnerListModel, SpinnerNumberModel}

import scala.swing._
import scala.swing.event.ButtonClicked

object ColoringParametersView extends GridPanel(10, 2) {
  val version_strings: Array[AnyRef] = Array("1", "2", "3")
  val version_spinner = new CustomSpinner(new SpinnerListModel(version_strings))
  val loops_spinner = new CustomSpinner(new SpinnerNumberModel(1, 1, 100, 1))
  val partitions_spinner = new CustomSpinner(new SpinnerNumberModel(0, 0, null, 1))
  val max_iterations_spinner = new CustomSpinner(new SpinnerNumberModel(400, 1, null, 1))
  val checkpoint_interval_spinner = new CustomSpinner(new SpinnerNumberModel(0, 0, null, 1))
  val choice_input_file = new RadioButton("File")
  val choice_input_generated = new RadioButton("Generated")
  val file_path_label = new Label("File : None") {
    tooltip = "Input data from gph file"
  }
  val file_button = new Button("Choose...")
  var file_path = ""
  val file_is_graphviz = new CheckBox("File format is Graphviz")
  val n_spinner = new CustomSpinner(new SpinnerNumberModel(3, 3, 100, 1))
  val t_spinner = new CustomSpinner(new SpinnerNumberModel(2, 2, 100, 1))
  val v_spinner = new CustomSpinner(new SpinnerNumberModel(2, 2, 100, 1))

  file_button.enabled = false
  file_is_graphviz.enabled = false
  n_spinner.setEnabled(false)
  t_spinner.setEnabled(false)
  v_spinner.setEnabled(false)

  contents += new Label("Algorithm version :") {
    tooltip = "Version of the algorithm to be use (1, 2, 3)"
  }
  contents += Component.wrap(version_spinner)
  contents += new Label("Loops run by spark : ") {
    tooltip = "Loops run by spark on the same data"
  }
  contents += Component.wrap(loops_spinner)
  contents += new Label("Number of partitions : ") {
    tooltip = "Number of partitions for each RDD (0 = auto)"
  }
  contents += Component.wrap(partitions_spinner)
  contents += new Label("Maximum number of iterations : ") {
    tooltip = "Maximum number of iteration before quit"
  }
  contents += Component.wrap(max_iterations_spinner)
  contents += new Label("Checkpoint interval : ") {
    tooltip = "Interval between two checkpoints (0 = off)"
  }
  contents += Component.wrap(checkpoint_interval_spinner)

  contents ++= new ButtonGroup(choice_input_file, choice_input_generated).buttons
  contents += file_path_label
  contents += new BoxPanel(Orientation.Vertical) {
    contents += file_button
    contents += file_is_graphviz
  }

  contents += new Label("Generated N") {
    tooltip = "Number of variables"
  }
  contents += Component.wrap(n_spinner)
  contents += new Label("Generated T") {
    tooltip = "Number of variables on each group"
  }
  contents += Component.wrap(t_spinner)
  contents += new Label("Generated V") {
    tooltip = "Number of value for one variable"
  }
  contents += Component.wrap(v_spinner)

  border = Swing.EmptyBorder(5)

  listenTo(file_button, choice_input_file, choice_input_generated)

  reactions += {
    case ButtonClicked(`file_button`) =>
      val res = choosePlainFile("Choose graph file")
      if (res.nonEmpty) {
        file_path_label.text = s"File : ${res.get.getName}"
        file_path = res.get.getAbsolutePath
        file_path_label.tooltip = res.get.getAbsolutePath
      }
      else {
        file_path_label.text = "File : None"
        file_path = ""
        file_path_label.tooltip = "Input data from hypergraph file"
      }
    case ButtonClicked(`choice_input_file`) =>
      file_button.enabled = true
      file_is_graphviz.enabled = true
      n_spinner.setEnabled(false)
      t_spinner.setEnabled(false)
      v_spinner.setEnabled(false)
    case ButtonClicked(`choice_input_generated`) =>
      file_button.enabled = false
      file_is_graphviz.enabled = false
      n_spinner.setEnabled(true)
      t_spinner.setEnabled(true)
      v_spinner.setEnabled(true)
  }

  def choosePlainFile(title: String = ""): Option[File] = {
    val filter = new FileNameExtensionFilter("Graph file", "gph")
    val chooser = new FileChooser(new File("."))
    chooser.title = title
    chooser.fileFilter = filter
    val result = chooser.showOpenDialog(null)
    if (result == FileChooser.Result.Approve) {
      Some(chooser.selectedFile)
    } else None
  }

  def getLaunchParameters: ColoringParameters = {
    val partitions =
      if (partitions_spinner.getValue.asInstanceOf[Int] == 0) Runtime.getRuntime.availableProcessors
      else partitions_spinner.getValue.asInstanceOf[Int]
    val checkpoint_interval =
      if (checkpoint_interval_spinner.getValue.asInstanceOf[Int] == 0) -1
      else checkpoint_interval_spinner.getValue.asInstanceOf[Int]

    new ColoringParameters(
      version_spinner.getValue.asInstanceOf[String].toInt,
      loops_spinner.getValue.asInstanceOf[Int],
      partitions,
      max_iterations_spinner.getValue.asInstanceOf[Int],
      checkpoint_interval,
      file_path,
      file_is_graphviz.selected,
      n_spinner.getValue.asInstanceOf[Int],
      t_spinner.getValue.asInstanceOf[Int],
      v_spinner.getValue.asInstanceOf[Int]
    )
  }
}
