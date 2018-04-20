package ca.lif.sparklauncher.gui

import javax.swing.{JSpinner, SpinnerModel}

class CustomSpinner(model: SpinnerModel) extends JSpinner(model: SpinnerModel) {
  setEditor(new JSpinner.DefaultEditor(this))
}
