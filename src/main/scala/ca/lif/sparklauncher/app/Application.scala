package ca.lif.sparklauncher.app

import ca.lif.sparklauncher.console.MainConsole
import ca.lif.sparklauncher.gui.MainGui

object Application extends App {
  if (args.isEmpty) {
    MainGui.main(args)
  }
  else {
    MainConsole.main(args)
  }
}
