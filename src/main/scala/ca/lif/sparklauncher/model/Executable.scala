package ca.lif.sparklauncher.model

trait Executable {
  def verify(): Boolean

  def execute(): Unit
}
