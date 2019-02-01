package com.mpojeda84.mapr.scala;

case class Configuration(source: String, target: String, community: String)

object Configuration {

  def parse(args: Seq[String]): Configuration = parser.parse(args, Configuration.default).get

  def default: Configuration = DefaultConfiguration



  object DefaultConfiguration extends Configuration(
    "path/to/table",
    "path/to/table",
    "path/to/table"
  )

  private val parser = new scopt.OptionParser[Configuration]("App Name") {
    head("App Name")

    opt[String]('s', "source")
      .action((s, config) => config.copy(source = s))
      .maxOccurs(1)
      .text("MapR-DB table origin")

    opt[String]('t', "target")
      .action((t, config) => config.copy(target = t))
      .text("MapR-DB table target")

    opt[String]('c', "community")
      .action((c, config) => config.copy(community = c))
      .text("MapR-DB table for commiunity data")

  }
}