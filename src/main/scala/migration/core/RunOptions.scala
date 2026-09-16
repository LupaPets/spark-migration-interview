package migration.core

final case class RunOptions(
    source: String,
    outputRoot: String = "",
    clinicId: String = "clinic_a",
    previewRows: Int = 10,
    validate: Boolean = false
) {
  def pipelineConfig: PipelineConfig = PipelineConfig(source, clinicId, outputRoot)
}

object RunOptions {
  private val usage =
    "source [output-root] [--clinic id] [--preview 0..100] [--validate]"

  def parse(args: Seq[String]): Either[String, RunOptions] = {
    val source = args.headOption.getOrElse("vet_system_one")
    if (!PipelineRegistry.supported.contains(source))
      return Left(s"Unknown source: $source. Usage: $usage")

    def loop(rest: List[String], options: RunOptions): Either[String, RunOptions] =
      rest match {
        case Nil => Right(options)
        case "--validate" :: tail => loop(tail, options.copy(validate = true))
        case "--clinic" :: id :: tail if id.nonEmpty && !id.startsWith("--") =>
          loop(tail, options.copy(clinicId = id))
        case "--preview" :: value :: tail =>
          value.toIntOption match {
            case Some(rows) if rows >= 0 && rows <= 100 =>
              loop(tail, options.copy(previewRows = rows))
            case _ => Left("Preview rows must be an integer between 0 and 100")
          }
        case value :: tail if !value.startsWith("--") && options.outputRoot.isEmpty =>
          loop(tail, options.copy(outputRoot = value))
        case unexpected :: _ => Left(s"Unexpected or incomplete argument: $unexpected. Usage: $usage")
      }

    loop(args.drop(1).toList, RunOptions(source))
  }
}
