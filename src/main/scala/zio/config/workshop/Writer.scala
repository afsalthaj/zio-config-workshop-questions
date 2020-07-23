package zio.config.workshop

import java.time.LocalDate

import zio.config._
import com.typesafe.config.ConfigRenderOptions
import zio.{IO, Ref, ZIO}
import zio.config.magnolia.DeriveConfigDescriptor._
import zio.config.typesafe.TypesafeConfigSource
import zio.config.workshop.Writer.SourceDetails.{Destination, Partition}
import zio.config.typesafe._

import scala.util.Try

object Writer extends EitherSupport {
  final case class Sample(value: String, map: Map[String, String], list: List[String])

  /**
   * Write Sample a Json
   * Ex: Sample("a", Map("b" -> "c", "d" -> "e"), List("f", "g", "h"))
   */
  def writeSampleToJson =
    println(write(descriptor[Sample], Sample("a", Map("b" -> "c", "d" -> "e"), List("f", "g", "h"))).map(_.toJson))

  /**
   * Write Sample a HOCON
   * Ex: Sample("a", Map("b" -> "c", "d" -> "e"), List("f", "g", "h"))
   */
  def writeSampleToHocon =
    println(write(descriptor[Sample], Sample("a", Map("b" -> "c", "d" -> "e"), List("f", "g", "h"))).map(_.toHoconString))

  /**
   * Write Sample a HOCON concise
   * Ex: Sample("a", Map("b" -> "c", "d" -> "e"), List("f", "g", "h"))
   */
  def writeSampleToTypesafeHoconConcise =
    println(write(descriptor[Sample], Sample("a", Map("b" -> "c", "d" -> "e"), List("f", "g", "h"))).map(_.toHocon.render(ConfigRenderOptions.concise())))

  /**
   * Write Sample a a flattened map
   * Ex: Sample("a", Map("b" -> "c", "d" -> "e"), List("f", "g", "h"))
   */
  def wrtiteSampleToAFlattenedMap =
    println(write(descriptor[Sample], Sample("a", Map("b" -> "c", "d" -> "e"), List("f", "g", "h"))).map(_.flattenString()))


  /**
   * A sample application:
   *
   * Now that, we are familiar SourceDetails. We have seen different versions of it while we learned zio-config.
   *
   * Consider SourceDetails as your configuration for an export application that exports
   * data from a source to a destination. We can use zio-config to read the HOCON config to SourceDetails.
   * Given the source details, we pick the data and transfer it to destination.
   *
   * Every day new data comes into a partition attributed by the date-time in the source. A source can be AWS s3, or a database. It doesn't matter here much.
   * The export job is a daily job that sends the data to a destination, and it must make sure that there are no duplicate data transfers.
   * We should also get a view of the failed, running and completed partitions anytime we ask for!
   *
   * Instead of building an entire app, see if you can just use zio-config for state management here.
   *
   * Use name annotation
   */
  final case class SourceDetails(
    sourceTableName: String,
    partition: Either[Writer.SourceDetails.Partition, Int],
    destination: Destination,
    columns: List[String],
    sourceCredentials: SourceDetails.Credentials,
    renameFileTo: Option[String],
    options: Map[String, String]
  )

  object SourceDetails {
    final case class Partition(value: LocalDate)
    final case class Credentials(username: String, password: String)

    sealed trait Env
    final case object Prod extends Env
    final case object Dev extends Env

    sealed trait Destination {
      def environment: Env
    }

    object Destination {
      final case class S3(basePath: String, environment: Env) extends Destination
      final case class Database(name: String, environment: Env) extends Destination
    }
  }


  // Building block 1: What's our status
  sealed trait Status {
    def runTime: LocalDate
  }

  object Status {
    final case class Success(runTime: LocalDate) extends Status
    final case class Failure(runTime: LocalDate, failureReason: String) extends Status
    final case class Running(runTime: LocalDate) extends Status
  }

  // Building block 2: What's our state
  final case class State(state: Map[String, Map[Partition, Status]])

  object State {
    implicit def descriptorForMap[A : Descriptor]: Descriptor[Map[Partition, A]] =
      Descriptor[Map[String, A]].xmapEither(map => convert(map), v => Right(mapKeys(v)(_.value.toString)))

    def convert[A, E](map: Map[String, A]): Either[String, Map[Partition, A]] =
      map.toList.map({
        case(k, v) =>  Try {
          LocalDate.parse(k)
        }.map(l => (Partition(l), v)).toEither
      }).sequence.map(_.toMap).leftMap(_.getMessage)

    def mapKeys[A, B, C](map: Map[A, B])(f: A => C): Map[C, B] =
      map.toList.map { case (a, b) => (f(a), b)
      }.toMap
  }

  // Building block 4; Read and write from File
  trait StateMachine[E] {
    def readState: IO[E, State]
    def writeState(state: State): IO[E, Unit]
  }

  object StateMachine {
    import State._

    def fromRef[E](ref: Ref[String]): StateMachine[String] = new StateMachine[String] {
      override def readState: IO[String, State] =
        ref.get.flatMap(v => ZIO.fromEither({
          TypesafeConfigSource.fromHoconString(v)
            .flatMap(s => read(descriptor[State] from s).leftMap(_.prettyPrint()))

        }))

      override def writeState(state: State): IO[String, Unit] =
        ZIO.fromEither(write(descriptor[State], state).map(_.toHoconString))
            .flatMap(str => ref.update(_ => str))
    }
  }

  /**
   *  Try out state read and write
   */

  def writeStateExample = {
    import State._
    val state =
      State(Map("xyz" -> Map(Partition(LocalDate.of(2019, 9, 10)) -> Status.Running(runTime = LocalDate.now()))))

    println(write(descriptor[State], state).map(_.toJson))
  }

  def readStateExample ={
    import State._
    val hocon =
   s"""
       |
       |    {
       |      "state" : {
       |          "xyz" : {
       |           "2019-09-10" : {
       |              "running" :  {
       |                  "runTime" : "2020-07-23"
       |               }
       |            },
       |            "2019-09-20" : {
       |              "running" :  {
       |                  "runTime" : "2020-07-23"
       |               }
       |            }
       |         }
       |      }
       |    }
       |
       |""".stripMargin

    println(TypesafeConfigSource.fromHoconString(hocon).flatMap(v => read(descriptor[State] from v).leftMap(_.prettyPrint())))
  }
}

