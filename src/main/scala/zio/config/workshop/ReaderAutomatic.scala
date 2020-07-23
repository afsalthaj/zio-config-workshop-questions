package zio.config.workshop

import java.time.{LocalDate, ZonedDateTime}

import zio.config.derivation.describe
import zio.config.magnolia.DeriveConfigDescriptor.{Descriptor, descriptor}
import zio.config.typesafe.TypesafeConfigSource
import zio.config.read
import zio.config.camelToKebab

import scala.util.Try

object ReaderAutomatic extends EitherSupport {

  /**
   * Assuming that we are familiar with manually describing the config, and got a reader out of it,
   * Lets do all of this thing using automatic description.
   */
  final case class SourceDetails6(
    tableName: String,
    partition: Int,
    columns: List[String],
    credentials: SourceDetails6.Credentials,
    renameFileTo: Option[String],
    options: Map[String, String]
  )

  object SourceDetails6 {
    final case class Credentials(username: String, password: String)
  }

  /**
   * using automatic descriptor to read from HOCON
   * Try out removing some config and see the errors as well
   */
  def readSourceDetail6FromHocon: Either[String, SourceDetails6] = {
    val hocon =
      s"""
         |{
         |  "tableName" : "x",
         |  "partition" : "1000",
         |  "columns" : [a, b, c, d],
         |  "credentials" : {
         |     "username" : "xyz",
         |     "password" : "123"
         |   }
         |  "renameFileTo" : "a",
         |  "options" : {
         |     "quoteAll" : "true",
         |     "delimiter" :   "|"
         |  }
         |}
         |
         |""".stripMargin

    for {
      source <- TypesafeConfigSource.fromHoconString(hocon)
      config  <- read(descriptor[SourceDetails6] from source).leftMap(_.prettyPrint())
    } yield config
  }

  /**
   * Either:
   *
   * With automatic configuration, make sure Either[LocalDate, Int] works
   */
  final case class SourceDetails7(
    tableName: String,
    partition: Either[LocalDate, Int],
    columns: List[String],
    credentials: SourceDetails6.Credentials,
    renameFileTo: Option[String],
    options: Map[String, String]
  )

  /**
   * Read SourceDetails7 from HOCON
   */
  def readSourceDetails7FromHocon: Either[String, SourceDetails7] = {
    val hocon =
      s"""
         |{
         |  "tableName" : "x",
         |  "partition" : "2019-09-10",
         |  "columns" : [a, b, c, d],
         |  "credentials" : {
         |     "username" : "xyz",
         |     "password" : "123"
         |   }
         |  "renameFileTo" : "a",
         |  "options" : {
         |     "quoteAll" : "true",
         |     "delimiter" :   "|"
         |  }
         |}
         |
         |""".stripMargin

    for {
      source <- TypesafeConfigSource.fromHoconString(hocon)
      config  <- read(descriptor[SourceDetails7] from source).leftMap(_.prettyPrint())
    } yield config
  }

  /**
   * Assume that there are two ways of configuring credentials.
   * One is username and password (say basic), and the other is clientId and tokenId (say advanced)
   *
   * Design the config, and use the  automatic derivation
   *
   */

  sealed trait Credentials[A]

  object Credentials {
    final case class Basic[A](username: A, password: A) extends Credentials[A]
    final case class Advanced[A](clientId: A, tokenId: A) extends Credentials[A]
    final case object NoAuth extends Credentials[String]
  }

  final case class SourceDetails8(
    tableName: String,
    partition: Either[LocalDate, Int],
    columns: List[String],
    credentials: Credentials[String],
    renameFileTo: Option[String],
    options: Map[String, String]
  )


  // Look through the error reports
  // Make it Gadt
  def readSourceDetails8FromHocon: Either[String, SourceDetails8] = {
    val hocon =
      s"""
         |{
         |  "tableName" : "x",
         |  "partition" : "2019-09-10",
         |  "columns" : [a, b, c, d],
         |  "credentials" : {
         |     "basic" : {
         |        "username" : "abc"
         |        "password" : "xyz"
         |     }
         |  }
         |  "renameFileTo" : "a",
         |  "options" : {
         |     "quoteAll" : "true",
         |     "delimiter" :   "|"
         |  }
         |}
         |
         |""".stripMargin

    for {
      source <- TypesafeConfigSource.fromHoconString(hocon)
      config  <- read(descriptor[SourceDetails8] from source).leftMap(_.prettyPrint())
    } yield config
  }

  /**
   * Add documentation to each field so that you get more details when values are missing
   */
  final case class SourceDetails9(
    @describe("the base s3 path directory") tableName: String,
    @describe("A specific partition or the partition that it needs to backtrack") partition: Either[LocalDate, Int],
    @describe("list of columns") columns: List[String],
    credentials: Credentials[String],
    renameFileTo: Option[String],
    @describe("Additional options to run the export job") options: Map[String, String]
  )

  def readSourceDetails9FromHocon: Either[String, SourceDetails9] = {
    val hocon =
      s"""
         |{
         |  "tableName" : "x",
         |  "partition" : "2019-09-10",
         |  "columns" : [a, b, c, d],
         |  "credentials" : {
         |     "basic" : {
         |        "username" : "abc"
         |        "password" : "xyz"
         |     }
         |  }
         |  "renameFileTo" : "a",
         |  "sdsd" : {
         |     "quoteAll" : "true",
         |     "delimiter" :   "|"
         |  }
         |}
         |
         |""".stripMargin

    for {
      source <- TypesafeConfigSource.fromHoconString(hocon)
      config  <- read(descriptor[SourceDetails9] ?? "General config for export" from source).leftMap(_.prettyPrint())
    } yield config
  }

  /**
   * Custom types:
   * Say you have a ZonedDateTime which is not existing in zio-config. Ex: zonedDateTime("something")
   */
  final case class SourceDetails10(
    tableName: String,
    partition: Either[SourceDetails10.Partition[ZonedDateTime], Int],
    columns: List[String],
    credentials: Credentials[String],
    renameFileTo: Option[String],
    options: Map[String, String]
  )

  object SourceDetails10 {
    final case class Partition[A](value: A)

    object Partition {
      // Define custom implicit here
      implicit def descriptorForPartition[A : Descriptor]: Descriptor[Partition[A]] =
        Descriptor[A].xmap[Partition[A]](v => Partition(v), _.value)
    }
  }

  /**
   * Read the ZonedDetails10 from HOCON
   */
  def readSourceDetails10FromHocon: Either[Throwable, SourceDetails10] = {
    implicit def descriptorForZonedDateTime: Descriptor[ZonedDateTime] =
      Descriptor[String]
        .xmapEitherE(v => Try(ZonedDateTime.parse(v)).toEither)(z => Right(z.toString))(e => s"Cannot retrieve zonedDateTime ${e.getMessage}")

    val hocon =
      s"""
         |{
         |  "tableName" : "x",
         |  "partition" : "2",
         |  "columns" : [a, b, c, d],
         |  "credentials" : {
         |     "basic" : {
         |        "username" : "abc"
         |        "password" : "xyz"
         |     }
         |  }
         |  "renameFileTo" : "a",
         |  "options" : {
         |     "quoteAll" : "true",
         |     "delimiter" :   "|"
         |  }
         |}
         |
         |""".stripMargin

    for {
      source <-  TypesafeConfigSource.fromHoconString(hocon).leftMap(t => new RuntimeException(t))
      config  <- read(descriptor[SourceDetails10] from source)
    } yield config
  }

  /**
   * Manipulate keys
   * Read the ZonedDetails10 from HOCON such that keys in HOCON are kebab case
   */
  def readSourceDetails10FromHoconKebab: Either[Throwable, SourceDetails10] = {
    implicit def descriptorForZonedDateTime: Descriptor[ZonedDateTime] =
      Descriptor[String]
        .xmapEitherE(v => Try(ZonedDateTime.parse(v)).toEither)(z => Right(z.toString))(e => s"Cannot retrieve zonedDateTime ${e.getMessage}")

    val hocon =
      s"""
         |{
         |  "table-name" : "x",
         |  "partition" : "2",
         |  "columns" : [a, b, c, d],
         |  "credentials" : {
         |     "basic" : {
         |        "username" : "abc"
         |        "password" : "xyz"
         |     }
         |  }
         |  "rename-file-to" : "a",
         |  "options" : {
         |     "quoteAll" : "true",
         |     "delimiter" :   "|"
         |  }
         |}
         |
         |""".stripMargin

    for {
      source <-  TypesafeConfigSource.fromHoconString(hocon).leftMap(t => new RuntimeException(t))
      config  <- read(descriptor[SourceDetails10].mapKey(camelToKebab) from source)
    } yield config
  }
}
