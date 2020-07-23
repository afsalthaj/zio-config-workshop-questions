package zio.config.workshop

import java.time.ZonedDateTime

import zio.config.ReadError
import zio.config._
import ConfigDescriptor._
import zio.config.typesafe.TypesafeConfigSource
import zio.config.workshop.Reader.SourceDetails4.Partition
import zio.{IO, ZIO}

import scala.util.Try

/**
 *
 * It can do the following
 *
 * 1. It can read config from various sources such as system env, system properties, hocon/json file/string/resource-path , java properties, simple map (and yaml which in progress).
 * 2. It can write the config back to simple scala Map or even to HOCON/Json
 * 3. It can introspect your program and emit a documentation and report based on the config.
 *
 * Details:
 * 5. Given the same config description (which is essentially) a program, we can read, write, document and report.
 * 4. Manually writing the config is given importance too, while the app is capable of automatically deriving the program for you to fetch the config from various sources.
 * 5. Full support for automated derivations.
 * 6. The program to read the config is independent of the source.
 * 7. It accumulates the errors all the way.
 * 8. It can compose different sources
 *
 * Keep a note, zio-config is not a json library, and it is NOT tightly coupled to any protocol/format/source.
 */
object Reader extends EitherSupport {
  final case class Credentials(username: String, password: String)

  object Credentials {
    /**
     * EXERCISE 2:
     * Define a config descriptor for Credentials
     */
    val defaultConfig: ConfigDescriptor[Credentials] = ???
  }

  import Credentials._

  /**
   * EXERCISE 1:
   *
   * Populate Credentials case class using simple scala.Map.
   * Make sure errors are accumulated if the map doesn't consist of the keys we are looking for
   */
  def readSimpleMapNotUsingZioConfig: Either[List[String], Credentials] = {
    val map =
      Map()
    ???
  }

  /**
   * EXERCISE 3:
   *
   * Read the given map to Credentials case class using `Credentials.defaultConfig` description
   */
  def readSimpleMap: Either[ReadError[String], Credentials] = {
    val map =
      Map(
        "username" -> "jon",
        "password" -> "abcd"
      )

    ???
  }

  /**
   * EXERCISE 4:
   *
   * Read the given map to Credentials case class,
   * but make sure the errors are accumulated since there are no
   * keys that are matching the ones in config description
   */
  def readSimpleMapWithError: Either[ReadError[String], Credentials] = {
    val map =
      Map("a" -> "b")

    ???
  }

  /**
   * EXERCISE 5:
   *
   * Read the config from System env to Credentials case class.
   * Instead of using `Config.fromSystemEnv` in zio-config, use the ConfigSource and read function in zio-config.
   * Consider them as a low level API that we can use with zio-config.
   */
  def readCredentialsFromSystemEnv: ZIO[Any, ReadError[String], Credentials] = ???

  /**
   * EXERCISE 6: (similar to EXERCISE 5)
   *
   * Read the config from System properties to Credentials case class.
   * Instead of using `Config.fromSystemProperties` in zio-config, use the ConfigSource and read function in zio.config._.
   */
  def readCredentialsFromSystemProperties: ZIO[Any, ReadError[String], Credentials] = ???

  /**
   * EXERCISE 7
   *
   * Read the config from Json to Credentials case class.
   * Instead of using TypesafeConfig.fromHocon, use the TypesafeConfigSource and read functions= in zio-config.
   * It can be the case you will get Either as a return type and not ZIO
   */
  def readCredentialsFromSystemJson: Either[String, Credentials] = {
    val json =
      s"""
         |
         | {
         |   "username" : "jon"
         |   "password" : "abc"
         | }
         |
         |""".stripMargin
    ???
  }

  /**
   * EXERCISE 8 (Similar to 7, but with errors)
   *
   * Read the config from Json to Credentials case class.
   * Instead of using TypesafeConfig.fromHocon, use the TypesafeConfigSource from read functions.
   * It can be the case you will get Either as a return type and not ZIO.
   * Also this time you get a ReadError in return since our json  is wrong.
   */
  def readCredentialsFromSystemHoconWithError: Either[String, Credentials] = {
    val json =
      s"""
         |
         | {
         |   "username1" : "jon"
         |   "password2" : "abc"
         | }
         |
         |""".stripMargin

    ???
  }


  /**
   * LIST, OPTION and NESTING
   * -------------------------
   * Now that you got a gist of how to load config from various sources.
   * Given 1 single description, it could read any source, and it behaved in the same manner producing same error accumulation.
   */
  final case class SourceDetails(
    tableName: String,
    partitionCount: Int,
    credentials: Credentials
  )

  object SourceDetails {
    /**
     * EXERCISE 9
     *
     * Define the config descriptor for SourceDetails
     */
    val defaultConfig: ConfigDescriptor[SourceDetails] = ???
  }

  /**
   * EXERCISE 10:
   *
   * Read SourceDetails from Map (assuming that it is your system env)
   */
  def readSourceDetails: Either[ReadError[K], SourceDetails] = {
    val map =
      Map(
        "tableName" -> "xyz",
        "partitionCount" -> "xyz",
        "username" -> "xyz",
        "password" -> "xyz"
      )

    ???
  }

  final case class SourceDetails2(
    tableName: String,
    partitionCount: Int,
    columns: List[String],
    credentials: Credentials,
    renameFileTo: Option[String]
  )

  object SourceDetails2 {
    /**
     * EXERCISE 11:
     *
     * Try and write a config description corresponding to the below case case class
     * Note that this time around, we have a list, option and map
     *
     * Have a look at how a common config description is behaving differently
     * to these complex data types.
     *
     * Hint:
     *
     *  {{{
     *    val optionalConfig: ConfigDescriptor[Option[String]] = string("key").optional
     *    val listConfig : ConfigDescriptor[List[String]]      = list("key")(string)
     *    val listConfig2: ConfigDescriptor[List[Int]]         = list("key")(int)
     *    val listConfig3: ConfigDescriptor[List[Int]]         = list("key")(int)
     *  }}}
     *
     *  This is definitely becoming verbose enough for the application, but we will discuss auto derivation in the next section
     */
    val defaultConfig: ConfigDescriptor[SourceDetails2] = ???
  }

  /**
   * EXERCISE 12:
   *
   * Read the SourceDetails2 from a simple map.
   * Consider map as a representation of system environment / system properties.
   * The main take away from this problem is retrieving a list from flattened map like source.
   */
  def readSourceDetails2FromMap: Either[ReadError[String], SourceDetails2] = {
    val map =
      Map(
        "tableName" -> "x",
        "partitionCount" -> "2",
        "columns" -> "a,b,c,d",
        "username" -> "xy",
        "password" -> "xyz",
        "renameFileTo" -> "a"
      )

    ???
  }

  /**
   * EXERCISE 13:
   *
   * Read the SourceDetails2 from a simple HOCON/JSON.
   * Consider map as a representation of system environment / system properties.
   * The main take away from this problem is retrieving a list from HOCON/JSON.
   *
   * The list representation in HOCON is much more nicer rather than representing it in a Map.
   * The only change here is the source and not the description and it is still able to read the List.
   *
   * You may also try giving `renameFileTo: null` or remove `renameFileTo` as such to see
   * the behaviour of optional. All of this work as you would expect
   */
  def readSourceDetails2FromHocon: Either[String, SourceDetails2] = {
    val hocon =
      s"""
         |
         | {
         |   "tableName" : "x",
         |   "partitionCount" : "2",
         |   "columns" : [a, b, c, d],
         |   "username" : "xy",
         |   "password" : "xyz",
         |   "renameFileTo" : "a"
         | }
         |
         |""".stripMargin

    ???
  }

  /**
   * NESTING and MAP:
   * ------------------
   * EXERCISE 14
   *
   * Now although we used HOCON, above the config is still flattened with no proper namespacing.
   *
   * It works for a smaller configuration set up, but as soon as we have a large number of them, it's good to namespace them properly.
   * How do we make it work given we have a nested HOCON/JSON config?
   */
  def readSourceDetails2FromHoconNested: Either[String, SourceDetails2] = {
    val hocon =
      s"""
         |{
         |  "tableName" : "x",
         |  "partitionCount" : "2",
         |  "columns" : [a, b, c, d],
         |  "credentials" : {
         |     "username" : "xyz",
         |     "password" : "123"
         |   }
         |  "renameFileTo" : "a"
         |}
         |
         |""".stripMargin

    // This time around we have to have a new description of the config specifying credentials are nested
    val nonDefaultConfig: ConfigDescriptor[SourceDetails2] = ???

    for {
      source <- TypesafeConfigSource.fromHoconString(hocon)
      config <- read(nonDefaultConfig from source).leftMap(_.prettyPrint())
    } yield config
  }

  /**
   * EXERCISE 15:
   *
   * Now with JSON/HOCON, it was easier for us to represent nested config.
   * But can we achieve the same using a flattened map, which can represent a system environment
   * or system properties.
   */
  def readSourceDetails2FromSystemEnvNested: Either[ReadError[String], SourceDetails2] = {
    val map =
      Map(
        "tableName" -> "x",
        "partitionCount" -> "2",
        "columns" -> "a,b,c,d",
        "credentials_username" -> "xy",
        "credentials_password" -> "xyz",
        "renameFileTo" -> "a"
      )

    // Same as above solution
    val nonDefaultConfig: ConfigDescriptor[SourceDetails2] = ???

    val source =
      ConfigSource.fromMap(map, keyDelimiter = Some('_'), valueDelimiter = Some(','))

    read(nonDefaultConfig from source)
  }


  /**
   * EXERCISE 16: MAP
   *
   * What if your config needs a dynamic set of map.
   *
   * Say for example you have a key called options which holds a set of dynamic key-value pairs
   */
  final case class SourceDetails3(
    tableName: String,
    partitionCount: Int,
    columns: List[String],
    credentials: Credentials,
    renameFileTo: Option[String],
    options: Map[String, String]
  )

  object SourceDetails3 {
    val defaultConfig: ConfigDescriptor[SourceDetails3] = ???

  }

  /**
   * EXERCISE 16:
   *
   * Read the SourceDetails3.defaultConfig from an HOCON source
   */
  def readSourceDetails3FromHocon: Either[String, SourceDetails3] = {
    val hocon =
      s"""
         |{
         |  "tableName" : "x",
         |  "partitionCount" : "2",
         |  "columns" : [a, b, c, d],
         |  "credentials" : {
         |     "username" : "xyz",
         |     "password" : "123"
         |   }
         |  "renameFileTo" : "a",
         |  "options" : {
         |     "quoteAll" : "true",
         |     "delimiter" : "|"
         |  }
         |}
         |
         |""".stripMargin

    ???
  }


  /**
   * EXERCISE 17:
   *
   * MAP in a flattened config
   *
   * Read the SourceDetails3.defaultConfig from a flattened Map that represents SourceDetails3.
   */
  def readSourceDetails3FromSystemEnvNested: Either[ReadError[String], SourceDetails3] = {
    val map =
      Map(
        "tableName" -> "x",
        "partitionCount" -> "2",
        "columns" -> "a,b,c,d",
        "credentials_username" -> "xy",
        "credentials_password" -> "xyz",
        "renameFileTo" -> "a",
        "spark_quoteAll" -> "true",
        "spark_delimiter" -> "|"
      )

    ???
  }

  /**
   * EXERCISE 18: Multiple sources:
   *
   * Now assume that we need to override the partitionCount which happens to be in a HOCON File with a system env.
   *
   * You can consider a scala Map to be system environment for the purpose of example.
   *
   * HINT: There is an updateSource function in ConfigDescriptor
   */
  def readSourceDetails3OverridingPartitionCount: Either[String, SourceDetails3] = {
    val hocon =
      s"""
         |{
         |  "tableName" : "x",
         |  "partitionCount" : "2",
         |  "columns" : [a, b, c, d],
         |  "credentials" : {
         |     "username" : "xyz",
         |     "password" : "123"
         |   }
         |  "renameFileTo" : "a",
         |  "spark" : {
         |     "quoteAll" : "true",
         |     "delimiter" : "|"
         |  }
         |}
         |
         |""".stripMargin


    val systemEnv =
      ConfigSource.fromMap(Map("partitionCount" -> "10"))

    ???
  }

  /**
   *
   * EXERCISE 19: MAP
   *
   * Multiple sources and source priority
   * Now assume that you already have a descriptor that is attached to a source.
   * Update the description such that it is now able to read system environment as well.
   */
  def readSourceDetails3ByUpdatingTheSource: Either[String, SourceDetails3] = {
    val hocon =
      s"""
         |{
         |  "tableName" : "x",
         |  "partitionCount" : "2",
         |  "columns" : [a, b, c, d],
         |  "credentials" : {
         |     "username" : "xyz",
         |     "password" : "123"
         |   }
         |  "renameFileTo" : "a",
         |  "spark" : {
         |     "quoteAll" : "true",
         |     "delimiter" : "|"
         |  }
         |}
         |
         |""".stripMargin


    val systemEnv =
      ConfigSource.fromMap(Map("partitionCount" -> "10"))

    val existingConfig = SourceDetails3.defaultConfig from systemEnv

    ???
  }

  /**
   *
   * EXERCISE 20:
   *
   * Custom data types.
   *
   * Partition below is a custom data type that's not supported by zio-config.
   * Write the description for it.
   */

  final case class SourceDetails4(
    tableName: String,
    partitionCount: Int,
    partition: Partition,
    columns: List[String],
    credentials: Credentials,
    renameFileTo: Option[String],
    options: Map[String, String]
  )

  object SourceDetails4 {
    final case class Partition(value: ZonedDateTime) extends AnyVal

    object Partition {
      /**
       * Custom Types and Documentations
       * Define a config for partition
       * use xmapEither. This will be renamed to `transformEither` before zio-config hits 1.0
       */
      val defaultConfig: ConfigDescriptor[Partition] =
        string("partition")
          .xmapEitherE(v =>
            Try(ZonedDateTime.parse(v)).toEither.map(Partition.apply))(partition => Right(partition.value.toString))(_.getMessage) ?? "should be a zoned date time"
    }

    val defaultConfig: ConfigDescriptor[SourceDetails4] =
      (string("tableName") |@|
        int("partitionCount") |@|
        Partition.defaultConfig |@|
        list("columns")(string) |@|
        nested("credentials")(Credentials.defaultConfig) |@|
        string("renameFileTo").optional |@|
        map("options")(string))(SourceDetails4.apply, SourceDetails4.unapply)
  }


  final case class SourceDetails5(
    tableName: String,
    partitionCount: Either[Partition, Int],
    columns: List[String],
    credentials: Credentials,
    renameFileTo: Option[String],
    options: Map[String, String]
  )

  object SourceDetails5 {
    /**
     * EXERCISE 21:
     *
     * As of now we gave both partitionCount and partition as part of the config.
     * Ideally we need either Partition or the partitionCount
     *
     * There are many ways to solve this problem. A sealed trait, or you can simply use Either[Partition, Int]
     *
     * {{{
     *   val config: ConfigDescriptor[Either[PartitionCount, Int]] = Partition.defaultConfig.orElseEither(int("partitionCount"))
     * }}}
     */
    val defaultConfig: ConfigDescriptor[SourceDetails5] = ???
  }

  /**
   * EXERCISE 21:
   *
   * Discuss more on Either.
   * How does it behave when nothing is given for Either.
   *
   * Try running the below function
   */
  def readSourceDetaisl5FromHocon: Either[String, SourceDetails5] = {
    val hocon =
      s"""
         |{
         |  "tableName" : "x",
         |  "partitionCount" : "1000",
         |  "columns" : [a, b, c, d],
         |  "credentials" : {
         |     "username" : "xyz",
         |     "password" : "123"
         |   }
         |  "renameFileTo" : "a",
         |  "spark" : {
         |     "quoteAll" : "true",
         |     "delimiter" : "|"
         |  }
         |}
         |
         |""".stripMargin

    // See how it behaves if you don'y give partitionCount or partition and renameFileTo
    // Also try and introduce format error. Give name partition and partitionCount the same name

    for {
      hocon <- TypesafeConfigSource.fromHoconString(hocon)
      config <- read(SourceDetails5.defaultConfig from(hocon)).leftMap(_.prettyPrint())
    } yield config
  }


  final case class SourceDetails6(
    tableName: String,
    partitionCount: Either[Partition, Int],
    columns: List[String],
    credentials: Credentials,
    renameFileTo: Option[String],
    options: Map[String, List[String]]
  )

  object SourceDetail6 {
    /**
     * EXERCISE 22:
     *
     * Read more complicated structures.
     *
     * Say you want to read Map[String, List[String]]
     */
    val defaultConfig: ConfigDescriptor[SourceDetails6] = ???
  }

  /**
   * Try running the below code to see the behavior of SourceDetail6.defaultConfig
   */
  def readSourceDetaisl6FromHocon: Either[String, SourceDetails6] = {
    val hocon =
      s"""
         |{
         |  "tableName" : "x",
         |  "partitionCount" : "1000",
         |  "columns" : [a, b, c, d],
         |  "credentials" : {
         |     "username" : "xyz",
         |     "password" : "123"
         |   }
         |  "renameFileTo" : "a",
         |  "options" : {
         |     "characters" : [a, b, c, d],
         |     "integers" : ["1"]
         |  }
         |}
         |
         |""".stripMargin

    // See how it behaves if you don'y give partitionCount or partition and renameFileTo
    // Also try and introduce format error. Give name partition and partitionCount the same name
    for {
      hocon <- TypesafeConfigSource.fromHoconString(hocon)
      config <- read(SourceDetail6.defaultConfig from(hocon)).leftMap(_.prettyPrint())
    } yield config
  }

  /**
   * EXERCISE 23:
   *
   * What does map("input")(string("key")) represents?
   *
   * Try running the below code:
   */
  def complexMap1 = {
    val hocon =
      s"""
         | "input" : {
         |     x : {
         |        "key" : "value",
         |     }
         |
         |     y : {
         |        "key" : "value2"
         |     }
         |  }
         |
         |
         |""".stripMargin

    for {
      hocon <- TypesafeConfigSource.fromHoconString(hocon)
      config <- read(map("input")(string("key")) from hocon).leftMap(_.prettyPrint())
    } yield config
  }

  final case class Keys(key1: String, key2: String)

  object Keys {
    val config: ConfigDescriptor[Keys] =
      (string("key1") |@| string("key2"))(Keys.apply, Keys.unapply)
  }

  /**
   * EXERCISE 24:
   *
   * What does map("input")(nested("key")(list(Keys.config)) represent?
   * Try running the below code, and see the error reporting too
   */
  def complexMap2: Either[String, Map[String, List[Keys]]] = {
    val hocon =
      s"""
         | "input" : {
         |     x : {
         |        "key" :
         |         [
         |           {
         |            "key1" : "value1" ,
         |            "key2" : "value2"
         |           }
         |         ]
         |     }
         |
         |     y :  {
         |       key :
         |        [
         |         {
         |          "key1" : "value2",
         |          "key2" : "value2"
         |         }
         |       ]
         |    }
         |  }
         |
         |
         |""".stripMargin

    for {
      hocon <- TypesafeConfigSource.fromHoconString(hocon)
      config <- read(map("input")(nested("key")(list(Keys.config))) from hocon).leftMap(_.prettyPrint())
    } yield config
  }

}



