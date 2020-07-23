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
 * Take a look at Runner to call the function that you need to verify.
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
        "USERNAME" -> "jon",
        "PASSWORD" -> "abcd"
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
   * Read the config from Json to Credentials case class.
   * Instead of using TypesafeConfig.fromHocon, use the TypesafeConfigSource from read functions.
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

    for {
      source <- TypesafeConfigSource.fromHoconString(json)
      config <- read(defaultConfig from source).leftMap(_.prettyPrint())
    } yield config
  }

  /**
   * Read the config from Json to Credentials case class.
   * Instead of using TypesafeConfig.fromHocon, use the TypesafeConfigSource from read functions.
   * It can be the case you will get Either as a return type and not ZIO.
   * Also this time you get a ReadError in return since our json  is wrong.
   */
  def readCredentialsFromSystemHoconWithError: Either[String, Credentials] = {
    val hocon =
      s"""
         |
         | {
         |   "username1" : "jon"
         |   "password2" : "abc"
         | }
         |
         |""".stripMargin

    for {
      source <- TypesafeConfigSource.fromHoconString(hocon)
      config <- read(defaultConfig from source).leftMap(_.prettyPrint())
    } yield config
  }


  /**
   * Now that you got a gist of the idea, that given the same program
   * it could read any source, and it behaved in the same manner producing same error accumulation.
   * Let's go with the behaviour of nesting
   */

  final case class SourceDetails(
    tableName: String,
    partitionCount: Int,
    credentials: Credentials
  )

  object SourceDetails {
    val defaultConfig =
      (string("tableName") |@| int("partitionCount") |@| Credentials.defaultConfig)(SourceDetails.apply, SourceDetails.unapply)
  }

  /**
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
    read(SourceDetails.defaultConfig from ConfigSource.fromMap(map))
  }


  /**
   * Try and write a config description corresponding to the below case case class
   * Note that this time around, we have a list, option and map
   *
   * Have a look at how a common config description is behaving differently
   * to these complex data types.
   *
   * Hint:
   *
   *  {{{
   *    val optionalConfig: ConfigDescriptor[Option[String]]= string("key").optional
   *    val listConfig : ConfigDescriptor[List[String]] = list("key")(string)
   *    val listConfig2: ConfigDescriptor[List[Int]] = list("key")(int)
   *    val listConfig3: ConfigDescriptor[List[Int]] = list("key")(int)
   *  }}}
   */
  final case class SourceDetails2(
    tableName: String,
    partitionCount: Int,
    columns: List[String],
    credentials: Credentials,
    renameFileTo: Option[String]
  )

  object SourceDetails2 {
    // This is slowly becoming verbose enough for the application.
    val defaultConfig: ConfigDescriptor[SourceDetails2] =
      (string("tableName") |@|
        int("partitionCount") |@|
        list("columns")(string) |@|
        Credentials.defaultConfig |@|
        string("renameFileTo").optional
        )(SourceDetails2.apply, SourceDetails2.unapply)
  }

  /**
   * Read the SourceDetails2 from a simple map.
   * Consider map as sort of our system environment / system properties.
   * The main take away from this problem is retrieving a list from flattned map like source.
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

    read(SourceDetails2.defaultConfig from ConfigSource.fromMap(map, valueDelimiter = Some(',')))
  }

  /**
   * Read the SourceDetails2 from a simple hocon.
   * Consider map as sort of our system environment / system properties.
   * The main take away from this problem is retrieving a list from Hocon Json.
   *
   * The list representation in HOCON is much more nicer rather than fiddling with delimiters.
   * Same time, note that the only thing that we change here is the source and not the description
   * and it is still able to read the List.
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

    for {
      source <- TypesafeConfigSource.fromHoconString(hocon)
      config <- read(SourceDetails2.defaultConfig from source).leftMap(_.prettyPrint())
    } yield config
  }

  /**
   *
   * Nesting:
   *
   * Now although we used HOCON above the config is still flattened with no readable grouping.
   * It works for a smaller configuration set up, but as soon as we have a large number of them it's good to namespace them properly.
   * How do we make it work given we have a nested HOCON/json config?
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
    val nonDefaultConfig =
      (string("tableName") |@|
        int("partitionCount") |@|
        list("columns")(string) |@|
        nested("credentials")(Credentials.defaultConfig) |@|
        string("renameFileTo").optional
        )(SourceDetails2.apply, SourceDetails2.unapply)

    for {
      source <- TypesafeConfigSource.fromHoconString(hocon)
      config <- read(nonDefaultConfig from source).leftMap(_.prettyPrint())
    } yield config
  }

  /**
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

    // This time around we have to have a new description of the config specifying credentials are nested
    val nonDefaultConfig =
      (string("tableName") |@|
        int("partitionCount") |@|
        list("columns")(string) |@|
        nested("credentials")(Credentials.defaultConfig) |@|
        string("renameFileTo").optional
        )(SourceDetails2.apply, SourceDetails2.unapply)

    val source =
      ConfigSource.fromMap(map, keyDelimiter = Some('_'), valueDelimiter = Some(','))

    read(nonDefaultConfig from source)
  }


  /**
   * Nesting:
   *
   * Now that we know regardless of the source we are a
   * What if your config needs a dynamic set of map.
   *
   * Say for example you have a key called options which in turn holds all the key value pairs.
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
    val defaultConfig =
      (string("tableName") |@|
        int("partitionCount") |@|
        list("columns")(string) |@|
        nested("credentials")(Credentials.defaultConfig) |@|
        string("renameFileTo").optional |@|
        map("spark")(string)
        )(SourceDetails3.apply, SourceDetails3.unapply)

  }

  /**
   * Nesting:
   *
   * Read the SourceDetails3.defaultConfig from an hocon source
   * @return
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
         |  "spark" : {
         |     "quoteAll" : "true",
         |     "delimiter" : "|"
         |  }
         |}
         |
         |""".stripMargin

    for {
      source <- TypesafeConfigSource.fromHoconString(hocon)
      config <- read(SourceDetails3.defaultConfig from source).leftMap(_.prettyPrint())
    } yield config
  }


  /**
   *
   *  Nesting in a flattened config
   *
   *  Read the SourceDetails3.defaultConfig from a flattened Map that represents
   *  SourceDetails3.
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

    val source =
      ConfigSource.fromMap(map, keyDelimiter = Some('_'), valueDelimiter = Some(','))

    read(SourceDetails3.defaultConfig from source)
  }

  /**
   * Multiple sources:
   *
   * Now assume that we need to override the partitionCount which happens to be in a hoconFile
   * with a system env.
   *
   * You can assume a scala Map to be system environment
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


    val dummySystemEnv =
      Map("partitionCount" -> "10")

    for {
      hocon <- TypesafeConfigSource.fromHoconString(hocon)
      env   = ConfigSource.fromMap(dummySystemEnv)
      source = env.orElse(hocon)
      config <- read(SourceDetails3.defaultConfig from source).leftMap(_.prettyPrint())
    } yield config
  }

  /**
   * Multiple sources and source priority
   * Now assume that you have already a descriptor that is already attached to a source.
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


    val dummySystemEnv =
      ConfigSource.fromMap(Map("partitionCount" -> "10"))

    val existingConfig = SourceDetails3.defaultConfig from dummySystemEnv

    for {
      hocon <- TypesafeConfigSource.fromHoconString(hocon)
      config <- read(existingConfig.updateSource(_.orElse(hocon))).leftMap(_.prettyPrint())
    } yield config
  }

  /**
   * Custom behaviours.
   * Assume that you have a type called Partition that holds the a ZonedDate Time
   * that's not supported by zio-config.
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
    val defaultConfig: ConfigDescriptor[SourceDetails4] =
      (string("tableName") |@|
        int("partitionCount") |@|
        Partition.defaultConfig |@|
        list("columns")(string) |@|
        nested("credentials")(Credentials.defaultConfig) |@|
        string("renameFileTo").optional |@|
        map("options")(string))(SourceDetails4.apply, SourceDetails4.unapply)

    final case class Partition(value: ZonedDateTime) extends AnyVal

    object Partition {
      /**
       * Custom Types and Documentations
       * Define a config for partition
       * use xmapEither. This will be renamed to `transformEither` before it is 1.0
       *
       */
      val defaultConfig: ConfigDescriptor[Partition] =
        string("partition")
          .xmapEitherE(v =>
            Try(ZonedDateTime.parse(v)).toEither.map(Partition.apply))(partition => Right(partition.value.toString))(_.getMessage) ?? "should be a zoned date time"
    }
  }

  /**
   *
   * Either:
   *
   * As of now we gave both partitionCount and partition as part of the config.
   * Ideally we need either Partition or the partitionCount
   *
   * There are many ways to solve this problem. A sealed trait, or you can simply Either[Partition, Int]
   *
   * {{{
   *   val config: ConfigDescriptor[Either[PartitionCount, Int]] = Partition.defaultConfig |@| int("partitionCount")
   * }}}
   *
   */

  final case class SourceDetails5(
    tableName: String,
    partitionCount: Either[Partition, Int],
    columns: List[String],
    credentials: Credentials,
    renameFileTo: Option[String],
    options: Map[String, String]
  )

  object SourceDetails5 {
    val defaultConfig: ConfigDescriptor[SourceDetails5] =
      (string("tableName") |@|
        Partition.defaultConfig.orElseEither(int("partitionCount")) |@|
        list("columns")(string) |@|
        nested("credentials")(Credentials.defaultConfig) |@|
        string("renameFileTo").optional |@|
        map("spark")(string))(SourceDetails5.apply, SourceDetails5.unapply)
  }

  /**
   * Discuss more on Either.
   * How does it behave when none is given in an Either.
   * Read SourceDetails5.defaultConfig from a HOCON
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

  /**
   * Read more complicated structures:
   * Say you want to read Map[String, List[String]]
   */
  final case class SourceDetails6(
    tableName: String,
    partitionCount: Either[Partition, Int],
    columns: List[String],
    credentials: Credentials,
    renameFileTo: Option[String],
    options: Map[String, List[String]]
  )

  object SourceDetail6 {
    val defaultConfig: ConfigDescriptor[SourceDetails6] =
      (string("tableName") |@|
        Partition.defaultConfig.orElseEither(int("partitionCount")) |@|
        list("columns")(string) |@|
        nested("credentials")(Credentials.defaultConfig) |@|
        string("renameFileTo").optional |@|
        map("options")(list(string)))(SourceDetails6.apply, SourceDetails6.unapply)
  }

  /**
   * Read SourceDetail6 from the HOCON
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
   * What does map("input")(string("key")) represents?
   */
  def complexMap1 = {
    val hocon =
      s"""
         | "input" : {
         |     x : {
         |        "key" : "value"
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
   * What does map("input")(nested("key")(list(Keys.config)) represent?
   * See the error messages as well
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



