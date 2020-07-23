package zio.config.workshop

import zio.config.magnolia.DeriveConfigDescriptor.descriptor
import zio.config._

/**
 * This is more of discussing the details, than a corner case of optional.
 */
object OptionalTypeCornerCases {
  final case class MyConfig(a: String, b: Option[String], c: Option[Int])

  val map =
    Map(
      "c" -> "v2",
    )

  /**
   * Could you guess what's the result of the below function ?
   * Fix the value of c to be an integer and see the result.
   * Replace map with Map("b" -> "x") and see the result.
   * Remove all of the values from map and see the result
   * @return
   */
  def run =
    read(descriptor[MyConfig].optional from ConfigSource.fromMap(map))
}
