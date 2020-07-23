package zio.config.workshop

import zio.{ExitCode, URIO, ZIO}
import zio.console.Console
import zio.config._
import zio.console.putStrLn
import zio.config.magnolia.DeriveConfigDescriptor.descriptor

object ZioConfigVerboseExample {
  object MainApp extends zio.App {
    override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {
      val getConfig =
        for {
          source <- ConfigSource.fromSystemEnv
          v <- ZIO.fromEither(read(descriptor[AppConfig.MyConfig] from source))
          _ <- BusinessLogic.run(v)
        } yield v

      getConfig.foldM(v => putStrLn(v.prettyPrint()) *> ZIO.succeed(ExitCode.failure), _ => ZIO.succeed(ExitCode.success ))
    }
  }

  object BusinessLogic {
    def run(config: AppConfig.MyConfig): ZIO[Console, Nothing, Unit] =
      for {
        _ <- putStrLn(config.toString)
      } yield ()
  }

  object AppConfig {
    final case class MyConfig(username: String, password: String)
  }
}

object ZIOConfigZLayerExample {
  object MainApp extends zio.App {
    override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {
      val app =
        BusinessLogic.run
          .provideCustomLayer(
            Config.fromSystemEnv(descriptor[AppConfig.X])
          )

      app.foldM(v => putStrLn(v.prettyPrint()) *> ZIO.succeed(ExitCode.failure), _ => ZIO.succeed(ExitCode.success ))
    }
  }

  object BusinessLogic {
    def run: ZIO[Console with Config[AppConfig.X], Nothing, Unit] =
      for {
        x <- config[AppConfig.X]
        _ <- putStrLn(x.toString)
      } yield ()
  }

  object AppConfig {
    final case class X(username: String, password: String)
  }
}
