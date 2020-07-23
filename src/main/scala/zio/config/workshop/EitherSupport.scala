package zio.config.workshop

trait EitherSupport {
  implicit class EitherOps[A, B](either: Either[A, B]) {
    def leftMap[C](f: A => C): Either[C, B] = either.swap.map(f).swap
  }

  implicit class TraverseOps[A, B](listEither: List[Either[A, B]]) {
    def sequence: Either[A, List[B]] =
      listEither.foldLeft(Right(Nil): Either[A, List[B]]){
        (acc, either) => acc.flatMap(a => either.map(e => e :: a))
      }
  }
}
