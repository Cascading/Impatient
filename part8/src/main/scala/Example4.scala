import com.twitter.scalding._

class Example4(args : Args) extends Job(args) {
  val stopPipe = Tsv(args("stop"), ('stop), skipHeader = true)
    .read

  Tsv(args("doc"), ('doc_id, 'text), skipHeader = true)
    .read
    .flatMap('text -> 'token) { text : String => text.split("[ \\[\\]\\(\\),.]") }
    .mapTo('token -> 'token) { token : String => scrub(token) }
    .filter('token) { token : String => token.length > 0 }
    .leftJoinWithTiny('token -> 'stop, stopPipe)
    .filter('stop) { stop : String => stop == null }
    .groupBy('token) { _.size('count) }
    .write(Tsv(args("wc"), writeHeader = true))

  def scrub(token : String) : String = {
    token
      .trim
      .toLowerCase
  }
}
