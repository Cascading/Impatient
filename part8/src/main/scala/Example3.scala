import com.twitter.scalding._

class Example3(args : Args) extends Job(args) {
  Tsv(args("doc"), ('doc_id, 'text), skipHeader = true)
    .read
    .flatMap('text -> 'token) { text : String => text.split("[ \\[\\]\\(\\),.]") }
    .mapTo('token -> 'token) { token : String => scrub(token) }
    .filter('token) { token : String => token.length > 0 }
    .groupBy('token) { _.size('count) }
    .write(Tsv(args("wc"), writeHeader = true))

  def scrub(token : String) : String = {
    token
      .trim
      .toLowerCase
  }
}
