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

  // kudos to Chris Severs for this workaround, when running "fat jars" -
  // avoids the "ClassNotFoundException cascading.*" exception on a Hadoop cluster

  override def config(implicit mode: Mode): Map[AnyRef, AnyRef] = {
    super.config(mode) ++ Map("cascading.app.appjar.class" -> classOf[Example3])
  }
}
