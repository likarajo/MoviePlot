import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

class MovieSearch extends Serializable {

  def main(args: Array[String]) {

    if (args.length < 2) {
      println("Usage: atleast 2 arguments required => directory word(s)")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("AirportRank")
    val sc = new SparkContext(conf)

    val dir = args(0)
    val words = args(2)

    val lines = sc.textFile(dir + "/plot_summaries.txt")

    val N = lines.count().toInt

    val movie_words = lines.flatMap {
      line => {
        val Array(movieid, text) = line split "\t"
        val words = text.trim.split("""\W+""")
        words.filter(_.length() > 4).map(item => (movieid.trim -> item))
      }
    }

    val word_in_movie = movie_words
      .map {
        case (movieid, word) => (word, movieid)
      }.map {
      case (word, movieid) => ((word, movieid), 1)
    }.reduceByKey {
      case (n1, n2) => n1 + n2
    }.cache()

    val term_freq = word_in_movie // ((word,movie),count)
      .map { case ((w, m), c) => (w, (m, c)) }
      .groupBy { case (w, (m, c)) => w }
      .map { case (w, seq) =>
        val seq2 = seq map {
          case (_, (m, n)) => (m, n)
        }
        (w, seq2.mkString(" ")) // (word1,(movie1,count1),(movie2,count2))
      }

    val doc_freq = word_in_movie // ((word,movie),count)
      .map { x => (x._1) } // (word,movie)
      .map { x => (x._1, 1) } // (word,1)
      .reduceByKey(_ + _) // (word,doc_count)

    val tfdf = term_freq.join(doc_freq) // (word,((movie1,count1),(movie2,count2),doc_count))

    val final_weight = for (a <- tfdf) // (word,[(movie,tf)],df))
      yield {
        val word = a._1
        val df = a._2._2
        val movie_tuples = a._2._1
        val movie_tf = movie_tuples.split(" ")

        val word_tuple_final = for (b <- movie_tf)
          yield {
            val movie = b.split(",")(0).replace("(", "").replace(")", "")
            val tf = b.split(",")(1).replace("(", "").replace(")", "")
            val idf = scala.math.log(N / df.toInt)
            var weight = tf.toInt * idf

            var word_tuple = new ListBuffer[String]()
            word_tuple += word
            word_tuple += movie
            word_tuple += weight.toString
            word_tuple
          }
        word_tuple_final
      }

    //Sorting the final_weight according to the weight calculated: (word,(movie,weight))
    val tfidf = final_weight
      .flatMap(x => x)
      .map(x => (x(0), (x(1), x(2))))
      .sortBy { case (word: String, (movieid: String, tfidf: String)) => -tfidf.toDouble }

    val topMovies = tfidf.filter(x => x._1 == words).take(10)
    topMovies.toSeq.foreach(println)

  }

}
