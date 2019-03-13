import org.apache.spark.{SparkConf, SparkContext}

object MovieSearch2 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("MovieSearch2")
      .setMaster("local") // remove when running on a Spark cluster

    val sc = new SparkContext(conf)

    val dir = args(0)
    val query = args(1)

    val lookup = sc.textFile(dir + "/movie.metadata.tsv").map(row => (row.split("\t")(0), row.split("\t")(2)))

    val moviesum = sc.textFile(dir + "/plot_summaries.txt")

    //preprocessing the data
    val movieWords = moviesum.map(row => row.replaceAll("""[\p{Punct}]""", " ").toLowerCase.split("""\s+"""))
    val filteredWords = movieWords.map(row => row.map(text => text).filter(text => text.length() > 4 ))

    //extracting search data
    val terms = query.split("\\s+").map(_.trim).toList

    val search = sc.parallelize(terms)

    val words = search.cartesian(filteredWords)

    val wordsMapper = words.map(r => ((r._1, r._2(0)), r._2))

    val _TF = wordsMapper.map(row => (row._1, row._2.count(_.equals(row._1._1)).toDouble / row._2.size)).filter(freq => freq._2 != 0.0)
    val TF = _TF.map(r => ((r._1._1), (r._1._2, r._2)))

    val _DF = _TF.map(item => (item._1._1, 1)).reduceByKey(_ + _)

    val N = moviesum.count()
    val IDF = {_DF.map(r => (r._1, Math.log(N / r._2)))}

    val TfIDfjoin = TF.join(IDF)

    val TFIDF = TfIDfjoin.map(r => ((r._1, r._2._1._1), (r._2._1._2) * r._2._2))
    val searchCount = search.count()

    if (searchCount == 1) {

      val TF_IDF = TFIDF.map(row => (row._1._2, row._2))
      val topTen = lookup.join(TF_IDF).map(row => (row._2._1, row._2._2)).sortBy(-_._2).take(10)
      val topTenRDD = sc.parallelize(topTen)

      topTenRDD.saveAsTextFile(dir + "/output")

    } else {

      val queryCount = search.map(r => (r, 1)).reduceByKey(_ + _)

      val freq = queryCount.map(r => (r._1, r._2.toDouble / searchCount))

      val IDF_freq = IDF.join(freq)

      val TFIDF_Query = IDF_freq.map(r => (r._1, r._2._1 * r._2._2))

      val TFIDF_combine = TFIDF.map(r => ((r._1._1), (r._1._2, r._2)))

      val joinedTFIDF = TFIDF_combine.join(TFIDF_Query)

      //Cosine Similarity
      val CS = joinedTFIDF.map(r => ((r._2._1._1), (r._1, r._2._1._2 * r._2._2))).map(r => (r._1, r._2._2)).reduceByKey(_ + _)
      val topTenQ = lookup.join(CS).map(r => (r._2._1, r._2._2)).sortBy(-_._2).take(10)
      val topTenQRDD = sc.parallelize(topTenQ)
      topTenQRDD.coalesce(1).saveAsTextFile(dir + "/output")

    }

    sc.textFile(dir + "/output/part-*").collect().foreach(println)

  }

}