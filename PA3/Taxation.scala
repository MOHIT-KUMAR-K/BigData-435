import org.apache.spark.sql.SparkSession

object Taxation {
  def main(args: Array[String]): Unit = {
//    val sc = SparkSession.builder().master("local").getOrCreate().sparkContext
val sc = SparkSession.builder().master("yarn").getOrCreate().sparkContext

    val lines = sc.textFile(args(0))
    val titles = sc.textFile(args(1)).zipWithIndex().mapValues(x => x + 1).map(_.swap);
    val links = lines.map(s => (s.split(": ")(0), s.split(": ")(1).split("//s+")))
    val total = lines.count()

    var ranks = links.mapValues(v => 1.0 / total)

    for (i <- 1 to 25) {
      val tempRank = links.join(ranks).values.flatMap {
        case (urls, rank) =>
          val noOfOutgoingLinks = urls.size
          urls.map(url => (url, rank / noOfOutgoingLinks))
      }
      ranks = tempRank.reduceByKey(_ + _).mapValues((0.15 / total) + 0.85 * _)
    }
    val titlefinal = titles.map { case (index, name) => (index.toString, name) }
    val finalrank = titlefinal.join(ranks).values
    val sortPR = finalrank.sortBy(_._2, false).take(10)
    sc.parallelize(sortPR.toSeq).saveAsTextFile(args(2))

  }
}
