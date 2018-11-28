package com.test.spark.wiki.extracts

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions._

case class Q2_ShowLeagueStatsTask(bucket: String) extends Runnable {
  private val session: SparkSession = SparkSession.builder().master("local").getOrCreate()

  import session.implicits._
  import org.apache.spark.sql.functions.udf

  override def run(): Unit = {
    val standings = session.read.parquet(bucket).as[LeagueStanding].cache()

    // TODO Répondre aux questions suivantes en utilisant le dataset $standings
    standings
      // ...code...
      .show()

    // TODO Q1
    println("Utiliser createTempView sur $standings et, en sql, afficher la moyenne de buts par saison et " +
      "par championnat")
    standings.createOrReplaceTempView("standings")
    session.sql("select season, league, avg(goalsFor) from standings group by season, league").show(10, false)

    // TODO Q2
    println("En Dataset, quelle est l'équipe la plus titrée de France ?")
    session.sql("SELECT team, " +
      "count(*) OVER (PARTITION BY team) as cnt from standings where league = 'Ligue 1' and position = 1 order by cnt desc limit 1").show(10, false)


    // TODO Q3
    println("En Dataset, quelle est la moyenne de points des vainqueurs sur les 5 différents championnats ?")
    session.sql("SELECT league, " +
      "avg(points) from standings where position = 1 group BY league ").show(10, false)


    // TODO Q5 Ecrire une udf spark "decade" qui retourne la décennie d'une saison sous la forme 199X ?
    val decade: Int => String = y => y / 10 + "X"
    val decadeUdf = udf(decade)

    // TODO Q4
    println("En Dataset, quelle est la moyenne de points d'écart entre le 1er et le 10ème de chaque championnats " +
      "par décennie")

    standings.where(($"position" === 1)).withColumnRenamed("points", "first")
      .join(standings.where($"position" === 10).withColumnRenamed("points", "tenth")
        , Seq("season", "league"))
      .withColumn("difference", when(col("first").isNull, lit(0)).otherwise(col("first"))
        - when(col("tenth").isNull, lit(0)).otherwise(col("tenth")))
      .withColumn("decade", decadeUdf('season)).groupBy("league", "decade").avg("difference").orderBy("league")
      .show(10, false)
  }
}
