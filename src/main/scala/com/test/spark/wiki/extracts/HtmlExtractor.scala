package com.test.spark.wiki.extracts

import org.jsoup.Jsoup
import org.jsoup.nodes.Element
import org.jsoup.select.Elements

import scala.collection.JavaConverters._

object HtmlExtractor {

  def extract(season: Int, league: String, url: String): Seq[LeagueStanding] = {
    val doc = Jsoup.connect(url).get
    val table = doc.select("table.wikitable")
    val rows = findClassementTable(table)
    rows.asScala.tail.map(row => extractDataFromRow(row, season, league))
  }

  def extractDataFromRow(row: Element, season: Int, league: String) = {

    val position = extractNumber(row.getElementsByTag("td").get(0).text)
    val team = row.select("a").get(0).text().trim
    val points = extractNumber(row.getElementsByTag("td").get(2).text)
    val played = row.getElementsByTag("td").get(3).text.trim.toInt
    val won = row.getElementsByTag("td").get(4).text.trim.toInt
    val drawn = row.getElementsByTag("td").get(5).text.trim.toInt
    val lost = row.getElementsByTag("td").get(6).text.trim.toInt
    val goalsFor = row.getElementsByTag("td").get(7).text.trim.toInt
    val goalsAgainst = row.getElementsByTag("td").get(8).text.trim.toInt
    val goalsDifference = row.getElementsByTag("td").get(9).text.trim.toInt

    LeagueStanding(league,
      season,
      position,
      team,
      points,
      played,
      won,
      drawn,
      lost,
      goalsFor,
      goalsAgainst,
      goalsDifference)
  }

  def findClassementTable(tables: Elements): Elements = {
    if (tables.size() == 1)
      return tables.get(0).select("tr")

    tables.asScala
      .filter(isClassementTable)
      .map(_.select("tr")).head
  }

  def isClassementTable(table: Element) = {
    table.select("th") != null &&
      table.select("th").first() != null &&
      Seq("Place", "Rang").contains(table.select("th").first().text())
  }

  def extractNumber(ss: String) ={
    ("""\d+""".r findFirstIn ss.trim get) toInt
  }

}
