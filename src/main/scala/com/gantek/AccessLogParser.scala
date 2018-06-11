package com.gantek

import java.util.regex.Pattern
import java.text.SimpleDateFormat
import java.util.Locale
import scala.util.control.Exception._
import java.util.regex.Matcher
import scala.util.{Try, Success, Failure}

@SerialVersionUID(100)
class AccessLogParser extends Serializable{
  private val ddd = "\\d{1,3}"                      // at least 1 but not more than 3 times (possessive)
  private val ip = s"($ddd\\.$ddd\\.$ddd\\.$ddd)?"  // like `123.456.7.89`
  private val client = "(\\S+)"                     // '\S' is 'non-whitespace character'
  private val user = "(\\S+)"
  private val dateTime = "(\\[.+?\\])"              // like `[21/Jul/2009:02:48:13 -0700]`
  private val request = "\"(.*?)\""                 // any number of any character, reluctant
  private val status = "(\\d{3})"
  private val bytes = "(\\S+)"                      // this can be a "-"
  private val referer = "\"(.*?)\""
  private val agent = "\"(.*?)\""
  private val regex = s"$ip $client $user $dateTime $request $status $bytes $referer $agent"
  private val p = Pattern.compile(regex)

  def parseRecord(record:String):Option[AccessLogRecord]={
    val matcher = p.matcher(record)
    if(matcher.find){
      Some(builAccessLogRecord(matcher))
    }else{
      None
    }
  }

  def parseRecordReturningNullObjectOnFailure(record: String):AccessLogRecord={
    val matcher = p.matcher(record)
    if(matcher.find){
      builAccessLogRecord(matcher)
    }else{
      AccessLogParser.nullObjectAccessLogRecord
    }
  }

  def builAccessLogRecord(matcher: Matcher)={
    AccessLogRecord(
      matcher.group(1),
      matcher.group(2),
      matcher.group(3),
      matcher.group(4),
      matcher.group(5),
      matcher.group(6),
      matcher.group(7),
      matcher.group(8),
      matcher.group(9)
    )
  }
}

object AccessLogParser{
  val nullObjectAccessLogRecord = AccessLogRecord("", "", "", "", "", "", "", "", "")

  def parseRequestField(request:String):Option[Tuple3[String,String,String]]={
    val arr = request.split(" ")
    if(arr.size == 3) Some((arr(0),arr(1),arr(2))) else None
  }

  def parseDateField(field:String):Option[java.util.Date] = {
    val dateRegex = "\\[(.*?) .+]"
    val datePattern = Pattern.compile(dateRegex)
    val dateMatcher = datePattern.matcher(field)
    if(dateMatcher.find){
      val dateString = dateMatcher.group(1)
      println("***** DATE STRING" + dateString)
      val dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH)
      allCatch.opt(dateFormat.parse(dateString)) // return Option[Date]
    }else{
      None
    }
  }
}
