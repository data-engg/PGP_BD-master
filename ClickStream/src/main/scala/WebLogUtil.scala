import java.util.regex.{Matcher, Pattern}

object WebLogUtil {

  def logFields(line : String) : Array[String] = {
    val pattern: String = "^(?<ip>[\\w/.]+)" +
      " (?<id>\\S+) " +
      "(?<pwd>\\S+) " +
      "\\[(?<ts>([\\w/:]+)\\s((\\-|\\+)\\d{4}))\\] " +
      "\"(?<req>.+)\" " +
      "(?<response>\\d{3}) " +
      "(?<byte>\\d+) " +
      "(?<agent>.*)\" " +
      "\"(?<ref>\\S+)\"$"

    val logPat: Pattern = Pattern.compile(pattern)
    val matcher: Matcher = logPat.matcher(line.trim())
    matcher.matches()

    val tokens: Array[String] = Array(matcher.group("ip"), matcher.group("id"), matcher.group("pwd")
      , matcher.group("ts").replaceAll("[\\[\\]]","")
      , matcher.group("req").replaceAll("\"","")
      , matcher.group("response"), matcher.group("byte")
      , matcher.group("agent").replaceAll("\"","")
      , matcher.group("ref")
    )

    return tokens
  }

  def logFormatValidate(line : String): Boolean ={
    val pattern : String = "^(?<ip>[\\w/.]+)" +
      " (?<id>\\S+) " +
      "(?<pwd>\\S+) "+
      "\\[(?<ts>([\\w/:]+)\\s((\\-|\\+)\\d{4}))\\] "+
      "\"(?<req>.+)\" "+
      "(?<response>\\d{3}) " +
      "(?<byte>\\d+) " +
      "(?<agent>.*)\" "+
      "\"(?<ref>\\S+)\"$"

    if (line.matches(pattern))
      return true
    else
      return false
  }

}
