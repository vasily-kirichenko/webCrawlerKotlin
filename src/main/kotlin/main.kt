import com.github.kittinunf.fuel.httpGet
import com.github.kittinunf.result.Result
import kotlinx.coroutines.experimental.channels.ActorJob

sealed class Message
class Done : Message()
class Mailbox(val actor: ActorJob<Message>) : Message()
class Stop : Message()
class Url(val value: String?) : Message()

val gate = 5

fun extractLinks(html: String): Sequence<String> {
    val pattern1 = """(?i)href\\s*=\\s*(["'])/?((?!#.*|/\B|mailto:|location\.|javascript:)[^\"\']+)(["'])""".toRegex()
    val pattern2 = "(?i)^https?".toRegex()
    return pattern1
        .findAll(html)
        .mapNotNull { it.groups[2]?.value }
        .filter { pattern2.matches(it) }
}

fun fetch(url: String): String? {
    try {
        var res: String? = null
        url
            .httpGet()
            .timeout(5000)
            .responseString { req, resp, result ->
                res =
                    when (result) {
                        is Result.Failure -> null
                        is Result.Success -> {
                            if ("html".toRegex().matches(result.value)) result.value
                            else null
                        }
                    }
            }
        return res
    } catch(_: Throwable) {
        return null
    }
}

fun collectLinks(url: String) = fetch(url)?.let(::extractLinks) ?: emptySequence()
