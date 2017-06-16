import com.github.kittinunf.fuel.httpGet
import com.github.kittinunf.result.Result
import kotlinx.coroutines.experimental.CommonPool
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.channels.actor
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.runBlocking
import kotlinx.coroutines.experimental.selects.select
import java.lang.System.err
import java.util.concurrent.TimeUnit
import kotlin.coroutines.experimental.suspendCoroutine

sealed class SupervisorMessage {
    object Done : SupervisorMessage()
    class Mailbox(val actor: Channel<CrawlerMessage>) : SupervisorMessage()
    object Stop : SupervisorMessage()
}

sealed class UrlCollectorMessage {
    class Url(val value: String) : UrlCollectorMessage()
    object Done : UrlCollectorMessage()
    object NoUrls : UrlCollectorMessage()
}

sealed class CrawlerMessage {
    class Url(val value: String) : CrawlerMessage()
    object NoUrls : CrawlerMessage()
    object Stop : CrawlerMessage()
}

val gate = 5

fun extractLinks(html: String): List<String> {
    val pattern1 = """(?i)href\\s*=\\s*(["'])/?((?!#.*|/\B|mailto:|location\.|javascript:)[^\"\']+)(["'])""".toRegex()
    val pattern2 = "(?i)^https?".toRegex()
    return pattern1
        .findAll(html)
        .mapNotNull { it.groups[2]?.value }
        .filter { pattern2.matches(it) }
        .toList()
}

suspend fun fetch(url: String): String = suspendCoroutine { cont ->
    try {
        url.httpGet().timeout(5000).responseString { _, _, result ->
            when (result) {
                is Result.Failure -> cont.resumeWithException(result.getException())
                is Result.Success -> cont.resume(result.value)
            }
        }
    } catch(e: Throwable) {
        cont.resumeWithException(e)
    }
}

suspend fun collectLinks(url: String): List<String> {
    println("Downloading $url...")

    val page =
        try {
            fetch(url)
                .let { if (it.contains("html")) it else null }
                ?.let { extractLinks(it) }
        } catch (e: Throwable) {
            err.println(e)
            null
        }

    return page?.apply { println("Got $this!") }.orEmpty()
}

suspend fun crawl(url: String, limit: Int) {
    val urlsToProcess = Channel<String>(1_000_000)
    val set = HashSet<String>()

    val supervisor = actor<SupervisorMessage>(CommonPool) {
        val msg = receive()
        when (msg) {
            is SupervisorMessage.Mailbox -> {
                val count = set.count()
                if (count < limit - 1) {
                    if (urlsToProcess.isEmpty) msg.actor.send(CrawlerMessage.NoUrls)
                    else {
                        val nextUrl = urlsToProcess.receive()
                        msg.actor.send(if (set.add(nextUrl)) CrawlerMessage.Url(nextUrl) else CrawlerMessage.NoUrls)
                    }
                }
            }
            is SupervisorMessage.Stop -> return@actor
            is SupervisorMessage.Done -> println("Supervisor is done.")
        }
    }

    val urlCollector = actor<UrlCollectorMessage>(CommonPool) {
        var count = 1
        select {
            onReceive { msg ->
                when (msg) {
                    is UrlCollectorMessage.Url -> urlsToProcess.send(msg.value)
                    is UrlCollectorMessage.NoUrls ->
                        when (count) {
                            gate -> supervisor.send(SupervisorMessage.Done)
                            else -> count++
                        }
                }
            }
            onTimeout(6, TimeUnit.SECONDS) {
                supervisor.send(SupervisorMessage.Stop)
            }
        }
    }

    fun crawler(id: Int) = actor<CrawlerMessage>(CommonPool) {
        val msg = receive()
        when (msg) {
            is CrawlerMessage.Url -> {
                val links = collectLinks(msg.value)
                println("${msg.value} crawled by agent $id.")
                links.forEach { urlCollector.send(UrlCollectorMessage.Url(it)) }
                supervisor.send(SupervisorMessage.Mailbox(channel))
            }
            is CrawlerMessage.NoUrls -> supervisor.send(SupervisorMessage.Mailbox(channel))
            is CrawlerMessage.Stop -> urlCollector.send(UrlCollectorMessage.Done)
        }
    }

    val crawlers = List(gate, ::crawler)
    crawlers.first().send(CrawlerMessage.Url(url))
    crawlers.drop(1).forEach { it.send(CrawlerMessage.NoUrls) }
}

fun main(args: Array<String>) = runBlocking {
    crawl("http://wl-legio-27:88", 25)
    delay(10, TimeUnit.SECONDS)
}















