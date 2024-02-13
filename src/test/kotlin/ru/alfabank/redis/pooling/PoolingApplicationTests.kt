package ru.alfabank.redis.pooling

import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory
import reactor.test.StepVerifier
import ru.alfabank.redis.pooling.configuration.TestRedisConfiguration
import java.nio.ByteBuffer

@SpringBootTest(classes = [TestRedisConfiguration::class])
class PoolingApplicationTests {

    @Autowired
    private lateinit var redisConnectionFactory: ReactiveRedisConnectionFactory

    @Test
    fun test() {
        val connection = redisConnectionFactory.reactiveConnection
        val lPush = connection.listCommands()
            .lPush("key".toByteBuffer(), listOf("first".toByteBuffer(), "second".toByteBuffer()))
            .log("Pushed")
        StepVerifier.create(lPush)
            .expectNext(2L)
            .verifyComplete()

        val lPop = connection.listCommands().lPop("key".toByteBuffer())
            .log("Popped")
        StepVerifier.create(lPop)
            .expectNext("second".toByteBuffer())
            .verifyComplete()
    }

    companion object {
        private val DEFAULT_CHARSET = Charsets.UTF_8
        fun String.toByteBuffer(): ByteBuffer = ByteBuffer.wrap(toByteArray(DEFAULT_CHARSET))
    }
}
