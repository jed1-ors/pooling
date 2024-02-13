package ru.alfabank.redis.pooling.configuration

import mu.KLogging
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.context.event.EventListener
import org.springframework.data.redis.connection.RedisStandaloneConfiguration
import org.springframework.test.context.event.BeforeTestMethodEvent
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import javax.annotation.PostConstruct
import javax.annotation.PreDestroy

@TestConfiguration
class TestRedisConfiguration {

    private val redis: KGenericContainer =
        KGenericContainer(DEFAULT_REDIS_IMAGE)
            .waitingFor(Wait.defaultWaitStrategy())
            .withExposedPorts(DEFAULT_REDIS_PORT)
            .withEnv("ALLOW_EMPTY_PASSWORD", "yes")

    private var mappedPort: Int? = null

    @PostConstruct
    fun startRedisContainer() {
        // Для тестов на рестарт/дисконнект: поднять редис на том же порту
        logger.info { "Starting redis container..." }
        if (mappedPort != null) {
            redis.portBindings = listOf("$mappedPort:$DEFAULT_REDIS_PORT")
        }
        redis.start()
        mappedPort = redis.firstMappedPort
        // нужно дождаться тут реконнекта, чтобы не ломались в тестах проверки
        // TODO ждать в тестах до реконнекта только там, где нужно?
        Thread.sleep(TIMEOUT_4_RECONNECT_MS)
    }

    @PreDestroy
    fun stopRedisContainer() {
        logger.info { "Stopping redis container..." }
        redis.stop()
    }

    @Bean
    @Primary
    fun testRedisStandaloneConfiguration() =
        RedisStandaloneConfiguration(
            redis.host,
            redis.firstMappedPort
        )

    @EventListener(BeforeTestMethodEvent::class)
    fun cleanUpCache() {
        if (redis.isRunning) redis.execInContainer("redis-cli", "flushdb")
    }

    companion object : KLogging() {
        private const val DEFAULT_REDIS_PORT = 6379
        private const val DEFAULT_REDIS_IMAGE = "redis:6.2.6"
        private const val TIMEOUT_4_RECONNECT_MS = 2000L
    }
}

class KGenericContainer(imageName: String) : GenericContainer<KGenericContainer>(imageName)