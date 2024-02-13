package ru.alfabank.redis.pooling.controller

import mu.KLogging
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory
import org.springframework.data.redis.connection.RedisStringCommands
import org.springframework.data.redis.connection.ReturnType
import org.springframework.data.redis.core.types.Expiration
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.io.File
import java.nio.ByteBuffer
import java.time.Duration
import java.util.*

@RestController
class PoolingController(
    private val redisConnectionFactory: ReactiveRedisConnectionFactory
) {
    @PostMapping("/")
    fun put(@RequestBody key: String): Mono<Boolean> =
        redisConnectionFactory
            .reactiveConnection
            .stringCommands()
            .set(
                "$key-mainRedesigned".toByteBuffer(),
                File("./src/main/resources/static/response.json")
                    .readText(Charsets.UTF_8).toByteBuffer()
            )

    @GetMapping("/")
    fun getByKey(@RequestParam key: String) =
        redisConnectionFactory
            .reactiveConnection
            .stringCommands()
            .get("$key-mainRedesigned".toByteBuffer())
            .map { it.convertToString() }

    @PostMapping("/lock")
    fun setByLock(@RequestBody key: String): Mono<Boolean> {
        val lockKey = "sync-locks-$key-mainRedesigned".toByteBuffer()
        val lockValue = UUID.randomUUID().toString().toByteBuffer()
        return runWithLock(key, lockKey, lockValue)
    }

    fun runWithLock(key: String, lockKey: ByteBuffer, lockValue: ByteBuffer): Mono<Boolean> =
        Mono.usingWhen(
            doAcquireOrDie(lockKey, lockValue),
            { action(key) },
            { release(lockKey, lockValue) }
        )

    private fun action(key: String): Mono<Boolean> {
        logger.info { "Obtain lock: $key" }
        return redisConnectionFactory
            .reactiveConnection
            .stringCommands()
            .set(
                "$key-mainRedesigned".toByteBuffer(),
                File("./src/main/resources/static/response.json")
                    .readText(Charsets.UTF_8).toByteBuffer()
            )
    }

    private fun doAcquireOrDie(lockKey: ByteBuffer, lockValue: ByteBuffer): Mono<Boolean> =
        redisConnectionFactory
            .reactiveConnection
            .stringCommands()
            .set(
                lockKey,
                lockValue,
                Expiration.from(Duration.ofMillis(7000)),
                RedisStringCommands.SetOption.SET_IF_ABSENT
            )

    private fun release(lockKey: ByteBuffer, lockValue: ByteBuffer): Flux<Void> =
        redisConnectionFactory
            .reactiveConnection.scriptingCommands()
            .eval(
                DELETE_KEY_SCRIPT.trimMargin().toByteBuffer(),
                ReturnType.fromJavaType(Void::class.java),
                1,
                lockKey,
                lockValue
            )

    companion object : KLogging() {
        private val DEFAULT_CHARSET = Charsets.UTF_8
        fun String.toByteBuffer(): ByteBuffer = ByteBuffer.wrap(toByteArray(DEFAULT_CHARSET))
        fun ByteBuffer.convertToString(): String = String(array(), DEFAULT_CHARSET)
        private const val DELETE_KEY_SCRIPT = """if redis.call('get', KEYS[1]) == ARGV[1]
            | then 
            |  redis.call('del', KEYS[1])
            | end"""
    }
}