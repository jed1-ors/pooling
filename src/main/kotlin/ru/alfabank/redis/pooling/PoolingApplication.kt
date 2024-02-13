package ru.alfabank.redis.pooling

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class PoolingApplication

fun main(args: Array<String>) {
	runApplication<PoolingApplication>(*args)
}