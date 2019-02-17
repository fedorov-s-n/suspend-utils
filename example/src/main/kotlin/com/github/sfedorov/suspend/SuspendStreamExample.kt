package com.github.sfedorov.suspend

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking

fun main(args: Array<String>) {
    runBlocking {
        SuspendStream.parallelStream(100_000_000, 10_000_000, 1_000_000, 100_000, 10_000, 1_000)
                .map { it to findPi(it) }
                .forEach { println("operations: ${it.first}, result: ${it.second}, error: ${error(it.second)}") }
    }
}

suspend fun CoroutineScope.findPi(throws: Int) = async {
    var hit = 0
    (1..throws).forEach {
        val x = Math.random()
        val y = Math.random()
        if (x * x + y * y <= 1) ++hit
    }
    return@async (4.0 * hit) / throws
}.await()

suspend fun error(value: Double): Double {
    return Math.abs(value - Math.PI) / Math.PI
}