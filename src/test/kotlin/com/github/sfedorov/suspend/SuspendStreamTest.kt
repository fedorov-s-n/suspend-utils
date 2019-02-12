package com.github.sfedorov.suspend

import com.github.sfedorov.suspend.SuspendStream.Companion.emptyStream
import com.github.sfedorov.suspend.SuspendStream.Companion.parallelStream
import com.github.sfedorov.suspend.SuspendStream.Companion.sequentialStream
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import org.junit.ComparisonFailure
import org.junit.Test
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.fail

class SuspendStreamTest {
    fun <T, R> SuspendStream<T>.expect(expected: R, collect: suspend SuspendStream<T>.() -> R) {
        assertEquals(expected, runBlocking { collect() })
    }

    fun <T> SuspendStream<T>.expect(vararg expected: T) {
        val actual = runBlocking { toList() }
        if (actual.size != expected.size || (0..(actual.size - 1)).any { actual[it] != expected[it] }) {
            throw ComparisonFailure("stream contents differ from expected", expected.asList().toString(), actual.toString())
        }
    }

    @Test
    fun `compatibility of sequential stream`() {
        sequentialStream(1, 2).map { "x$it" }.expect("x1", "x2")
        sequentialStream(1, 2, 3).filter { it > 1 }.expect(2, 3)
        sequentialStream(1.toInt(), 2.toLong(), 3.toInt()).filter<Long>().expect(2L)
        sequentialStream(1, 2).flatMap { sequentialStream(it * 2, it * 3, it * 4) }.expect(2, 3, 4, 4, 6, 8)

        StringBuilder().let { builder ->
            sequentialStream(1, 2, 3)
                .peek { builder.append(it) }
                .expect("123") { toList().let { builder.toString() } }
        }
        sequentialStream(3, 2, 1).sort().expect(1, 2, 3)
        sequentialStream(1, 2, 3, 1, 2, 4).distinct().expect(1, 2, 3, 4)

        sequentialStream(1, 2, 3).limit(2).expect(1, 2)
        sequentialStream(1, 2, 3).skip(1).expect(2, 3)
        sequentialStream(0) { it + 1 }.limit(3).expect(0, 1, 2)
        sequentialStream(0) { it + 1 }.skip(2).limit(3).expect(2, 3, 4)
        sequentialStream(0) { it + 1 }.limit(3).skip(2).expect(2)
        sequentialStream(1, 2, 3).takeWhile { it < 2 }.expect(1)
        sequentialStream(1, 2, 3).skipUntil { it < 2 }.expect(2, 3)
        sequentialStream(0) { it + 1 }.takeWhile { it < 2 }.expect(0, 1)
        sequentialStream(0) { it + 1 }.skipUntil { it < 2 }.takeWhile { it < 2 }.expect()

        sequentialStream(1, 2).expect(mapOf(1 to "1", 2 to "2")) { toMap({ it }, { "$it" }) }
        sequentialStream(1, 1, 1).expect(setOf(1)) { toSet() }
        StringBuilder().let { builder ->
            sequentialStream(1, 2, 3).expect("123") { forEach { builder.append(it) }.let { builder.toString() } }
        }
        sequentialStream(1, 2, 3).expect(6) { sum() }
        sequentialStream<Int>().expect(null) { reduce { a, b -> a + b } }
        sequentialStream(2, 4, 1, 3).expect(1) { min() }
        sequentialStream(2, 4, 1, 3).expect(4) { max() }
        sequentialStream<Int>().expect(null) { max() }
        sequentialStream<Int>().expect(null) { min() }

        sequentialStream(1).expect(true) { any { it > 0 } }
        sequentialStream(1).expect(false) { any { it > 1 } }
        sequentialStream(1, 2).expect(true) { any { it > 1 } }
        sequentialStream<Int>().expect(false) { any { it > 1 } }
        sequentialStream(1).expect(true) { all { it > 0 } }
        sequentialStream(1).expect(false) { all { it > 1 } }
        sequentialStream(1, 2).expect(false) { all { it > 1 } }
        sequentialStream<Int>().expect(true) { all { it > 1 } }
        sequentialStream(2, 3, 4).expect(24) { reduce { a, b -> a * b } }
        sequentialStream<Int>().expect(null) { reduce { a, b -> a * b } }

        sequentialStream(1, 2).expect(1) { first() }
        sequentialStream(1, 2).expect(2) { last() }
        sequentialStream<Any>().expect(null) { first() }
        sequentialStream<Any>().expect(null) { last() }
    }

    @Test
    fun `compatibility of parallel stream`() {
        parallelStream(1, 2).map { "x$it" }.expect("x1", "x2")
        parallelStream(1, 2, 3).filter { it > 1 }.expect(2, 3)
        parallelStream(1.toInt(), 2.toLong(), 3.toInt()).filter<Long>().expect(2L)
        parallelStream(1, 2).flatMap { sequentialStream(it * 2, it * 3, it * 4) }.expect(2, 3, 4, 4, 6, 8)

        // cannot expect any particular execution order for parallel stream
        AtomicInteger().let { sum ->
            parallelStream(1, 2, 4)
                .peek { sum.addAndGet(it) }
                .expect(7) { toList().let { sum.get() } }
        }
        parallelStream(3, 2, 1).sort().expect(1, 2, 3)
        // distinct DOES guarantee preserved element order in parallel execution
        parallelStream(1, 2, 3, 1, 2, 4).distinct().expect(1, 2, 3, 4)

        parallelStream(1, 2, 3).limit(2).expect(1, 2)
        parallelStream(1, 2, 3).skip(1).expect(2, 3)
        parallelStream(0) { it + 1 }.limit(3).expect(0, 1, 2)
        parallelStream(0) { it + 1 }.skip(2).limit(3).expect(2, 3, 4)
        parallelStream(0) { it + 1 }.limit(3).skip(2).expect(2)
        parallelStream(1, 2, 3).takeWhile { it < 2 }.expect(1)
        parallelStream(1, 2, 3).skipUntil { it < 2 }.expect(2, 3)
        parallelStream(0) { it + 1 }.takeWhile { it < 2 }.expect(0, 1)
        parallelStream(0) { it + 1 }.skipUntil { it < 2 }.takeWhile { it < 2 }.expect()

        parallelStream(1, 2).expect(mapOf(1 to "1", 2 to "2")) { toMap({ it }, { "$it" }) }
        parallelStream(1, 1, 1).expect(setOf(1)) { toSet() }
        StringBuilder().let { builder ->
            // yeah, element order in forEach is preserved as well
            parallelStream(1, 2, 3).expect("123") { forEach { builder.append(it) }.let { builder.toString() } }
        }
        parallelStream(1, 2, 3).expect(6) { sum() }
        parallelStream<Int>().expect(null) { reduce { a, b -> a + b } }
        parallelStream(2, 4, 1, 3).expect(1) { min() }
        parallelStream(2, 4, 1, 3).expect(4) { max() }
        parallelStream<Int>().expect(null) { max() }
        parallelStream<Int>().expect(null) { min() }

        parallelStream(1).expect(true) { any { it > 0 } }
        parallelStream(1).expect(false) { any { it > 1 } }
        parallelStream(1, 2).expect(true) { any { it > 1 } }
        parallelStream<Int>().expect(false) { any { it > 1 } }
        parallelStream(1).expect(true) { all { it > 0 } }
        parallelStream(1).expect(false) { all { it > 1 } }
        parallelStream(1, 2).expect(false) { all { it > 1 } }
        parallelStream<Int>().expect(true) { all { it > 1 } }
        parallelStream(2, 3, 4).expect(24) { reduce { a, b -> a * b } }
        parallelStream<Int>().expect(null) { reduce { a, b -> a * b } }

        parallelStream(1, 2).expect(1) { first() }
        parallelStream(1, 2).expect(2) { last() }
        parallelStream<Any>().expect(null) { first() }
        parallelStream<Any>().expect(null) { last() }
    }

    @Test
    fun `laziness - generated stream isn't evaluated after limiting condition is met`() {
        for (inputStream in arrayOf(sequentialStream(1) { it + 1 }, parallelStream(1) { it + 1 })) {
            val counter = AtomicInteger()
            inputStream
                .peek { counter.incrementAndGet() }
                .limit(5)
                .expect(15) { sum() }
            assertEquals(5, counter.getAndSet(0))

            inputStream
                .peek { counter.incrementAndGet() }
                .limit(0)
                .expect(0) { sum() }
            assertEquals(0, counter.getAndSet(0))

            inputStream
                .peek { counter.incrementAndGet() }
                .limit(1)
                .expect(1) { sum() }
            assertEquals(1, counter.getAndSet(0))

            inputStream
                .limit(10)
                .peek { counter.incrementAndGet() }
                .limit(5)
                .expect(15) { sum() }
            assertEquals(5, counter.getAndSet(0))

            inputStream
                .skip(4)
                .peek { counter.incrementAndGet() }
                .limit(1)
                .expect(5) { sum() }
            assertEquals(1, counter.getAndSet(0))

            inputStream
                .skipUntil { it < 5 }
                .peek { counter.incrementAndGet() }
                .limit(1)
                .expect(5) { sum() }
            assertEquals(1, counter.getAndSet(0))

            inputStream
                .peek { counter.incrementAndGet() }
                .skipUntil { it < 5 }
                .limit(1)
                .expect(5) { sum() }
            assertEquals(5, counter.getAndSet(0))

            inputStream
                .takeWhile { it < 5 }
                .peek { counter.incrementAndGet() }
                .expect(10) { sum() }
            assertEquals(4, counter.getAndSet(0))

            inputStream
                .peek { counter.incrementAndGet() }
                .takeWhile { it < 5 }
                .expect(10) { sum() }
            assertEquals(5, counter.getAndSet(0))

            inputStream
                .skipUntil { it < 6 }
                .skip(4)
                .peek { counter.incrementAndGet() }
                .limit(1)
                .expect(10) { sum() }
            assertEquals(1, counter.getAndSet(0))

            inputStream
                .skipUntil { it < 6 }
                .peek { counter.incrementAndGet() }
                .skip(4)
                .limit(1)
                .expect(10) { sum() }
            assertEquals(5, counter.getAndSet(0))

            inputStream
                .limit(4)
                .peek { counter.incrementAndGet() }
                .takeWhile { it < 1 }
                .expect(0) { sum() }
            assertEquals(1, counter.getAndSet(0))

            inputStream
                .limit(4)
                .takeWhile { it < 1 }
                .peek { counter.incrementAndGet() }
                .expect(0) { sum() }
            assertEquals(0, counter.getAndSet(0))

            inputStream
                .limit(0)
                .peek { counter.incrementAndGet() }
                .expect(0) { sum() }
            assertEquals(0, counter.getAndSet(0))

            inputStream
                .peek { counter.incrementAndGet() }
                .limit(0)
                .expect(0) { sum() }
            assertEquals(0, counter.getAndSet(0))

            inputStream
                .takeWhile { false }
                .peek { counter.incrementAndGet() }
                .expect(0) { sum() }
            assertEquals(0, counter.getAndSet(0))

            inputStream
                .peek { counter.incrementAndGet() }
                .takeWhile { false }
                .expect(0) { sum() }
            assertEquals(1, counter.getAndSet(0))
        }
    }

    @Test
    fun `order - parallel stream doesn't mess forEach order`() {
        parallelStream(0..100000).expect(*Array(100001) { it })
    }

    @Test
    fun `parallelism - execution order may not coincide with sequential`() {
        val queue = ArrayDeque<Pair<CompletableFuture<Int>, CompletableFuture<Int>>>()
        var toWait: CompletableFuture<Int> = CompletableFuture.completedFuture(10)
        for (i in 0..9) {
            val toFinish = CompletableFuture<Int>()
            queue.push(toWait to toFinish)
            toWait = toFinish
        }
        val builder = StringBuilder()
        parallelStream(queue).map {
            val first = it.first.await()
            builder.append(first)
            val second = first - 1
            it.second.complete(second)
            return@map second.toString()
        }.expect("0123456789") { join(delimiter = "") }
        assertEquals("10987654321", builder.toString())
    }

    @Test
    fun `reuse - results won't be spoiled from executing the same stream again`() {
        val input = (0..99).toList()
        val output = input.sum()
        for (stream in arrayOf(sequentialStream(input), parallelStream(input))) {
            stream.flatMap {
                sequentialStream(it, it, it)
            }.expect(3 * output) { sum() }
            stream.flatMap {
                parallelStream(it, it, it)
            }.expect(3 * output) { sum() }
            stream.expect(output) { sum() }
            stream.expect(output) { sum() }
        }
    }

    @Test
    fun `extension - sum function`() {
        sequentialStream(0..9).expect(45) { sum() }
        parallelStream(0..9).expect(45) { sum() }
        sequentialStream(0L..9L).expect(45L) { sum() }
        parallelStream(0L..9L).expect(45L) { sum() }
        sequentialStream(0..9).map { it.toDouble() }.expect(45.0) { sum<Double>() }
        parallelStream(0..9).map { it.toDouble() }.expect(45.0) { sum<Double>() }
        sequentialStream(0..9).map { it.toFloat() }.expect(45.toFloat()) { sum() }
        parallelStream(0..9).map { it.toFloat() }.expect(45.toFloat()) { sum() }
        sequentialStream(0..9).map { it.toShort() }.expect(45.toShort()) { sum() }
        parallelStream(0..9).map { it.toShort() }.expect(45.toShort()) { sum() }
        sequentialStream(0..9).map { it.toByte() }.expect(45.toByte()) { sum() }
        parallelStream(0..9).map { it.toByte() }.expect(45.toByte()) { sum() }
        sequentialStream(0..9).map { it.toBigInteger() }.expect(45.toBigInteger()) { sum() }
        parallelStream(0..9).map { it.toBigInteger() }.expect(45.toBigInteger()) { sum() }
        sequentialStream(0..9).map { it.toBigDecimal() }.expect(45.toBigDecimal()) { sum() }
        parallelStream(0..9).map { it.toBigDecimal() }.expect(45.toBigDecimal()) { sum() }

        val x = object : Number() {
            override fun toByte() = 1.toByte()
            override fun toChar() = 1.toChar()
            override fun toDouble() = 1.toDouble()
            override fun toFloat() = 1.toFloat()
            override fun toInt() = 1
            override fun toLong() = 1.toLong()
            override fun toShort() = 1.toShort()
        }

        try {
            sequentialStream(x).expect(1) { sum() }
            fail("Should throw exception")
        } catch (ex: IllegalArgumentException) {
        }


        try {
            parallelStream(x).expect(1) { sum() }
            fail("Should throw exception")
        } catch (ex: IllegalArgumentException) {
        }
    }

    @Test
    fun `empty stream compatibility`() {
        emptyStream<Int>().map { "x$it" }.expect()
        emptyStream<Int>().filter { it > 1 }.expect()
        emptyStream<Int>().filter<Long>().expect()
        emptyStream<Int>().flatMap { sequentialStream(it * 2, it * 3, it * 4) }.expect()

        StringBuilder().let { builder ->
            emptyStream<Int>()
                .peek { builder.append(it) }
                .expect("") { toList().let { builder.toString() } }
        }
        emptyStream<Int>().sort().expect()
        emptyStream<Int>().distinct().expect()

        emptyStream<Int>().limit(2).expect()
        emptyStream<Int>().skip(1).expect()
        emptyStream<Int>().takeWhile { it < 2 }.expect()
        emptyStream<Int>().skipUntil { it < 2 }.expect()

        emptyStream<Int>().expect(mapOf()) { toMap({ it }, { "$it" }) }
        emptyStream<Int>().expect(setOf()) { toSet() }
        StringBuilder().let { builder ->
            emptyStream<Int>().expect("") { forEach { builder.append(it) }.let { builder.toString() } }
        }
        emptyStream<Int>().expect(0) { sum() }
        emptyStream<Int>().expect(null) { reduce { a, b -> a + b } }
        emptyStream<Int>().expect(null) { min() }
        emptyStream<Int>().expect(null) { max() }

        emptyStream<Int>().expect(false) { any { it > 0 } }
        emptyStream<Int>().expect(true) { all { it > 1 } }

        emptyStream<Int>().expect(null) { first() }
        emptyStream<Int>().expect(null) { last() }
        emptyStream<Int>().expect(0) {
            collect(object : Collector<Int, Int> {
                var counter = 0

                override suspend fun consume(element: Int) {
                    counter++
                }

                override suspend fun getResult() = counter
            })
        }
    }

    @Test
    fun `foolproof - unlimited parallel stream is not created when possible`() {
        val stream = parallelStream(1.0) { it + 1 }
        try {
            stream.expect(1) { sum() }
            fail("Should throw exception")
        } catch (ex: java.lang.IllegalArgumentException) {
        }
        stream.limit(1).expect(1.0) { sum() }
    }

    @Test
    fun `accessibility - factory methods are available`() {
        val array = arrayOf(1, 2, 3)

        sequentialStream(*array).expect(6) { sum() }
        sequentialStream(array.toList()).expect(6) { sum() }
        sequentialStream(array.asSequence()).expect(6) { sum() }
        sequentialStream(1) { it + 1 }.limit(3).expect(6) { sum() }

        parallelStream(*array).expect(6) { sum() }
        parallelStream(array.toList()).expect(6) { sum() }
        parallelStream(array.asSequence(), finite = true).expect(6) { sum() }
        parallelStream(1) { it + 1 }.limit(3).expect(6) { sum() }
    }
}