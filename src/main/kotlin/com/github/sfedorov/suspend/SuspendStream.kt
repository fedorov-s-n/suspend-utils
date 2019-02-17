package com.github.sfedorov.suspend

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.future.await
import kotlinx.coroutines.future.future
import java.math.BigDecimal
import java.math.BigInteger
import java.util.*
import java.util.concurrent.CompletionStage
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.collections.ArrayList
import kotlin.collections.HashMap
import kotlin.collections.HashSet
import kotlin.coroutines.CoroutineContext

/**
 * Suspended stream is an interface that allows to program in functional-like style with Kotlin suspend functions.
 * Qualifiers: either sequential or parallel, lazy, ordered, reusable, possibly infinite, suspended.
 *
 * Suspended streams are lazy. No function is going to be called on elements that are filtered out or skipped.
 * No action is going to be taken until terminate method is called. All terminate methods have `suspend`
 * modifier and all intermediate ones have not.
 *
 * Extremely large and even infinite streams are supported, including parallel mode. Parallel infinite streams elements
 * are handled in sequential manner until limiting condition (conditions from `limit()` or `takeWhile()`) is met or
 * until terminate method interrupts consuming (the following methods can do that: `any()`, `all()`, `first()`).
 * Parallel infinite streams elements are handled in parallel in all other cases. If a terminate method without
 * short-circuit evaluation is called on an infinite parallel stream, exception is to be thrown. That measure doesn't
 * guarantee that parallel execution of infinite stream will not be started (it's possible, for instance, call
 * `takeWhile { true }` or `any { false }`), but serves as foolproof for cases when it's possible to detect an infinite
 * parallel stream without dynamic code analysis. Other cases are responsibility of developer using the class.
 *
 * Elements of both parallel and sequential streams are ordered: the element order is preserved when elements
 * are passed to terminal method callbacks. However, the execution order is not preserved in the parallel stream.
 * Thus, calls to intermediate methods for different elements may come in any order. Element and execution order in
 * sequential stream are always preserved: the next element handling starts only when the previous one has ended.
 *
 * Suspended streams are reusable. A stream has no state, all parameters it keeps are final. A state only exists as
 * a set of local variables created on a terminate method call. So it's valid to call a terminate method of the same
 * stream several times. However, idempotency is not guaranteed in any cases. Input collections/arrays/sequences
 * or their elements are neither copied nor cached. Results are not cached anywhere. If a stream is created
 * from a collection, changing the collection before call to a terminate method will change the stream as well.
 * More, if an iterable or a sequence used as input to stream only allows to be iterated once, stream will not have
 * correct results on the second terminate method call.
 */
interface SuspendStream<T> {
    companion object {
        /**
         * Creates an empty stream of given type
         */
        fun <T> emptyStream(): SuspendStream<T> = EmptyStream()

        /**
         * Creates sequential stream from [iterable]
         */
        fun <T> sequentialStream(iterable: Iterable<T>): SuspendStream<T> =
            SequentialStream(Origin.Source.Iterated({ iterable.iterator() }))

        /**
         * Creates sequential stream from [values]. To use array values as input, use `*array`.
         */
        fun <T> sequentialStream(vararg values: T): SuspendStream<T> =
            SequentialStream(Origin.Source.Iterated({ values.iterator() }))

        /**
         * Creates sequential stream from [sequence].
         */
        fun <T> sequentialStream(sequence: Sequence<T>): SuspendStream<T> =
            SequentialStream(Origin.Source.Iterated({ sequence.iterator() }))

        /**
         * Creates unlimited stream from initial value [unit] and [generator].
         */
        fun <T> sequentialStream(unit: T, generator: suspend (T) -> T): SuspendStream<T> =
            SequentialStream(Origin.Source.Generated(unit, generator))

        /**
         * Creates parallel stream from [iterable]. Stream is considered limited for collections and closed ranges
         * ans unlimited otherwise. Use argument [finite] to overwrite this setting. Unlimited stream is handled
         * as a sequential one until a limit is met and in parallel after that.
         */
        fun <T> parallelStream(iterable: Iterable<T>, coroutineContext: CoroutineContext = Dispatchers.Default,
            finite: Boolean = iterable.limited): SuspendStream<T> =
            ParallelStream(Origin.Source.Iterated({ iterable.iterator() }, finite), coroutineContext)

        /**
         * Creates a parallel stream from [values]. To use array values as input, use `*array`. The stream is considered
         * limited until argument [finite] is set to false. Unlimited stream is handled as a sequential one
         * until a limit is met and in parallel after that.
         */
        fun <T> parallelStream(vararg values: T, coroutineContext: CoroutineContext = Dispatchers.Default,
            finite: Boolean = true): SuspendStream<T> =
            ParallelStream(Origin.Source.Iterated({ values.iterator() }, finite), coroutineContext)

        /**
         * Creates a parallel stream from [sequence]. The stream is considered unlimited until argument [finite] is set
         * to true. Unlimited stream is handled as a sequential one until a limit is met and in parallel after that.
         */
        fun <T> parallelStream(sequence: Sequence<T>, coroutineContext: CoroutineContext = Dispatchers.Default,
            finite: Boolean = false): SuspendStream<T> =
            ParallelStream(Origin.Source.Iterated({ sequence.iterator() }, finite), coroutineContext)

        /**
         * Creates a parallel stream from initial value [unit] and [generator]. The stream should be limited with
         * either [limit] or [takeWhile] method. Another option is to terminate stream with short-circuit operation.
         * Unlimited stream is handled as a sequential one until the limit is met and in parallel after that.
         */
        fun <T> parallelStream(unit: T, generator: suspend (T) -> T,
            coroutineContext: CoroutineContext = Dispatchers.Default): SuspendStream<T> =
            ParallelStream(Origin.Source.Generated(unit, generator), coroutineContext)

        /**
         * Creates a parallel stream from initial value [unit] and [generator]. The stream should be limited with
         * either [limit] or [takeWhile] method. Another option is to terminate stream with short-circuit operation.
         * Unlimited stream is handled as a sequential one until the limit is met and in parallel after that.
         */
        fun <T> parallelStream(unit: T, coroutineContext: CoroutineContext = Dispatchers.Default,
            generator: suspend (T) -> T): SuspendStream<T> =
            ParallelStream(Origin.Source.Generated(unit, generator), coroutineContext)

        private val <T> Iterable<T>.limited
            get() = when (this) {
                is Collection<*> -> true
                is ClosedRange<*> -> true
                else -> false
            }
    }

    /**
     * Returns a stream consisting of the results of applying the given function to the elements of this stream.
     * This is an intermediate operation.
     */
    fun <K> map(mappingFunction: suspend (T) -> K): SuspendStream<K>

    /**
     * Returns a stream consisting of the results of replacing each element of this stream with the contents of
     * a mapped stream produced by applying the provided mapping function to each element.
     * Each mapped stream is closed after its contents have been placed into this stream.
     * This is an intermediate operation.
     */
    fun <K> flatMap(mappingFunction: suspend (T) -> SuspendStream<K>): SuspendStream<K>

    /**
     * Returns a stream consisting of the elements of this stream that match the given predicate.
     * This is an intermediate operation.
     */
    fun filter(predicate: suspend (T) -> Boolean): SuspendStream<T>

    /**
     * Returns a stream consisting of the elements of this stream, additionally performing the provided action
     * on each element as elements are consumed from the resulting stream.
     * This is an intermediate operation.
     */
    fun peek(consumer: suspend (T) -> Unit): SuspendStream<T>

    /**
     * Returns a str * eam consisting of the elements of this stream, sorted according to comparator.
     * This is a blocking intermediate operation. Parallel stream should be limited before calling this method.
     */
    fun sort(comparator: Comparator<in T>): SuspendStream<T>

    /**
     * Returns a stream consisting of the distinct elements (according to Object.equals(Object)) of this stream.
     * The element appearing first is preserved. Element order is preserved: results of executing this method on
     * parallel and sequential streams are the same. This is a non-blocking intermediate operation. However, the stream
     * has a state that depends on the stream size, so executing it on unlimited stream leads to unlimited memory usage.
     */
    fun distinct(): SuspendStream<T>

    /**
     * Returns a stream consisting of the elements of this stream, truncated to be no longer than maxSize in length.
     * This is a limiting intermediate operation.
     */
    fun limit(length: Int): SuspendStream<T>

    /**
     * Returns a stream consisting of the remaining elements of this stream after discarding the first n elements
     * of the stream. If this stream contains fewer than n elements then an empty stream will be returned.
     * This is an intermediate operation.
     */
    fun skip(length: Int): SuspendStream<T>

    /**
     * Returns a stream consisting of the elements of this stream, truncated to contain elements until the given
     * predicate returns true. This is a limiting intermediate operation.
     */
    fun takeWhile(predicate: suspend (T) -> Boolean): SuspendStream<T>

    /**
     * Returns a stream consisting of the remaining elements of this stream after the given predicate returns false
     * the first time. If the predicate always returns true then an empty stream will be returned.
     * This is an intermediate operation.
     */
    fun skipUntil(predicate: suspend (T) -> Boolean): SuspendStream<T>

    /**
     * Performs a mutable reduction operation on the elements of this stream using a Collector.
     * A Collector encapsulates the functions to consume an element and to return result.
     * Reduction performs sequentially even if the stream is parallel. Elements come in order, all required
     * synchronization is done in stream implementation, so Collector implementation shouldn't care about that.
     * This is a terminal operation.
     */
    suspend fun <C> collect(collector: Collector<T, C>): C

    /**
     * Returns a List containing all elements produced by this stream. Elements are ordered.
     * This is a terminal operation.
     */
    suspend fun toList(): List<T>

    /**
     * Returns a Set containing all elements produced by this stream. Elements are ordered.
     * This is a terminal operation.
     */
    suspend fun toSet(): Set<T>

    /**
     * Returns a Map whose keys and values are the result of applying the provided mapping functions to the input
     * elements. If the mapped keys contain duplicates (according to { @link Object#equals(Object) }), the last value
     * retains mapped by the key. This is a terminal operation.
     */
    suspend fun <K, V> toMap(keyFunction: suspend (T) -> K, valueFunction: suspend (T) -> V): Map<K, V>

    /**
     * Performs an action for each element of this stream. Elements are ordered. This is a terminal operation.
     */
    suspend fun forEach(consumer: suspend (T) -> Unit)

    /**
     * Performs a reduction on the elements of this stream, using the provided identity value and an accumulation
     * function, and returns the reduced value. Elements are ordered. This is a terminal operation.
     */
    suspend fun <U> reduce(unit: U, mergeFunction: suspend (U, T) -> U): U

    /**
     * Performs a reduction on the elements of this stream, using an associative accumulation function,
     * and returns the reduced value, if any, or null otherwise. Elements are ordered. This is a terminal operation.
     * Null can be returned also in case when stream type allows null elements and null is the result of reduction.
     */
    suspend fun reduce(mergeFunction: suspend (T, T) -> T): T?

    /**
     * Returns the maximum element of this stream according to the provided Comparator.
     * This is a special case of a reduction. This is a terminal operation.
     * Null is returned when the stream contains no elements or null is the maximum value according to the comparator.
     */
    suspend fun max(comparator: suspend (T, T) -> Int): T?

    /**
     * Returns the minimum element of this stream according to the provided Comparator.
     * This is a special case of a reduction. This is a terminal operation.
     * Null is returned when the stream contains no elements or null is the minimum value according to the comparator.
     */
    suspend fun min(comparator: suspend (T, T) -> Int): T?

    /**
     * Returns whether any elements of this stream match the provided predicate.
     * Should not evaluate the predicate on all elements if not necessary for determining the result.
     * If the stream is empty then false is returned and the predicate is not evaluated.
     * This is a short-circuiting terminal operation.
     */
    suspend fun any(predicate: suspend (T) -> Boolean): Boolean

    /**
     * Returns whether all elements of this stream match the provided predicate.
     * Should not evaluate the predicate on all elements if not necessary for determining the result.
     * If the stream is empty then true is returned and the predicate is not evaluated.
     * This is a short-circuiting terminal operation.
     */
    suspend fun all(predicate: suspend (T) -> Boolean): Boolean

    /**
     * Returns the first element of this stream, or null if the stream is empty. Elements are ordered.
     * This is a short-circuiting terminal operation.
     */
    suspend fun first(): T?

    /**
     * Returns the last element of this stream, or null if the stream is empty. Elements are ordered.
     * This is a short-circuiting terminal operation.
     */
    suspend fun last(): T?
}

/**
 * Returns a stream consisting of the elements of this stream that have the given type.
 * This is an intermediate operation. Result stream is of given type.
 */
inline fun <reified K : Any> SuspendStream<*>.filter(): SuspendStream<K> = filter { it is K }.map { it as K }

/**
 * Returns a stream consisting of the elements of this stream, sorted according to natural order.
 * This is a blocking intermediate operation. Parallel stream should be limited before calling this method.
 */
fun <T : Comparable<T>> SuspendStream<T>.sort() = sort(Comparator { a, b -> a.compareTo(b) })

/**
 * Returns the maximum element of this stream according to natural order.
 * This is a special case of a reduction. This is a terminal operation.
 * Null is returned when the stream contains no elements or null is the maximum value according to natural order.
 */
suspend fun <T : Comparable<T>> SuspendStream<T>.max() = max { a, b -> a.compareTo(b) }

/**
 * Returns the minimum element of this stream according to natural order.
 * This is a special case of a reduction. This is a terminal operation.
 * Null is returned when the stream contains no elements or null is the minimum value according to natural order.
 */
suspend fun <T : Comparable<T>> SuspendStream<T>.min() = min { a, b -> a.compareTo(b) }

/**
 * Returns the sum of elements in this stream. This is a special case of a reduction. This is a terminal operation.
 */
suspend inline fun <reified T : Number> SuspendStream<T>.sum(): T = when (T::class) {
    Int::class -> reduce(0) { a, b -> a + b.toInt() } as T
    Long::class -> reduce(0L) { a, b -> a + b.toLong() } as T
    Float::class -> reduce(0f) { a, b -> a + b.toFloat() } as T
    Double::class -> reduce(0.0) { a, b -> a + b.toDouble() } as T
    Short::class -> reduce(0.toShort()) { a, b -> (a + b.toShort()).toShort() } as T
    Byte::class -> reduce(0.toByte()) { a, b -> (a + b.toByte()).toByte() } as T
    BigInteger::class -> reduce(BigInteger.ZERO) { a, b -> a.add(b as BigInteger) } as T
    BigDecimal::class -> reduce(BigDecimal.ZERO) { a, b -> a.add(b as BigDecimal) } as T
    else -> throw IllegalArgumentException("Unsupported number implementation ${T::class}")
}

/**
 * Returns a String that is a concatenation of the input elements. Elements are ordered. This is a terminal operation.
 */
suspend fun SuspendStream<String>.join(delimiter: String = ", ", prefix: String = "", suffix: String = "") =
    collect(object : Collector<String, String> {
        val buffer = StringBuffer(prefix)
        var addDelimiter = false
        override suspend fun consume(element: String) {
            buffer.append(element)
            if (addDelimiter) {
                buffer.append(delimiter)
            } else {
                addDelimiter = true
            }
        }

        override suspend fun getResult(): String {
            return buffer.append(suffix).toString()
        }
    })

/**
 * A mutable reduction operation that accumulates input elements into a mutable result container, optionally
 * transforming the accumulated result into a final representation after all input elements have been processed.
 * Reduction operations can be performed either sequentially or in parallel, but collector may assume they are performed
 * sequentially, because all synchronization is done in stream implementation.
 */
interface Collector<T, R> {
    suspend fun consume(element: T)
    suspend fun getResult(): R
}

internal interface SuspendContext {
    suspend fun <T> poll(origin: Origin<T>, consumer: suspend (T) -> Unit)
}

internal object SequentialContext : SuspendContext {
    override suspend fun <T> poll(origin: Origin<T>, consumer: suspend (T) -> Unit) {
        origin.sequentialPoll(this) { consumer(it).run { true } }
    }
}

internal class ParallelContext(override val coroutineContext: CoroutineContext) : CoroutineScope, SuspendContext {
    override suspend fun <T> poll(origin: Origin<T>, consumer: suspend (T) -> Unit) {
        origin.parallelPoll(this) { stage, element ->
            stage?.await()
            consumer(element)
        }
    }

    fun throwUnlimitedStreamException() {
        throw IllegalArgumentException("Attempt to execute unlimited stream in parallel!")
    }

    suspend fun execute(execute: suspend ((suspend (CompletionStage<Unit>?) -> Unit) -> Unit) -> Unit) {
        withFutures {
            val futures = ArrayList<CompletionStage<Unit>>()
            var saved: CompletionStage<Unit>? = null

            val createFutureFunction: (suspend (CompletionStage<Unit>?) -> Unit) -> Unit = {
                val previous = saved
                val future = future { it(previous) }
                futures.add(future)
                saved = future
            }

            execute(createFutureFunction)
            return@withFutures futures
        }
    }

    suspend fun <T> execute(consumer: suspend (CompletionStage<Unit>?, T) -> Unit,
        getElements: suspend () -> () -> Iterator<T>) {
        withFutures {
            val iterator = getElements().invoke()
            val futures = ArrayList<CompletionStage<Unit>>()
            var saved: CompletionStage<Unit>? = null
            while (iterator.hasNext()) {
                val element = iterator.next()
                val previous = saved
                val future = future { consumer(previous, element) }
                futures.add(future)
                saved = future
            }
            return@withFutures futures
        }
    }

    suspend fun withFutures(futuresSupplier: suspend () -> Iterable<CompletionStage<Unit>>) {
        futuresSupplier().forEach { it.await() }
    }
}

internal sealed class Origin<T> {
    abstract suspend fun sequentialPoll(context: SuspendContext, consumer: suspend (T) -> Boolean)
    abstract suspend fun parallelPoll(context: ParallelContext, consumer: suspend (CompletionStage<Unit>?, T) -> Unit)
    abstract fun isLimited(): Boolean

    internal sealed class Source<T> : Origin<T>() {
        internal class Iterated<T>(private val iterable: () -> Iterator<T>,
            private val finite: Boolean = false) : Source<T>() {
            override suspend fun sequentialPoll(context: SuspendContext, consumer: suspend (T) -> Boolean) {
                val iterator = iterable()
                while (iterator.hasNext()) {
                    if (!consumer(iterator.next())) break
                }
            }

            override suspend fun parallelPoll(context: ParallelContext, consumer:
            suspend (CompletionStage<Unit>?, T) -> Unit) {
                return if (finite) {
                    context.execute(consumer) { iterable }
                } else {
                    context.throwUnlimitedStreamException()
                }
            }

            override fun isLimited(): Boolean = finite
        }

        internal class Generated<T>(private val unit: T, private val generator: suspend (T) -> T) : Source<T>() {
            override suspend fun sequentialPoll(context: SuspendContext, consumer: suspend (T) -> Boolean) {
                var shouldContinue: Boolean
                var element: T = unit
                var firstTime = true
                do {
                    if (firstTime) {
                        firstTime = false
                    } else {
                        element = generator(element)
                    }
                    shouldContinue = consumer(element)
                } while (shouldContinue)
            }

            override suspend fun parallelPoll(context: ParallelContext,
                consumer: suspend (CompletionStage<Unit>?, T) -> Unit) {

                return context.throwUnlimitedStreamException()
            }

            override fun isLimited(): Boolean = false
        }
    }

    internal sealed class Intermediate<S, T>(val source: Origin<S>) : Origin<T>() {
        internal class Sort<T>(private val comparator: Comparator<in T>, source: Origin<T>)
            : Intermediate<T, T>(source) {
            override fun isLimited(): Boolean = source.isLimited()

            override suspend fun sequentialPoll(context: SuspendContext, consumer: suspend (T) -> Boolean) {
                val collector = ArrayDeque<T>()
                context.poll(source) { collector.add(it) }
                val list = collector.sortedWith(comparator)
                for (element in list) {
                    if (!consumer(element)) break
                }
            }

            override suspend fun parallelPoll(context: ParallelContext,
                consumer: suspend (CompletionStage<Unit>?, T) -> Unit) {

                context.execute(consumer) {
                    val collector = ConcurrentLinkedQueue<T>()
                    source.parallelPoll(context) { _, element -> collector.add(element) }
                    val list = collector.sortedWith(comparator)
                    return@execute { list.iterator() }
                }
            }
        }

        internal sealed class Ordered<S, T>(source: Origin<S>) : Intermediate<S, T>(source) {
            internal sealed class FutureOrdered<T, C>(source: Origin<T>) : Intermediate.Ordered<T, T>(source) {
                abstract fun createCollector(): C
                abstract suspend fun filter(element: T, collector: C): Boolean
                override fun isLimited(): Boolean = source.isLimited()

                override suspend fun sequentialPoll(context: SuspendContext, consumer: suspend (T) -> Boolean) {
                    val collector = createCollector()
                    source.sequentialPoll(context) { element -> !filter(element, collector) || consumer(element) }
                }

                override suspend fun parallelPoll(context: ParallelContext,
                    consumer: suspend (CompletionStage<Unit>?, T) -> Unit) {

                    context.execute { createFuture ->
                        val collector = createCollector()
                        source.parallelPoll(context) { stage, element ->
                            stage?.await()
                            if (filter(element, collector)) {
                                createFuture { consumer(it, element) }
                            }
                        }
                    }
                }

                internal class Skip<T>(val length: Int, source: Origin<T>) : FutureOrdered<T, AtomicInteger>(source) {
                    override fun createCollector() = AtomicInteger()
                    override suspend fun filter(element: T, collector: AtomicInteger): Boolean {
                        return collector.incrementAndGet() > length
                    }
                }

                internal class SkipUntil<T>(val predicate: suspend (T) -> Boolean, source: Origin<T>)
                    : FutureOrdered<T, AtomicBoolean>(source) {

                    override fun createCollector() = AtomicBoolean(true)
                    override suspend fun filter(element: T, collector: AtomicBoolean): Boolean {
                        val value = collector.get() && predicate(element)
                        collector.set(value)
                        return !value
                    }
                }

                internal class Distinct<T>(source: Origin<T>) : FutureOrdered<T, MutableSet<T>>(source) {
                    override fun createCollector(): MutableSet<T> = HashSet()

                    override suspend fun filter(element: T, collector: MutableSet<T>): Boolean {
                        return collector.add(element)
                    }
                }
            }

            internal sealed class ExecutionOrdered<T>(source: Origin<T>) : Ordered<T, T>(source) {
                override fun isLimited(): Boolean {
                    var s: Origin<*> = source
                    while (s is Intermediate<*, *>) {
                        if (s is Sort) return s.isLimited() // because it's blocking
                        s = s.source
                    }
                    return true
                }

                internal class Limit<T>(val length: Int, source: Origin<T>) : ExecutionOrdered<T>(source) {
                    override suspend fun sequentialPoll(context: SuspendContext, consumer: suspend (T) -> Boolean) {
                        if (length > 0) {
                            val counter = AtomicInteger()
                            source.sequentialPoll(context) { element ->
                                val b1 = counter.incrementAndGet() < length
                                val b2 = consumer(element)
                                b1 && b2
                            }
                        }
                    }

                    override suspend fun parallelPoll(context: ParallelContext,
                        consumer: suspend (CompletionStage<Unit>?, T) -> Unit) {

                        if (length > 0) {
                            context.execute { createFuture ->
                                val counter = AtomicInteger()
                                source.sequentialPoll(context) { element ->
                                    (counter.incrementAndGet() < length).apply {
                                        createFuture { consumer(it, element) }
                                    }
                                }
                            }
                        }
                    }
                }

                internal class TakeWhile<T>(val predicate: suspend (T) -> Boolean, source: Origin<T>)
                    : ExecutionOrdered<T>(source) {

                    override suspend fun sequentialPoll(context: SuspendContext, consumer: suspend (T) -> Boolean) {
                        source.sequentialPoll(context) { element ->
                            predicate(element) && consumer(element)
                        }
                    }

                    override suspend fun parallelPoll(context: ParallelContext,
                        consumer: suspend (CompletionStage<Unit>?, T) -> Unit) {

                        context.execute { createFuture ->
                            source.sequentialPoll(context) { element ->
                                predicate(element).apply {
                                    if (this) createFuture { consumer(it, element) }
                                }
                            }
                        }
                    }
                }
            }
        }

        internal sealed class Unordered<S, T>(source: Origin<S>) : Intermediate<S, T>(source) {
            override fun isLimited(): Boolean = source.isLimited()

            internal class Mapping<S, T>(val mapper: suspend (S) -> T, source: Origin<S>) : Unordered<S, T>(source) {
                override suspend fun sequentialPoll(context: SuspendContext, consumer: suspend (T) -> Boolean) {
                    source.sequentialPoll(context) { consumer(mapper(it)) }
                }

                override suspend fun parallelPoll(context: ParallelContext,
                    consumer: suspend (CompletionStage<Unit>?, T) -> Unit) {

                    source.parallelPoll(context) { stage, element ->
                        consumer(stage, mapper(element))
                    }
                }
            }

            internal class Filter<T>(val predicate: suspend (T) -> Boolean, source: Origin<T>)
                : Unordered<T, T>(source) {

                override suspend fun sequentialPoll(context: SuspendContext, consumer: suspend (T) -> Boolean) {
                    source.sequentialPoll(context) { !predicate(it) || consumer(it) }
                }

                override suspend fun parallelPoll(context: ParallelContext,
                    consumer: suspend (CompletionStage<Unit>?, T) -> Unit) {

                    source.parallelPoll(context) { stage, element ->
                        if (predicate(element)) {
                            consumer(stage, element)
                        }
                    }
                }
            }

            internal class FlatMap<S, T>(val mapper: suspend (S) -> SuspendStream<T>, source: Origin<S>)
                : Unordered<S, T>(source) {

                override suspend fun sequentialPoll(context: SuspendContext, consumer: suspend (T) -> Boolean) {
                    source.sequentialPoll(context) { mapper(it).all(consumer) }
                }

                override suspend fun parallelPoll(context: ParallelContext,
                    consumer: suspend (CompletionStage<Unit>?, T) -> Unit) {

                    context.execute { createFuture ->
                        source.parallelPoll(context) { stage, element ->
                            stage?.await()
                            mapper(element).forEach { subElement -> createFuture { consumer(it, subElement) } }
                        }
                    }
                }
            }

            internal class Peek<T>(val peekConsumer: suspend (T) -> Unit, source: Origin<T>) : Unordered<T, T>(source) {
                override suspend fun sequentialPoll(context: SuspendContext, consumer: suspend (T) -> Boolean) {
                    source.sequentialPoll(context) { peekConsumer(it).run { consumer(it) } }
                }

                override suspend fun parallelPoll(context: ParallelContext,
                    consumer: suspend (CompletionStage<Unit>?, T) -> Unit) {

                    source.parallelPoll(context) { stage, element ->
                        peekConsumer(element)
                        consumer(stage, element)
                    }
                }
            }
        }
    }
}

internal abstract class AbstractStream<T>(protected val origin: Origin<T>) : SuspendStream<T> {
    override fun <K> map(mappingFunction: suspend (T) -> K): SuspendStream<K> =
        createStream(Origin.Intermediate.Unordered.Mapping(mappingFunction, origin))

    override fun <K> flatMap(mappingFunction: suspend (T) -> SuspendStream<K>) =
        createStream(Origin.Intermediate.Unordered.FlatMap(mappingFunction, origin))

    override fun filter(predicate: suspend (T) -> Boolean) =
        createStream(Origin.Intermediate.Unordered.Filter(predicate, origin))

    override fun peek(consumer: suspend (T) -> Unit) =
        createStream(Origin.Intermediate.Unordered.Peek(consumer, origin))

    override fun sort(comparator: Comparator<in T>) =
        createStream(Origin.Intermediate.Sort(comparator, origin))

    override fun limit(length: Int): SuspendStream<T> =
        createStream(Origin.Intermediate.Ordered.ExecutionOrdered.Limit(length, origin))

    override fun skip(length: Int): SuspendStream<T> =
        createStream(Origin.Intermediate.Ordered.FutureOrdered.Skip(length, origin))

    override fun takeWhile(predicate: suspend (T) -> Boolean) =
        createStream(Origin.Intermediate.Ordered.ExecutionOrdered.TakeWhile(predicate, origin))

    override fun skipUntil(predicate: suspend (T) -> Boolean) =
        createStream(Origin.Intermediate.Ordered.FutureOrdered.SkipUntil(predicate, origin))

    override fun distinct(): SuspendStream<T> =
        createStream(Origin.Intermediate.Ordered.FutureOrdered.Distinct(origin))

    override suspend fun <C> collect(collector: Collector<T, C>): C {
        collectInternal { collector.consume(it) }
        return collector.getResult()
    }

    override suspend fun toList(): List<T> {
        val list = ArrayList<T>()
        collectInternal { list.add(it) }
        return list
    }

    override suspend fun toSet(): Set<T> {
        val set = HashSet<T>()
        collectInternal { set.add(it) }
        return set
    }

    override suspend fun <K, V> toMap(keyFunction: suspend (T) -> K, valueFunction: suspend (T) -> V): Map<K, V> {
        val map = HashMap<K, V>()
        collectInternal { map[keyFunction(it)] = valueFunction(it) }
        return map
    }

    override suspend fun forEach(consumer: suspend (T) -> Unit) {
        collectInternal(consumer)
    }

    override suspend fun reduce(mergeFunction: suspend (T, T) -> T): T? {
        var value: T? = null
        var first = true
        collectInternal { element ->
            if (first) {
                value = element
                first = false
            } else {
                @Suppress("UNCHECKED_CAST")
                value = mergeFunction(value as T, element)
            }
        }
        return value
    }

    override suspend fun <U> reduce(unit: U, mergeFunction: suspend (U, T) -> U): U {
        var value: U = unit
        collectInternal { element ->
            value = mergeFunction(value, element)
        }
        return value
    }

    override suspend fun max(comparator: suspend (T, T) -> Int): T? {
        var value: T? = null
        var first = true
        collectInternal { element ->
            @Suppress("UNCHECKED_CAST")
            if (first) {
                value = element
                first = false
            } else if (comparator(value as T, element) < 0) {
                value = element
            }
        }
        return value
    }

    override suspend fun min(comparator: suspend (T, T) -> Int): T? {
        var value: T? = null
        var first = true
        collectInternal { element ->
            @Suppress("UNCHECKED_CAST")
            if (first) {
                value = element
                first = false
            } else if (comparator(value as T, element) > 0) {
                value = element
            }
        }
        return value
    }

    override suspend fun any(predicate: suspend (T) -> Boolean): Boolean {
        var value = false
        collectInternalLimited {
            !(value || predicate(it).apply {
                if (this) value = true
            })
        }
        return value
    }

    override suspend fun all(predicate: suspend (T) -> Boolean): Boolean {
        var value = true
        collectInternalLimited {
            value && predicate(it).apply {
                if (!this) value = false
            }
        }
        return value
    }

    override suspend fun first(): T? {
        var value: T? = null
        var first = true
        collectInternalLimited { element ->
            if (first) {
                value = element
                first = false
            }
            return@collectInternalLimited false
        }
        return value
    }

    override suspend fun last(): T? {
        var value: T? = null
        collectInternal { value = it }
        return value
    }

    private suspend fun collectInternal(internalCollector: suspend (T) -> Unit) {
        createContext().poll(origin, internalCollector)
    }

    private suspend fun collectInternalLimited(internalCollector: suspend (T) -> Boolean) {
        origin.sequentialPoll(SequentialContext) { element -> internalCollector(element) }
    }

    protected abstract fun <N> createStream(newOrigin: Origin<N>): SuspendStream<N>
    protected abstract fun createContext(): SuspendContext
}

internal class SequentialStream<T>(origin: Origin<T>) : AbstractStream<T>(origin) {
    override fun <N> createStream(newOrigin: Origin<N>): SuspendStream<N> = SequentialStream(newOrigin)
    override fun createContext() = SequentialContext
}

internal class ParallelStream<T>(origin: Origin<T>,
    private val coroutineContext: CoroutineContext) : AbstractStream<T>(origin) {

    override fun <N> createStream(newOrigin: Origin<N>) = ParallelStream(newOrigin, coroutineContext)

    override fun createContext() = ParallelContext(coroutineContext).apply {
        if (!origin.isLimited()) {
            throwUnlimitedStreamException()
        }
    }
}

internal class EmptyStream<T> : SuspendStream<T> {
    override fun <K> map(mappingFunction: suspend (T) -> K): SuspendStream<K> = EmptyStream()
    override fun <K> flatMap(mappingFunction: suspend (T) -> SuspendStream<K>): SuspendStream<K> = EmptyStream()
    override fun filter(predicate: suspend (T) -> Boolean): SuspendStream<T> = this
    override fun peek(consumer: suspend (T) -> Unit): SuspendStream<T> = this
    override fun sort(comparator: Comparator<in T>): SuspendStream<T> = this
    override fun distinct(): SuspendStream<T> = this
    override fun limit(length: Int): SuspendStream<T> = this
    override fun skip(length: Int): SuspendStream<T> = this
    override fun takeWhile(predicate: suspend (T) -> Boolean): SuspendStream<T> = this
    override fun skipUntil(predicate: suspend (T) -> Boolean): SuspendStream<T> = this
    override suspend fun <C> collect(collector: Collector<T, C>): C = collector.getResult()
    override suspend fun toList(): List<T> = emptyList()
    override suspend fun toSet(): Set<T> = emptySet()
    override suspend fun <K, V> toMap(keyFunction: suspend (T) -> K, valueFunction: suspend (T) -> V) = emptyMap<K, V>()
    override suspend fun forEach(consumer: suspend (T) -> Unit) = Unit
    override suspend fun <U> reduce(unit: U, mergeFunction: suspend (U, T) -> U): U = unit
    override suspend fun reduce(mergeFunction: suspend (T, T) -> T): T? = null
    override suspend fun max(comparator: suspend (T, T) -> Int): T? = null
    override suspend fun min(comparator: suspend (T, T) -> Int): T? = null
    override suspend fun any(predicate: suspend (T) -> Boolean): Boolean = false
    override suspend fun all(predicate: suspend (T) -> Boolean): Boolean = true
    override suspend fun first(): T? = null
    override suspend fun last(): T? = null
}