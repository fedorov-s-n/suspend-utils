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
 * Suspended stream is an interface that allows to do functional-like programming with Kotlin suspend functions.
 * Qualifiers: either sequential or parallel, lazy, ordered, re-entrable, possibly infinite, non-blocking.
 *
 * Suspended streams are lazy. That means that no function is going to be called on element you don't need. Laziness
 * also means that no action is going to be taken until terminate method is called. All terminate methods have `suspend`
 * modifier and all intermediate ones have not.
 *
 * Extremely large and even infinite streams are supported, including parallel mode. Parallel infinite streams elements
 * are handled in sequential manner until limiting condition (conditions from `limit()` or `takeWhile()`) is met or
 * until terminate method interrupts consuming (the following methods can do so: `any()`, `all()`, `first()`).
 * Parallel infinite streams elements are handled in parallel in all other cases. If unlimited infinite stream is
 * created it will throw exception upon call to terminate method that cannot interrupt consuming. That measure doesn't
 * prevent you completely from entering an infinite loop with no possibility to exit
 * (you may, for instance, call `takeWhile { true }`), but serves as fool proofing for cases one may detect without
 * dynamic code analysis.
 *
 * Elements of both parallel and sequential streams are ordered and the element order is preserved
 * when elements are passing the execution pipeline. However the execution order is not preserved in the parallel execution.
 * The execution order of a sequential stream is preserved as well as the element order of the same stream. That may have
 * some implications when any callback function is not purely functional.
 *
 * Suspended streams are re-entrable. A stream has no state, all parameters it keeps are final. A state only exists as set
 * of local variables created on a terminate method call. So it's valid to call a terminate method of the same stream
 * several times. However, idempotency is not guaranteed in any cases. Input collections/arrays/sequences
 * or their elements are neither copied nor cached. Results are not cached anywhere. If the stream is created
 * from a collection, changing the collection before call to a terminate method will change the stream as well.
 * More, if iterable or sequence used as input to stream only allows to be iterated once, stream isn't going to have
 * correct results on the second terminate method call.
 */
interface SuspendStream<T> {
    companion object {
        fun <T> emptyStream(): SuspendStream<T> = EmptyStream()

        fun <T> sequentialStream(iterable: Iterable<T>): SuspendStream<T> =
            SequentialStream(Origin.Source.Iterated({ iterable.iterator() }))

        fun <T> sequentialStream(vararg values: T): SuspendStream<T> =
            SequentialStream(Origin.Source.Iterated({ values.iterator() }))

        fun <T> sequentialStream(sequence: Sequence<T>): SuspendStream<T> =
            SequentialStream(Origin.Source.Iterated({ sequence.iterator() }))

        fun <T> sequentialStream(unit: T, generator: suspend (T) -> T): SuspendStream<T> =
            SequentialStream(Origin.Source.Generated(unit, generator))

        fun <T> parallelStream(iterable: Iterable<T>, coroutineContext: CoroutineContext = Dispatchers.Default): SuspendStream<T> =
            ParallelStream(Origin.Source.Iterated({ iterable.iterator() }, iterable.limited), coroutineContext)

        fun <T> parallelStream(vararg values: T, coroutineContext: CoroutineContext = Dispatchers.Default): SuspendStream<T> =
            ParallelStream(Origin.Source.Iterated({ values.iterator() }, true), coroutineContext)

        fun <T> parallelStream(sequence: Sequence<T>, coroutineContext: CoroutineContext = Dispatchers.Default, finite: Boolean = false): SuspendStream<T> =
            ParallelStream(Origin.Source.Iterated({ sequence.iterator() }, finite), coroutineContext)

        fun <T> parallelStream(unit: T, generator: suspend (T) -> T, coroutineContext: CoroutineContext = Dispatchers.Default): SuspendStream<T> =
            ParallelStream(Origin.Source.Generated(unit, generator), coroutineContext)

        fun <T> parallelStream(unit: T, coroutineContext: CoroutineContext = Dispatchers.Default, generator: suspend (T) -> T): SuspendStream<T> =
            ParallelStream(Origin.Source.Generated(unit, generator), coroutineContext)

        private val <T> Iterable<T>.limited
            get() = when (this) {
                is Collection<*> -> true
                is ClosedRange<*> -> true
                else -> false
            }
    }

    fun <K> map(mappingFunction: suspend (T) -> K): SuspendStream<K>
    fun <K> flatMap(mappingFunction: suspend (T) -> SuspendStream<K>): SuspendStream<K>
    fun filter(predicate: suspend (T) -> Boolean): SuspendStream<T>

    fun peek(consumer: suspend (T) -> Unit): SuspendStream<T>
    fun sort(comparator: Comparator<in T>): SuspendStream<T>
    fun distinct(): SuspendStream<T>

    fun limit(length: Int): SuspendStream<T>
    fun skip(length: Int): SuspendStream<T>
    fun takeWhile(predicate: suspend (T) -> Boolean): SuspendStream<T>
    fun skipUntil(predicate: suspend (T) -> Boolean): SuspendStream<T>

    suspend fun <C> collect(collector: Collector<T, C>): C
    suspend fun toList(): List<T>
    suspend fun toSet(): Set<T>
    suspend fun <K, V> toMap(keyFunction: suspend (T) -> K, valueFunction: suspend (T) -> V): Map<K, V>
    suspend fun forEach(consumer: suspend (T) -> Unit)

    suspend fun <U> reduce(unit: U, mergeFunction: suspend (U, T) -> U): U
    suspend fun reduce(mergeFunction: suspend (T, T) -> T): T?
    suspend fun max(comparator: suspend (T, T) -> Int): T?
    suspend fun min(comparator: suspend (T, T) -> Int): T?
    suspend fun any(predicate: suspend (T) -> Boolean): Boolean
    suspend fun all(predicate: suspend (T) -> Boolean): Boolean

    suspend fun first(): T?
    suspend fun last(): T?
}

inline fun <reified K : Any> SuspendStream<*>.filter(): SuspendStream<K> = filter { it is K }.map { it as K }
fun <T : Comparable<T>> SuspendStream<T>.sort() = sort(Comparator { a, b -> a.compareTo(b) })
suspend fun <T : Comparable<T>> SuspendStream<T>.max() = max { a, b -> a.compareTo(b) }
suspend fun <T : Comparable<T>> SuspendStream<T>.min() = min { a, b -> a.compareTo(b) }
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

    suspend fun <T> execute(consumer: suspend (CompletionStage<Unit>?, T) -> Unit, getElements: suspend () -> () -> Iterator<T>) {
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
        internal class Iterated<T>(private val iterable: () -> Iterator<T>, private val finite: Boolean = false) : Source<T>() {
            override suspend fun sequentialPoll(context: SuspendContext, consumer: suspend (T) -> Boolean) {
                val iterator = iterable()
                while (iterator.hasNext()) {
                    if (!consumer(iterator.next())) break
                }
            }

            override suspend fun parallelPoll(context: ParallelContext, consumer: suspend (CompletionStage<Unit>?, T) -> Unit) {
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

            override suspend fun parallelPoll(context: ParallelContext, consumer: suspend (CompletionStage<Unit>?, T) -> Unit) {
                return context.throwUnlimitedStreamException()
            }

            override fun isLimited(): Boolean = false
        }
    }

    internal sealed class Intermediate<S, T>(val source: Origin<S>) : Origin<T>() {
        internal class Sort<T>(private val comparator: Comparator<in T>, source: Origin<T>) : Intermediate<T, T>(source) {
            override fun isLimited(): Boolean = source.isLimited()

            override suspend fun sequentialPoll(context: SuspendContext, consumer: suspend (T) -> Boolean) {
                val collector = ArrayDeque<T>()
                context.poll(source) { collector.add(it) }
                val list = collector.sortedWith(comparator)
                for (element in list) {
                    if (!consumer(element)) break
                }
            }

            override suspend fun parallelPoll(context: ParallelContext, consumer: suspend (CompletionStage<Unit>?, T) -> Unit) {
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

                override suspend fun parallelPoll(context: ParallelContext, consumer: suspend (CompletionStage<Unit>?, T) -> Unit) {
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

                internal class SkipUntil<T>(val predicate: suspend (T) -> Boolean, source: Origin<T>) : FutureOrdered<T, AtomicBoolean>(source) {
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

                    override suspend fun parallelPoll(context: ParallelContext, consumer: suspend (CompletionStage<Unit>?, T) -> Unit) {
                        if (length > 0) {
                            context.execute { createFuture ->
                                val counter = AtomicInteger()
                                source.sequentialPoll(context) { element ->
                                    (counter.incrementAndGet() < length).apply { createFuture { consumer(it, element) } }
                                }
                            }
                        }
                    }
                }

                internal class TakeWhile<T>(val predicate: suspend (T) -> Boolean, source: Origin<T>) : ExecutionOrdered<T>(source) {
                    override suspend fun sequentialPoll(context: SuspendContext, consumer: suspend (T) -> Boolean) {
                        source.sequentialPoll(context) { element ->
                            predicate(element) && consumer(element)
                        }
                    }

                    override suspend fun parallelPoll(context: ParallelContext, consumer: suspend (CompletionStage<Unit>?, T) -> Unit) {
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

                override suspend fun parallelPoll(context: ParallelContext, consumer: suspend (CompletionStage<Unit>?, T) -> Unit) {
                    source.parallelPoll(context) { stage, element ->
                        consumer(stage, mapper(element))
                    }
                }
            }

            internal class Filter<T>(val predicate: suspend (T) -> Boolean, source: Origin<T>) : Unordered<T, T>(source) {
                override suspend fun sequentialPoll(context: SuspendContext, consumer: suspend (T) -> Boolean) {
                    source.sequentialPoll(context) { !predicate(it) || consumer(it) }
                }

                override suspend fun parallelPoll(context: ParallelContext, consumer: suspend (CompletionStage<Unit>?, T) -> Unit) {
                    source.parallelPoll(context) { stage, element ->
                        if (predicate(element)) {
                            consumer(stage, element)
                        }
                    }
                }
            }

            internal class FlatMap<S, T>(val mapper: suspend (S) -> SuspendStream<T>, source: Origin<S>) : Unordered<S, T>(source) {
                override suspend fun sequentialPoll(context: SuspendContext, consumer: suspend (T) -> Boolean) {
                    source.sequentialPoll(context) { mapper(it).all(consumer) }
                }

                override suspend fun parallelPoll(context: ParallelContext, consumer: suspend (CompletionStage<Unit>?, T) -> Unit) {
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

                override suspend fun parallelPoll(context: ParallelContext, consumer: suspend (CompletionStage<Unit>?, T) -> Unit) {
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

internal class ParallelStream<T>(origin: Origin<T>, private val coroutineContext: CoroutineContext) : AbstractStream<T>(origin) {
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
    override suspend fun <K, V> toMap(keyFunction: suspend (T) -> K, valueFunction: suspend (T) -> V): Map<K, V> = emptyMap()
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