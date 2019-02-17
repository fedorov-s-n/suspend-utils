# `suspend` utils

Collection of utility classes to make work with Kotlin suspend functions even more easy.

  - [**SuspendStream**](#SuspendStream) [*(link to docs)*](https://fedorov-s-n.github.io/suspend-utils/suspend-utils/com.github.sfedorov.suspend/-suspend-stream/index.html)
 

Build instruction:

    mvn clean package
    
Artifact is available on maven central

Maven:

    <dependency>
        <groupId>com.github.fedorov-s-n</groupId>
        <artifactId>suspend-utils</artifactId>
        <version>1.0.0</version>
    </dependency>

Gradle:

    compile group: 'com.github.fedorov-s-n', name: 'suspend-utils', version: '1.0.0'

## SuspendStream

Conjunction of Java `Stream` and Kotlin `suspend`. Suspend streams are either sequential or parallel, lazy, ordered, 
reusable and possibly infinite.

Usage example:

    val jokes = parallelStream(*urlArray)
        .map { downloadAsync(it) }
        .flatMap { sequentialStream(parseAnecdoteList(it)) }
        .skipUntil { it is Old }
        .filter { askIfJokeIsFunny(it).await() }
        .limit(100500)
        .toList()