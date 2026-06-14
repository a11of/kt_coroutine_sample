package org.example

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.channels.ticker
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flatMap
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.flatMapLatest
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.newSingleThreadContext
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread
import kotlin.coroutines.cancellation.CancellationException
import kotlin.system.measureTimeMillis
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.measureTime

//TIP 要<b>运行</b>代码，请按 <shortcut actionId="Run"/> 或
// 点击装订区域中的 <icon src="AllIcons.Actions.Execute"/> 图标。
suspend fun main() {
    sample29()
}
@Volatile
var x = 0
val y = AtomicInteger(0)
val singleThreadContext = newSingleThreadContext("single")
val mutex = Mutex()
fun sample29() = runBlocking {
    val n = 100
    val m = 1000
    val timeMillis = measureTimeMillis {
        coroutineScope {
            repeat(n) {
                launch(Dispatchers.IO) {
                    repeat(m) {
                        y.incrementAndGet()
//                        mutex.withLock {
//                            y.incrementAndGet()
//                        }

                    }
                }
            }
        }
    }
    withContext(Dispatchers.IO) {
        repeat(n) {
            launch {
                repeat(n) {
                    mutex.withLock {
                        x++
                    }
                }
            }
        }

    }
    println("this is x value = ${x} cost time millis:$timeMillis")
    println("this is y value = ${y.get()} cost time millis:$timeMillis")

}
val handler = CoroutineExceptionHandler { _, exception ->
    println("CoroutineExceptionHandler got $exception")
}
fun sample28() = runBlocking {
    val job2 = CoroutineScope(Dispatchers.IO).launch(handler) {
        val job1 = launch {
            try {
                delay(Long.MAX_VALUE)
            } finally {
                withContext(NonCancellable) {
                    println("Children are cancelled, but exception is not handled until all children terminate")
                    delay(100)
                    println("The first child finished its non cancellable block")
                }
            }
        }
        val job = launch {
            delay(1000)
            println("Second child throws an exception")
            throw ArithmeticException()
        }
    }
    job2.join()

}

fun sample27() = runBlocking {
    val job = GlobalScope.launch(handler) { // root coroutine, running in GlobalScope
        throw AssertionError()

    }
    val async = GlobalScope.async(handler) {
        throw ArithmeticException()
    }
    joinAll(async, job)
    println("done")

}
fun sample26() = runBlocking {
    val ticker = ticker(delayMillis = 200, initialDelayMillis = 0)
    withTimeoutOrNull(1.seconds){
        val element = ticker.receive()
        println(element)
    }
    withTimeoutOrNull(100){
        val element = ticker.receive()
        println(element)
    }
}
fun sample25() = runBlocking {
    val channel = Channel<String>()
    launch {
        channel.consumeEach {
            println("first receiver get $it on thread ${Thread.currentThread().name}")
            delay(300)
            channel.send("ping")
        }
    }
    launch {
        channel.consumeEach {
            println("second receiver get $it on thread ${Thread.currentThread().name}")
            delay(300)
            channel.send("pong")
        }
    }
    delay(1000)
    channel.send("start...")
    delay(2000)
    coroutineContext.cancelChildren()

}
fun sample24() = runBlocking {
    val channel = Channel<Int>(5)
    launch {
        var x = 0
        while (true) {
            delay(1.seconds)
            println("channel send $x on thread ${Thread.currentThread().name}")
            channel.send(x++)
        }
    }
    delay(1.seconds)
    val receive = channel.receive()
    println("receive: $receive on thread ${Thread.currentThread().name}")
    delay(1.seconds)
    val receive1 = channel.receive()
    println("receive: $receive1 on thread ${Thread.currentThread().name}")
    delay(1.seconds)
    launch {
        channel.consumeEach {
            println("receive: $it on thread ${Thread.currentThread().name}")
        }
    }
//    channel.close()

}
fun sample23() =  runBlocking {
    var x = 0;
    val produce = produce {
        while (true) {
            delay(100)
            send(x++)
        }
    }
    repeat(5){index->
        launch {
            produce.consumeEach {
                println("process:$index receive:$it") }
        }
    }
    delay(1.seconds)
    produce.cancel()
}
suspend fun sample22() = runBlocking{
    val channel = Channel<Int>()
    launch(Dispatchers.Default) {
        for (i in 0..5) {
            delay(100)
            channel.send(i)
        }
        channel.close()
    }
    launch {
        for (i in channel) {
            println("$i")
        }
    }
    val produce = produce<Int> {
        for (i in channel) {
            send(i * i)
        }
    }
    delay(300)
    for (i in produce) {
        println("$i")
    }
    coroutineContext.cancelChildren()
    println("Done!")



}
suspend fun sample21() = runBlocking<Unit> {
    val flow = flow {
        for (i in 0..5) {
            delay(100)
            emit(i)
        }
    }
    flow.flatMapConcat {
        flow {
            emit("flat start on thread ${Thread.currentThread().name}")
            delay(300)
            emit("$it on thread ${Thread.currentThread().name}")
        }
    }.collect { println(it) }
}
suspend fun sample20()  = runBlocking(Dispatchers.Default) {
    coroutineScope {
        val job = launch {
            val flow = flow {
                for (i in 1..5) {
                    delay(100)
                    emit(i)
                }
            }
            flow.collect { value ->
                println(value)
            }
        }
        delay(300)
        job.cancel()

    }

}
suspend fun sample19() = runBlocking {
//    val flow = flow {
//        for (i in 1..5) {
//            delay(100)
//            emit(i)
//        }
//    }
    (1..3).asFlow().collect {
        if (it == 3) {
            cancel()
        }
        println(it)
    }
//    flow.collect {
//        if (it == 3) {
//            cancel()
//        }
//        println("$it on thread ${Thread.currentThread().name}")
//    }
}
suspend fun sample18() = runBlocking<Unit> {
    val flow = flow {
        for (i in 0..3) {
            delay(100)
            emit(i)
        }
    }
    coroutineScope {
        launch(Dispatchers.IO) {
            val job = flow.onEach {
                println("current value:$it on thread:${Thread.currentThread().name}")
            }.launchIn(this)
            delay(300)
            job.cancel()
        }
        println("coroutineScope Done! thread:${Thread.currentThread().name}")
    }
    println("done! on thread:${Thread.currentThread().name}")
}
suspend fun sample17() = runBlocking {
    simple().collect { value -> println(value) }
}
fun simple(): Flow<Int> = flow {
    // The WRONG way to change context for CPU-consuming code in flow builder
    withContext(Dispatchers.Default) {
        for (i in 1..3) {
            Thread.sleep(100)// pretend we are computing it in CPU-consuming way
            emit(i) // emit next value
        }
    }
}
suspend fun sample16() {
    val flow = flow {
        for (i in 1..1000) {
            delay(100)
            emit(i)
        }
    }
    withContext(Dispatchers.Default) {
        withTimeoutOrNull(500.milliseconds) {
            flow.collect { value -> println("$value thread ${Thread.currentThread().name}") }
        }

        println("there is a string on the thread ${Thread.currentThread().name}")
    }
    println("Done!")
}
suspend fun sample15() {
    coroutineScope {
        launch {
            launch {
                for (i in 1..10) {
                    delay((i + 1) * 200L)
                    println("$i from first coroutine")
                }
            }
            launch {
                for (i in 10 downTo 1) {
                    delay((i + 1) * 200L)
                    println("$i from second coroutine")
                }
            }
            println("request: I'm done and I don't explicitly join my children that are still active")
        }
    }
    println("outer always wait coroutine scope")
}
suspend fun sample14(){
    coroutineScope {
        val job = launch {
            launch(Job()) {
                println("coroutine 1 run on own context")
                delay(2000)
                println("coroutine 1 dont effect by his father")
            }
            launch {
                println("coroutine 2 run on his father context")
                delay(2000)
                println("coroutine 2 if his father context cancel request than will not running")
            }
        }
        delay(1000)
        job.cancel()
        println("father context cancel context")
        delay(1000)
    }
    println("outer thread:${Thread.currentThread().name}")
}
suspend fun sample13() = runBlocking {
    val measureTimeMillis = measureTimeMillis {
        GlobalScope.async(start = CoroutineStart.LAZY) {}
        val twentyfour = async(start = CoroutineStart.LAZY) {
            delay(1000)
            24
        }
        val two = async(start = CoroutineStart.LAZY) {
            delay(1000)
            2
        }
        twentyfour.start()
        two.start()
        println("${twentyfour.await()} + ${two.await()}")
    }
    println("Completed in $measureTimeMillis ms")
}
suspend fun sample12() {
    withContext(Dispatchers.IO) {
        launch {
            sample1()
        }
        launch {
            sample10()
        }
        println("end of sample12 on thread ${Thread.currentThread().name}")
    }
    println("complete sample12 on thread ${Thread.currentThread().name}")
}
suspend fun sample11() {
    withContext(Dispatchers.Default) {
        // Used as a signal that the coroutine has started running
        val job1Started = CompletableDeferred<Unit>()

        val job1: Job = launch {

            println("The coroutine has started")

            // Completes the CompletableDeferred,
            // signaling that the coroutine has started running
            job1Started.complete(Unit)
            try {
                // Suspends indefinitely
                // Without cancellation, this call would never return
                delay(Duration.INFINITE)
            } catch (e: CancellationException) {
                println("The coroutine was canceled: $e")

                // Always rethrow cancellation exceptions!
                throw e
            }
            println("This line will never be executed")
        }

        // Waits for job1 to start before canceling it
        job1Started.await()

        // Cancels the coroutine, so delay() throws a CancellationException
        job1.cancel()

        // async returns a Deferred handle, which inherits from Job
        val job2 = async {
            // If the coroutine is canceled before its body starts executing,
            // this line may not be printed
            println("The second coroutine has started")

            try {
                // Equivalent to delay(Duration.INFINITE)
                // Suspends until this coroutine is canceled
                awaitCancellation()

            } catch (e: CancellationException) {
                println("The second coroutine was canceled")
                throw e
            }
        }
        job2.cancel()
    }
    // Coroutine builders such as withContext() or coroutineScope()
    // wait for all child coroutines to complete,
    // even when the children are canceled
    println("All coroutines have completed")
}
suspend fun sample10() {
    withContext(Dispatchers.IO) {
         val deferred = async {
             var sum = 0;
             for (i in 1..10) {
                 update(i)
                 sum += i
             }
             sum
         }
        println("await...")
        val await = deferred.await()
        println("Done! $await")
    }
    println("Done! ${Thread.currentThread().name}")
}
suspend fun update(i: Int) = withContext(Dispatchers.Default) {
    println("value:$i,thread:${Thread.currentThread().name}")
}


suspend fun sample9() {
    withContext(Dispatchers.IO) {
        launch {
            println("starting coroutineScope ${Thread.currentThread().name}")
            delay(2000)
        }
        withContext(Dispatchers.Default) {
            println("ending coroutineScope ${Thread.currentThread().name}")
        }
        println("coroutineScope ${Thread.currentThread().name}")
    }

    coroutineScope {
        launch {
            println("starting coroutineScope two ${Thread.currentThread().name}")
        }
    }
    CoroutineScope(Dispatchers.Default).launch {
        println("starting coroutineScope three ${Thread.currentThread().name}")
    }
}
suspend fun sample8() {
    val job = CoroutineScope(Dispatchers.Default).launch {
        println("starting sample8 run on ${Thread.currentThread().name}")
        launch {
            println("first page run on ${Thread.currentThread().name}")
        }
        launch(Dispatchers.Default) {
            println("second page run on ${Thread.currentThread().name}")
        }
        println("ending sample8 run on ${Thread.currentThread().name}")
    }
    coroutineScope {
        println("hello world run on ${Thread.currentThread().name}")
    }
    println("main run on ${Thread.currentThread().name}")
}
suspend fun sample7() = withContext(Dispatchers.Default) { // this: CoroutineScope
    println("Running withContext block on ${Thread.currentThread().name}")

    val one = this.async {
        println("First calculation starting on ${Thread.currentThread().name}")
        val sum = (1L..500_000L).sum()
        delay(200L)
        println("First calculation done on ${Thread.currentThread().name}")
        sum
    }

    val two = this.async {
        println("Second calculation starting on ${Thread.currentThread().name}")
        val sum = (500_001L..1_000_000L).sum()
        println("Second calculation done on ${Thread.currentThread().name}")
        sum
    }

    // Waits for both calculations and prints the result
    println("Combined total: ${one.await() + two.await()}")
}
suspend fun sample6() = coroutineScope {
    this.launch(Dispatchers.Default) {
        println("first page run on ${Thread.currentThread().name}")
    }
    this.launch {
        println("second page run on ${Thread.currentThread().name}")
    }
    println("main run on ${Thread.currentThread().name}")
}
fun sample5() = runBlocking {
    this.launch {
        withContext(Dispatchers.Default) {
            delay(1.seconds)
            println("First page run with: ${Thread.currentThread().name}")
        }
    }
    this.launch {
        println("Second page run with: ${Thread.currentThread().name}")
    }
    println("main page run with ${Thread.currentThread().name}")
}
suspend fun sample4() = coroutineScope {
    val firstPage = this.launch {
        delay(1.seconds)
        println("First page run with: ${Thread.currentThread().name}")
        "First page"
    }
    val secondPage = this.launch {
        delay(1.seconds)
        println("Second page run with: ${Thread.currentThread().name}")
        "Second page"
    }
    println("main page run with ${Thread.currentThread().name}")
}
suspend fun sample3() = withContext(Dispatchers.Default) {
    val firstPage = this.async {
        delay(1.seconds)
        println("First page")
        "First page"
    }
    val secondPage = this.async {
        delay(1.seconds)
        println("Second page")
        "Second page"
    }
    println("pages prepare...")
    val pagesAreEqual = firstPage.await() == secondPage.await()
    println("Pages already: $pagesAreEqual")
}
suspend fun sample2() {
    coroutineScope {
        this.launch {
            this.launch {
                delay(2.seconds)
                println("Child of the enclosing coroutine completed")
            }
            println("Child coroutine 1 completed")
        }
        this.launch {
            delay(1.seconds)
            println("Child coroutine 2 completed")
        }
        println("Coroutine scope completed")
    }
    println("the main outer")
}

suspend fun sample1() {
    withContext(Dispatchers.Default) {
        this.launch {
            delay(2000)
            println("The greet() on the thread: ${Thread.currentThread().name}")
        }
        this.launch {
            delay(1000)
            println("The CoroutineScope.launch() on the thread: ${Thread.currentThread().name}")
        }
        println("The withContext() on the thread: ${Thread.currentThread().name}")
    }
}

