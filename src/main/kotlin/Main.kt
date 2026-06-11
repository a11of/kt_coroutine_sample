package org.example

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlin.concurrent.thread
import kotlin.coroutines.cancellation.CancellationException
import kotlin.system.measureTimeMillis
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.measureTime

//TIP 要<b>运行</b>代码，请按 <shortcut actionId="Run"/> 或
// 点击装订区域中的 <icon src="AllIcons.Actions.Execute"/> 图标。
suspend fun main() {
    sample15()
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

