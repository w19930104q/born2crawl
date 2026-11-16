/*
 * Copyright 2022 Arthur Ivanets, arthur.ivanets.work@gmail.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.arthurivanets.born2crawl

import com.arthurivanets.born2crawl.CrawlingSession.Event
import com.arthurivanets.born2crawl.InputProcessor.Output.Companion.crawlableDataOf
import com.arthurivanets.born2crawl.InputProcessor.ValueHolder.Companion.asCrawlable
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Assertions.assertEquals
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

@OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)
internal class CrawlingSessionConcurrencyTests {

    private val testDispatcher = Dispatchers.Default.limitedParallelism(8)

    // A dedicated single-thread dispatcher for the test-side polling, so that a gate-blocked
    // processing coroutine can never starve the polling coroutines of threads.
    private val testControlDispatcher = Dispatchers.IO.limitedParallelism(1)

    private fun awaitSessionCompletion(listener: TestCrawlingSessionEventListener, timeoutMs: Long = 30_000L) =
        kotlinx.coroutines.runBlocking(testControlDispatcher) {
            withTimeout(timeoutMs) {
                while (listener.collectedEvents.none { event ->
                        (event is Event.CrawlingFinished) || (event is Event.CrawlingFailed)
                    }
                ) {
                    kotlinx.coroutines.delay(10)
                }
            }
        }

    private fun awaitGate(gate: ProcessingGate, timeoutMs: Long = 10_000L) =
        kotlinx.coroutines.runBlocking(testControlDispatcher) {
            withTimeout(timeoutMs) {
                while (gate.awaitingCount == 0) {
                    kotlinx.coroutines.delay(10)
                }
            }
        }

    @Test
    fun `cyclic graph - each processor input pair is processed at most once despite the cycles`() {
        // a -> b, b -> a (a cycle); plus a -> c -> a.
        val inputA = "input-a"
        val inputB = "input-b"
        val inputC = "input-c"

        val processingCounter = AtomicInteger(0)
        val processor = CountingInputProcessor(
            tagSuffix = "cyclic",
            outputProvider = { input ->
                processingCounter.incrementAndGet()
                when (input) {
                    inputA -> listOf(
                        crawlableDataOf("url" to inputB),
                        crawlableDataOf("url" to inputC),
                    )

                    inputB -> listOf(crawlableDataOf("url" to inputA))
                    inputC -> listOf(crawlableDataOf("url" to inputA))
                    else -> emptyList()
                }
            },
            maxObservedConcurrency = AtomicInteger(0),
        )

        val resultStore = InMemoryCrawlingResultStore()
        val listener = TestCrawlingSessionEventListener()

        val session = CrawlingSessionImpl(
            CrawlingSession.Config(
                initialInputs = setOf(inputA),
                inputProcessingBatchSize = 10,
                inputProcessors = setOf(processor),
                coroutineDispatcher = UnconfinedTestDispatcher(),
                resultStore = resultStore,
            )
        )
        session.eventListener = listener
        session.init()

        awaitSessionCompletion(listener)

        val finishedEvent = listener.collectedEvents.filterIsInstance<Event.CrawlingFinished>().single()
        val result = resultStore.getById(finishedEvent.resultId)!!

        assertEquals(3, processor.totalProcessingCount)
        assertEquals(1, processor.processingCountOf(inputA))
        assertEquals(1, processor.processingCountOf(inputB))
        assertEquals(1, processor.processingCountOf(inputC))
        // One output per distinct processed input.
        assertEquals(3, result.context.getAll().size)
    }

    @Test
    fun `concurrent duplicate discovery - a processor input pair gets processed only once and never concurrently`() {
        // Two distinct seed processors produce the SAME shared url, which a single shared processor handles.
        val seed = "seed-input"
        val sharedUrl = "https://www.example.com/shared"

        val sharedGate = ProcessingGate()
        val sharedConcurrency = AtomicInteger(0)
        val sharedProcessor = CountingInputProcessor(
            tagSuffix = "shared",
            inputProbingRegex = Regex(Regex.escape(sharedUrl)),
            outputProvider = { listOf(crawlableDataOf("leaf" to "leaf-value")) },
            gate = sharedGate,
            maxObservedConcurrency = sharedConcurrency,
        )

        val seedProcessorA = FakeInputProcessor(
            tagSuffix = "seed-a",
            inputProbingRegex = Regex(Regex.escape(seed)),
            output = listOf(mapOf("url" to sharedUrl.asCrawlable())),
        )
        val seedProcessorB = FakeInputProcessor(
            tagSuffix = "seed-b",
            inputProbingRegex = Regex(Regex.escape(seed)),
            output = listOf(mapOf("url" to sharedUrl.asCrawlable())),
        )

        val resultStore = InMemoryCrawlingResultStore()
        val listener = TestCrawlingSessionEventListener()

        val session = CrawlingSessionImpl(
            CrawlingSession.Config(
                initialInputs = setOf(seed),
                inputProcessingBatchSize = 10,
                inputProcessors = setOf(seedProcessorA, seedProcessorB, sharedProcessor),
                coroutineDispatcher = testDispatcher,
                resultStore = resultStore,
            )
        )
        session.eventListener = listener

        session.init()

        // Wait until the first (and only) processing of the shared url reaches the blocking gate.
        awaitGate(sharedGate)

        // Give the second branch enough time to reach the claim manager and (correctly) start waiting.
        runBlocking { kotlinx.coroutines.delay(500) }

        assertEquals(1, sharedProcessor.totalProcessingCount)
        assertEquals(1, sharedConcurrency.get())

        sharedGate.release()
        awaitSessionCompletion(listener)

        assertEquals(1, sharedProcessor.totalProcessingCount)
        assertEquals(1, sharedProcessor.processingCountOf(sharedUrl))
        assertEquals(1, sharedConcurrency.get())

        val finishedEvent = listener.collectedEvents.filterIsInstance<Event.CrawlingFinished>().single()
        val result = resultStore.getById(finishedEvent.resultId)!!
        // The shared output must appear exactly once in the final context.
        assertEquals(1, result.context.getValues("leaf").size)
    }

    @Test
    fun `processor exception - the processing does not crash the session and terminal failures are not retried by default`() {
        val failingInput = "failing-input"
        val healthyInput = "healthy-input"

        val failingProcessor = CountingInputProcessor(
            tagSuffix = "failing",
            inputProbingRegex = Regex(Regex.escape(failingInput)),
            throwException = true,
        )
        val healthyProcessor = CountingInputProcessor(
            tagSuffix = "healthy",
            inputProbingRegex = Regex(Regex.escape(healthyInput)),
            outputProvider = { listOf(crawlableDataOf("result" to "ok")) },
        )

        val resultStore = InMemoryCrawlingResultStore()
        val listener = TestCrawlingSessionEventListener()

        val session = CrawlingSessionImpl(
            CrawlingSession.Config(
                initialInputs = setOf(failingInput, healthyInput),
                inputProcessingBatchSize = 10,
                inputProcessors = setOf(failingProcessor, healthyProcessor),
                coroutineDispatcher = testDispatcher,
                resultStore = resultStore,
            )
        )
        session.eventListener = listener
        session.init()

        awaitSessionCompletion(listener)

        assertNotNull(listener.collectedEvents.filterIsInstance<Event.CrawlingFinished>().singleOrNull())
        // The failing processor was attempted exactly once (no retries by default) and didn't poison the session.
        assertEquals(1, failingProcessor.totalProcessingCount)
        assertEquals(1, healthyProcessor.totalProcessingCount)

        val finishedEvent = listener.collectedEvents.filterIsInstance<Event.CrawlingFinished>().single()
        val result = resultStore.getById(finishedEvent.resultId)!!
        assertEquals("ok", result.context.getValues("result").single())
    }

    @Test
    fun `processor failure is retried the configured number of times and succeeds on a later attempt`() {
        val flakyInput = "flaky-input"
        val maxRetries = 2

        val processor = CountingInputProcessor(
            tagSuffix = "flaky",
            inputProbingRegex = Regex(Regex.escape(flakyInput)),
            failOnce = true,
            outputProvider = { listOf(crawlableDataOf("result" to "recovered")) },
        )

        val resultStore = InMemoryCrawlingResultStore()
        val listener = TestCrawlingSessionEventListener()

        val session = CrawlingSessionImpl(
            CrawlingSession.Config(
                initialInputs = setOf(flakyInput),
                inputProcessingBatchSize = 10,
                inputProcessors = setOf(processor),
                coroutineDispatcher = testDispatcher,
                resultStore = resultStore,
                maxInputProcessingRetries = maxRetries,
            )
        )
        session.eventListener = listener
        session.init()

        awaitSessionCompletion(listener)

        // First attempt failed, second one succeeded.
        assertEquals(2, processor.totalProcessingCount)

        val finishedEvent = listener.collectedEvents.filterIsInstance<Event.CrawlingFinished>().single()
        val result = resultStore.getById(finishedEvent.resultId)!!
        assertEquals("recovered", result.context.getValues("result").single())
    }

    @Test
    fun `processor failure exhausts the configured retries and then gets skipped`() {
        val brokenInput = "broken-input"
        val maxRetries = 2

        val processor = CountingInputProcessor(
            tagSuffix = "broken",
            inputProbingRegex = Regex(Regex.escape(brokenInput)),
            failAlways = true,
        )

        val resultStore = InMemoryCrawlingResultStore()
        val listener = TestCrawlingSessionEventListener()

        val session = CrawlingSessionImpl(
            CrawlingSession.Config(
                initialInputs = setOf(brokenInput),
                inputProcessingBatchSize = 10,
                inputProcessors = setOf(processor),
                coroutineDispatcher = testDispatcher,
                resultStore = resultStore,
                maxInputProcessingRetries = maxRetries,
            )
        )
        session.eventListener = listener
        session.init()

        awaitSessionCompletion(listener)

        // The initial attempt + the configured number of retries.
        assertEquals(maxRetries + 1, processor.totalProcessingCount)
        assertNotNull(listener.collectedEvents.filterIsInstance<Event.CrawlingFinished>().singleOrNull())
    }

    @Test
    fun `cancelled processing releases the claim and a re-run processes the input again`() {
        val cancellableInput = "cancellable-input"
        val gate = ProcessingGate()

        val processor = CountingInputProcessor(
            tagSuffix = "cancellable",
            inputProbingRegex = Regex(Regex.escape(cancellableInput)),
            outputProvider = { listOf(crawlableDataOf("result" to "done")) },
            gate = gate,
        )

        // ---- First session: cancel it while the processing is blocked on the gate. ----
        val firstStore = InMemoryCrawlingResultStore()
        val firstListener = TestCrawlingSessionEventListener()

        val firstSession = CrawlingSessionImpl(
            CrawlingSession.Config(
                initialInputs = setOf(cancellableInput),
                inputProcessingBatchSize = 10,
                inputProcessors = setOf(processor),
                coroutineDispatcher = testDispatcher,
                resultStore = firstStore,
            )
        )
        firstSession.eventListener = firstListener
        firstSession.init()

        awaitGate(gate)

        firstSession.destroy()
        gate.release()

        runBlocking { kotlinx.coroutines.delay(500) }

        // The cancelled session must not have produced a result.
        assertTrue(firstStore.getAll().isEmpty())
        assertEquals(1, processor.totalProcessingCount)

        // ---- Second session (re-run): the same input must be processed from scratch. ----
        val secondGate = ProcessingGate()
        val reRunProcessor = CountingInputProcessor(
            tagSuffix = "cancellable-rerun",
            inputProbingRegex = Regex(Regex.escape(cancellableInput)),
            outputProvider = { listOf(crawlableDataOf("result" to "done")) },
            gate = secondGate,
        )
        val secondStore = InMemoryCrawlingResultStore()
        val secondListener = TestCrawlingSessionEventListener()

        val secondSession = CrawlingSessionImpl(
            CrawlingSession.Config(
                initialInputs = setOf(cancellableInput),
                inputProcessingBatchSize = 10,
                inputProcessors = setOf(reRunProcessor),
                coroutineDispatcher = testDispatcher,
                resultStore = secondStore,
            )
        )
        secondSession.eventListener = secondListener
        secondSession.init()

        // Wait until the re-run actually reaches the processor, then let it finish.
        awaitGate(secondGate)
        secondGate.release()
        awaitSessionCompletion(secondListener)

        assertEquals(1, reRunProcessor.totalProcessingCount)
        val finishedEvent = secondListener.collectedEvents.filterIsInstance<Event.CrawlingFinished>().single()
        val result = secondStore.getById(finishedEvent.resultId)!!
        assertEquals("done", result.context.getValues("result").single())
    }

    @Test
    fun `max crawl depth semantics remain unchanged with the claim-based deduplication`() {
        // depth 0: seed -> a ; depth 1: a -> b ; depth 2: b -> c
        // with maxCrawlDepth = 3, seed/a/b are processed (outputs at depths 1, 2, 3),
        // but the output at depth 3 (c) never enters the frontier, so c is never processed.
        val seed = "seed"
        val inputA = "a"
        val inputB = "b"
        val inputC = "c"

        val processor = CountingInputProcessor(
            tagSuffix = "depth",
            outputProvider = { input ->
                when (input) {
                    seed -> listOf(crawlableDataOf("url" to inputA))
                    inputA -> listOf(crawlableDataOf("url" to inputB))
                    inputB -> listOf(crawlableDataOf("url" to inputC))
                    else -> emptyList()
                }
            },
        )

        val resultStore = InMemoryCrawlingResultStore()
        val listener = TestCrawlingSessionEventListener()

        val session = CrawlingSessionImpl(
            CrawlingSession.Config(
                initialInputs = setOf(seed),
                inputProcessingBatchSize = 10,
                maxCrawlDepth = 3,
                inputProcessors = setOf(processor),
                coroutineDispatcher = UnconfinedTestDispatcher(),
                resultStore = resultStore,
            )
        )
        session.eventListener = listener
        session.init()

        awaitSessionCompletion(listener)

        assertEquals(3, processor.totalProcessingCount)
        assertEquals(1, processor.processingCountOf(seed))
        assertEquals(1, processor.processingCountOf(inputA))
        assertEquals(1, processor.processingCountOf(inputB))
        // The output produced at the maximum crawl depth never enters the frontier.
        assertEquals(0, processor.processingCountOf(inputC))

        val finishedEvent = listener.collectedEvents.filterIsInstance<Event.CrawlingFinished>().single()
        val result = resultStore.getById(finishedEvent.resultId)!!
        assertEquals(3, result.context.getAll().size)
    }
}
