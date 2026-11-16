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

import com.arthurivanets.born2crawl.util.UniqueIds
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * A simple thread gate that allows to block the [CountingInputProcessor]'s processing at a precise point
 * until the test decides to release it.
 */
internal class ProcessingGate {

    private val latch = CountDownLatch(1)
    private val waiters = AtomicInteger(0)

    val awaitingCount: Int
        get() = waiters.get()

    fun awaitIfNeeded() {
        waiters.incrementAndGet()
        try {
            check(latch.await(30, TimeUnit.SECONDS)) { "The ProcessingGate was not released in time" }
        } finally {
            waiters.decrementAndGet()
        }
    }

    fun release() {
        latch.countDown()
    }
}

/**
 * An [InputProcessor] that records the number of the processing invocations (both total and per-input),
 * enforces a configurable maximum processing concurrency, and supports failure / cancellation simulation.
 */
internal class CountingInputProcessor(
    tagSuffix: String,
    private val inputProbingRegex: Regex = Regex(".+"),
    private val outputProvider: (String) -> List<Map<String, InputProcessor.ValueHolder>> = { emptyList() },
    private val gate: ProcessingGate? = null,
    private val failOnce: Boolean = false,
    private val failAlways: Boolean = false,
    private val throwException: Boolean = false,
    private val maxObservedConcurrency: AtomicInteger? = null,
) : InputProcessor {

    override val source = Source("CountingProcessor-$tagSuffix", UniqueIds.random())

    private val activeProcessings = AtomicInteger(0)
    private val totalProcessings = AtomicInteger(0)
    private val processingsByInput = mutableMapOf<String, AtomicInteger>()
    private val remainingFailures = AtomicInteger(if (failOnce) 1 else 0)

    val totalProcessingCount: Int
        get() = totalProcessings.get()

    fun processingCountOf(input: String): Int {
        return synchronized(processingsByInput) {
            processingsByInput[input]?.get() ?: 0
        }
    }

    override fun canProcess(input: CrawlingInput, context: CrawlingContext): Boolean {
        return input.rawInput.matches(inputProbingRegex)
    }

    override fun process(input: CrawlingInput, context: CrawlingContext): InputProcessor.Result {
        val active = activeProcessings.incrementAndGet()

        maxObservedConcurrency?.let { max ->
            var currentMax = max.get()
            while (active > currentMax && !max.compareAndSet(currentMax, active)) {
                currentMax = max.get()
            }
        }

        totalProcessings.incrementAndGet()

        synchronized(processingsByInput) {
            processingsByInput.getOrPut(input.rawInput) { AtomicInteger(0) }.incrementAndGet()
        }

        try {
            if (throwException) {
                throw RuntimeException("Simulated processing failure for input = ${input.rawInput}")
            }

            if (failAlways || shouldFailOnce()) {
                return InputProcessor.Result.Failure(RuntimeException("Simulated failure for input = ${input.rawInput}"))
            }

            // Block the processing thread until the test releases the gate.
            gate?.awaitIfNeeded()

            return InputProcessor.Result.Success(
                InputProcessor.Output(
                    source = source,
                    startedBy = input.source,
                    input = input.rawInput,
                    data = outputProvider(input.rawInput),
                    timestamp = System.currentTimeMillis(),
                )
            )
        } finally {
            activeProcessings.decrementAndGet()
        }
    }

    private fun shouldFailOnce(): Boolean {
        if (!failOnce) {
            return false
        }

        while (true) {
            val remaining = remainingFailures.get()

            if (remaining == 0) {
                return false
            }

            if (remainingFailures.compareAndSet(remaining, remaining - 1)) {
                return true
            }
        }
    }
}
