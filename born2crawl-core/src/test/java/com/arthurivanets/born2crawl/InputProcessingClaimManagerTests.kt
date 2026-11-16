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

import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicInteger

internal class InputProcessingClaimManagerTests {

    private val testKey = "com.test.Processor(input)[source]"

    @Test
    fun `acquire issues a claim for an unknown key`() = runBlocking {
        val manager = InputProcessingClaimManager()

        val claim = manager.acquire(testKey)

        assertNotNull(claim)
        assertEquals(1, claim!!.attempt)
        assertFalse(manager.isTerminal(testKey))
    }

    @Test
    fun `acquire returns null once the claim is marked as succeeded`() = runBlocking {
        val manager = InputProcessingClaimManager()
        val claim = manager.acquire(testKey)!!

        claim.markSucceeded()

        assertNull(manager.acquire(testKey))
        assertTrue(manager.isTerminal(testKey))
    }

    @Test
    fun `acquire returns null once all the retries are exhausted`() = runBlocking {
        val manager = InputProcessingClaimManager()
        val claim = manager.acquire(testKey)!!

        assertTrue(claim.recordFailure(maxRetries = 2))
        assertEquals(2, claim.attempt)
        assertTrue(claim.recordFailure(maxRetries = 2))
        assertEquals(3, claim.attempt)
        assertFalse(claim.recordFailure(maxRetries = 2))

        claim.markExhausted()

        assertNull(manager.acquire(testKey))
        assertTrue(manager.isTerminal(testKey))
    }

    @Test
    fun `recordFailure does not permit retries when max retries is zero`() = runBlocking {
        val manager = InputProcessingClaimManager()
        val claim = manager.acquire(testKey)!!

        assertFalse(claim.recordFailure(maxRetries = 0))
        assertEquals(2, claim.attempt)
    }

    @Test
    fun `a released claim makes the key claimable again`() = runBlocking {
        val manager = InputProcessingClaimManager()
        val claim = manager.acquire(testKey)!!

        claim.release()

        assertFalse(manager.isTerminal(testKey))

        val newClaim = manager.acquire(testKey)

        assertNotNull(newClaim)
        assertEquals(1, newClaim!!.attempt)
    }

    @Test
    fun `concurrent acquires produce a single active claim - the waiters skip after success`() = runBlocking {
        val manager = InputProcessingClaimManager()
        val acquiredClaims = AtomicInteger(0)
        val skippedRequesters = AtomicInteger(0)

        coroutineScope {
            val requesters = (0 until 10).map {
                async {
                    val claim = manager.acquire(testKey)

                    if (claim != null) {
                        acquiredClaims.incrementAndGet()
                        delay(50)
                        claim.markSucceeded()
                    } else {
                        skippedRequesters.incrementAndGet()
                    }
                }
            }

            requesters.awaitAll()
        }

        assertEquals(1, acquiredClaims.get())
        assertEquals(9, skippedRequesters.get())
        assertTrue(manager.isTerminal(testKey))
    }

    @Test
    fun `concurrent acquires wait for the active claim and re-acquire it after release`() = runBlocking {
        val manager = InputProcessingClaimManager()
        val acquiredClaims = AtomicInteger(0)

        coroutineScope {
            val firstOwner = async {
                val claim = manager.acquire(testKey)!!
                acquiredClaims.incrementAndGet()
                delay(100)
                claim.release()
            }

            // Give the first owner a head start so that its claim is definitely active.
            delay(50)

            val waiters = (0 until 5).map {
                async {
                    val claim = manager.acquire(testKey)
                    if (claim != null) {
                        acquiredClaims.incrementAndGet()
                        claim.markSucceeded()
                    }
                }
            }

            firstOwner.await()
            waiters.awaitAll()
        }

        // The first owner + a single waiter that re-acquired the released claim.
        assertEquals(2, acquiredClaims.get())
        assertTrue(manager.isTerminal(testKey))
    }

    @Test
    fun `acquire suspends while a claim is active`() {
        val manager = InputProcessingClaimManager()
        val activeClaim = java.util.concurrent.CompletableFuture<InputProcessingClaimManager.Claim>()

        // Acquire the active claim on a dedicated worker thread so that the current (test) thread
        // remains free to perform a timed (blocking) acquire and to finalize the claim later on.
        val worker = Thread {
            kotlinx.coroutines.runBlocking {
                activeClaim.complete(manager.acquire(testKey)!!)
            }
        }
        worker.isDaemon = true
        worker.start()

        val claim = activeClaim.get(5, java.util.concurrent.TimeUnit.SECONDS)

        // While the claim is active, another acquire must suspend (and time out here).
        assertThrows(kotlinx.coroutines.TimeoutCancellationException::class.java) {
            kotlinx.coroutines.runBlocking {
                withTimeout(100) {
                    manager.acquire(testKey)
                }
            }
        }

        kotlinx.coroutines.runBlocking { claim.markSucceeded() }

        // After the claim is finalized, the (terminal) acquire completes immediately with null.
        kotlinx.coroutines.runBlocking {
            assertNull(manager.acquire(testKey))
        }
    }

    @Test
    fun `cancellation of the owning coroutine releases the claim`() = runBlocking {
        val manager = InputProcessingClaimManager()
        val acquiredClaims = AtomicInteger(0)

        // Use a real multi-threaded dispatcher so that the launched coroutine runs concurrently
        // and the cancellation is delivered on a worker thread rather than eagerly.
        withContext(kotlinx.coroutines.Dispatchers.Default) {
            val job = launch {
                val claim = manager.acquire(testKey)!!
                acquiredClaims.incrementAndGet()

                try {
                    kotlinx.coroutines.awaitCancellation()
                } finally {
                    // Mimic the production cleanup: release the claim when the coroutine is cancelled.
                    claim.release()
                }
            }

            // Wait until the claim is actually acquired before cancelling.
            withTimeout(5_000) {
                while (acquiredClaims.get() == 0) {
                    delay(10)
                }
            }

            job.cancel()
            job.join()
        }

        assertFalse(manager.isTerminal(testKey))
        assertNotNull(manager.acquire(testKey))
    }

    @Test
    fun `distinct keys get independent claims`() = runBlocking {
        val manager = InputProcessingClaimManager()

        val firstClaim = manager.acquire("key-1")
        val secondClaim = manager.acquire("key-2")

        assertNotNull(firstClaim)
        assertNotNull(secondClaim)

        firstClaim!!.markSucceeded()

        // key-1 reached a terminal state and must be skipped.
        assertNull(manager.acquire("key-1"))

        // key-2 is unaffected by key-1: release its active claim and re-acquire a brand new one.
        secondClaim!!.release()
        val reAcquiredClaim = manager.acquire("key-2")
        assertNotNull(reAcquiredClaim)

        reAcquiredClaim!!.markSucceeded()
        assertNull(manager.acquire("key-2"))
    }
}
