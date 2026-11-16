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

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext

/**
 * A thread-safe registry of the processing claims of the [InputProcessor]+[CrawlingInput] pairs.
 *
 * The registry guarantees that at any given moment in time there can only exist a single valid (active) claim
 * for a distinct [InputProcessor]+[CrawlingInput] pair. All the other processing requesters that target the very
 * same pair while a claim is active have to wait for the active claim to complete and then either:
 *
 *  - acquire a new claim themselves (if the active claim was released, e.g. due to a cancellation);
 *  - skip the processing entirely (if the active claim reached a terminal state, i.e. the processing succeeded
 *    or the configured number of retries was exhausted).
 *
 * A successfully processed pair is tracked permanently, which makes it possible to terminate the traversal
 * of cyclic input graphs while preventing the same pair from being processed more than once concurrently.
 */
internal class InputProcessingClaimManager {

    private val mutex = Mutex()
    private val states = mutableMapOf<String, State>()

    private sealed interface State {

        /**
         * A currently active (in-progress) claim.
         *
         * @param completion a signal that gets completed once the owner finishes processing the pair.
         */
        class InProgress(val completion: CompletableDeferred<Unit>) : State

        /**
         * A terminal state: the pair was processed successfully and must never be processed again.
         */
        object Succeeded : State

        /**
         * A terminal state: all the permitted processing attempts have failed.
         */
        object Exhausted : State
    }

    /**
     * A valid (active) processing claim for a distinct [InputProcessor]+[CrawlingInput] pair.
     *
     * The claim MUST always be finalized (via any of the available terminal methods) once the processing of
     * the pair is completed, so that the waiting requesters can proceed.
     */
    inner class Claim internal constructor(val key: String) {

        /**
         * The ordinal number of the current processing attempt (the very first attempt has the number 1).
         */
        var attempt: Int = 1
            private set

        /**
         * Records a failed processing attempt and advances the attempt counter.
         *
         * @return true if another processing attempt is permitted, false if all the permitted attempts were exhausted.
         */
        fun recordFailure(maxRetries: Int): Boolean {
            val canRetry = (attempt <= maxRetries)
            attempt++
            return canRetry
        }

        /**
         * Marks the processing of the pair as successfully completed.
         *
         * The pair will never be processed again within the current [InputProcessingClaimManager].
         */
        suspend fun markSucceeded() = finalize(State.Succeeded)

        /**
         * Marks all the permitted processing attempts of the pair as failed.
         *
         * The pair will never be processed again within the current [InputProcessingClaimManager].
         */
        suspend fun markExhausted() = finalize(State.Exhausted)

        /**
         * Releases the claim without reaching a terminal state (e.g. when the processing gets cancelled).
         *
         * The pair becomes eligible for re-processing and a subsequent [acquire] call will issue a new claim for it.
         */
        suspend fun release() = finalize(null)

        private suspend fun finalize(nextState: State?) {
            // The finalization must be guaranteed to run even when the owning coroutine gets cancelled,
            // otherwise the waiting requesters would get stuck forever and the claim would never be released.
            withContext(NonCancellable) {
                mutex.withLock {
                    val currentState = states.remove(key)

                    if (nextState != null) {
                        states[key] = nextState
                    }

                    (currentState as? State.InProgress)?.completion?.complete(Unit)
                }
            }
        }
    }

    /**
     * Attempts to acquire a processing claim for the specified [InputProcessor]+[CrawlingInput] pair.
     *
     * If a claim for the pair is already active, suspends until the active claim is finalized.
     *
     * @return a valid [Claim] if the pair needs to be processed, null if the pair was already processed
     * successfully (or all the permitted retries were exhausted) and thus must be skipped.
     */
    suspend fun acquire(key: String): Claim? {
        while (true) {
            val completion = mutex.withLock {
                when (val state = states[key]) {
                    null -> return createClaimLocked(key)
                    is State.InProgress -> state.completion
                    is State.Succeeded, is State.Exhausted -> return null
                }
            }

            // Wait for the active claim to be finalized while NOT holding the mutex.
            completion.join()

            val claim = mutex.withLock {
                when (states[key]) {
                    // The active claim was released (e.g. cancelled): the pair is claimable again.
                    null -> createClaimLocked(key)

                    // Another requester acquired the claim in the meantime: keep waiting for it.
                    is State.InProgress -> null

                    // The active claim reached a terminal state: the pair must be skipped.
                    is State.Succeeded, is State.Exhausted -> return null
                }
            }

            if (claim != null) {
                return claim
            }
        }
    }

    private fun createClaimLocked(key: String): Claim {
        states[key] = State.InProgress(CompletableDeferred<Unit>())
        return Claim(key)
    }

    /**
     * Checks if the specified [InputProcessor]+[CrawlingInput] pair has already reached a terminal state
     * (i.e. was processed successfully or all the permitted retries were exhausted).
     */
    suspend fun isTerminal(key: String): Boolean {
        return mutex.withLock {
            when (states[key]) {
                is State.Succeeded, is State.Exhausted -> true
                else -> false
            }
        }
    }
}
