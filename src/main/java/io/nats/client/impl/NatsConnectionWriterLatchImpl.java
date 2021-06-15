// Copyright 2015-2018 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.nats.client.impl;

import io.nats.client.Options;
import io.nats.client.support.ByteArrayBuilder;

import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

public class NatsConnectionWriterLatchImpl extends NatsConnectionWriterImpl {

    private static final int LATCH_AWAIT_INCREMENT = 1000;
    private static final int LATCH_COUNT = 1;
    private static final int MAX_ROUNDS_NO_MESSAGES = 500_000;

    private final ByteArrayBuilder regularSendBuffer;
    private final ByteArrayBuilder reconnectSendBuffer;
    private final int discardMessageCountThreshold;

    private long regularQueuedMessageCount;
    private long reconnectQueuedMessageCount;

    private final ReentrantLock lock;
    private final AtomicReference<CountDownLatch> latchRef;

    NatsConnectionWriterLatchImpl(NatsConnection connection) {
        super(connection);

        Options options = connection.getOptions();
        int bufSize = options.getBufferSize();
        regularSendBuffer = new ByteArrayBuilder(bufSize);
        reconnectSendBuffer = new ByteArrayBuilder(bufSize);

        discardMessageCountThreshold = options.isDiscardMessagesWhenOutgoingQueueFull()
                ? options.getMaxMessagesInOutgoingQueue() : Integer.MAX_VALUE;

        regularQueuedMessageCount = 0;
        reconnectQueuedMessageCount = 0;

        lock = new ReentrantLock();
        latchRef = new AtomicReference<>(new CountDownLatch(LATCH_COUNT));
    }

    // Should only be called if the current thread has exited.
    // Use the Future from stop() to determine if it is ok to call this.
    // This method resets that future so mistiming can result in badness.
    void start(Future<DataPort> dataPortFuture) {
        this.startStopLock.lock();
        try {
            this.dataPortFuture = dataPortFuture;
            this.running.set(true);
            this.stopped = connection.getExecutor().submit(this, Boolean.TRUE);
        } finally {
            this.startStopLock.unlock();
        }
    }

    // May be called several times on an error.
    // Returns a future that is completed when the thread completes, not when this
    // method does.
    Future<Boolean> stop() {
        this.running.set(false);
        return this.stopped;
    }

    @Override
    public void run() {
        try {
            dataPort = dataPortFuture.get(); // Will wait for the future to complete

            // -----
            // The choice to use a countdown latch was because it afforded both
            // a count to wait for and the ability to not wait indefinitely so as
            // not to get frozen waiting.
            // -----

            while (running.get()) {
                //noinspection ResultOfMethodCallIgnored
                latchRef.get().await(LATCH_AWAIT_INCREMENT, TimeUnit.MILLISECONDS);

                int roundsWithoutMessages = 0;
                while (++roundsWithoutMessages < MAX_ROUNDS_NO_MESSAGES) {
                    boolean rmode = reconnectMode.get();
                    long mcount = rmode ? reconnectQueuedMessageCount : regularQueuedMessageCount;
                    while (mcount > 0) {

                        // lock to have access to the buffers
                        lock.lock();
                        try {
                            ByteArrayBuilder bab = rmode ? reconnectSendBuffer : regularSendBuffer;
                            int byteCount = bab.length();
                            dataPort.write(bab.internalArray(), byteCount);
                            bab.clear();
                            connection.getNatsStatistics().registerWrite(byteCount);
                            if (rmode) {
                                reconnectQueuedMessageCount = 0;
                            } else {
                                regularQueuedMessageCount = 0;
                            }
                        } catch (IOException io) {
                            connection.handleCommunicationIssue(io);
                        } finally {
                            lock.unlock();
                        }

                        // we got messages, so reset these
                        roundsWithoutMessages = 0;

                        // check the count again
                        mcount = rmode ? reconnectQueuedMessageCount : regularQueuedMessageCount;
                    }
                }

                latchRef.set(new CountDownLatch(LATCH_COUNT));
            }
        } catch (CancellationException | ExecutionException | InterruptedException ex) {
            System.out.println(ex);
            // Exit
        } finally {
            running.set(false);
        }
    }

    void setReconnectMode(boolean reconnectMode) {
        this.reconnectMode.set(reconnectMode);
    }

    @Override
    long queuedBytes() {
        return regularSendBuffer.length();
    }

    boolean queue(NatsMessage msg) {
        if (regularQueuedMessageCount >= discardMessageCountThreshold) {
            return false;
        }
        _queue(msg, regularSendBuffer);
        return true;
    }

    void queueInternalMessage(NatsMessage msg) {
        if (reconnectMode.get()) {
            _queue(msg, reconnectSendBuffer);
        } else {
            _queue(msg, regularSendBuffer);
        }
    }

    void _queue(NatsMessage msg, ByteArrayBuilder bab) {
        lock.lock();
        try {
            //last message not consumed, wait for it be consumed
            long startSize = bab.length();
            msg.appendSerialized(bab);
            long added = bab.length() - startSize;

            // it's safe check for object equality
            if (bab == regularSendBuffer) {
                regularQueuedMessageCount++;
            }
            else {
                reconnectQueuedMessageCount++;
            }

            connection.getNatsStatistics().incrementOutMsgsAndBytes(added);
            latchRef.get().countDown();
        } finally {
            lock.unlock();
        }
    }
}
