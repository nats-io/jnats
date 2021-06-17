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
import java.nio.BufferOverflowException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

public class NatsConnectionWriterWaitImpl extends NatsConnectionWriterImpl {

    private static final int LATCH_COUNT = 1;

    private static final int TOTAL_SLEEP = 40;
    private static final int EACH_SLEEP = 4;
    private static final int MAX_BEFORE_FLUSH = 10;

    private final ByteArrayBuilder regularSendBuffer;
    private final ByteArrayBuilder reconnectSendBuffer;
    private final int discardMessageCountThreshold;

    private final ReentrantLock lock;
    private long regularQueuedMessageCount;
    private long reconnectQueuedMessageCount;

    private final AtomicReference<CountDownLatch> latchRef;

    NatsConnectionWriterWaitImpl(NatsConnection connection) {
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
        latchRef.get().countDown();
        return this.stopped;
    }

    @Override
    public void run() {
        try {
            dataPort = dataPortFuture.get(); // Will wait for the future to complete
            // --------------------------------------------------------------------------------
            // NOTE
            // --------------------------------------------------------------------------------
            // regularQueuedMessageCount/reconnectQueuedMessageCount are volatile variables
            // that are read in this method outside of the buffersAccessLock.lock() block.
            // They are written to inside the _queue method, inside of a lock.
            // Since we are reading, if we happen to miss a write, we don't care, as the loop
            // will just check (read) those variables again soon.
            // --------------------------------------------------------------------------------
            int waits = 0;
            while (running.get()) {
                boolean rmode = reconnectMode.get();
                long mcount = rmode ? reconnectQueuedMessageCount : regularQueuedMessageCount;

                while (waits < TOTAL_SLEEP && mcount < MAX_BEFORE_FLUSH) {
                    try { //noinspection BusyWait
                        Thread.sleep(EACH_SLEEP);
                    } catch (Exception ignore) { /* don't care */ }
                    waits += EACH_SLEEP;
                }

                if (mcount > 0) {
                    lock.lock();
                    try {
                        ByteArrayBuilder bab = rmode ? reconnectSendBuffer : regularSendBuffer;
                        int byteCount = bab.length();
                        dataPort.write(bab.internalArray(), byteCount);
                        bab.clear();
                        connection.getNatsStatistics().registerWrite(byteCount);
                        if (rmode) {
                            reconnectQueuedMessageCount = 0;
                        }
                        else {
                            regularQueuedMessageCount = 0;
                        }
                    } finally {
                        lock.unlock();
                    }
                }
            }
        } catch (IOException | BufferOverflowException io) {
            connection.handleCommunicationIssue(io);
        } catch (CancellationException | ExecutionException | InterruptedException ex) {
            // Exit
        } finally {
            running.set(false);
        }
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
        }
        finally {
            lock.unlock();
        }
    }
}
