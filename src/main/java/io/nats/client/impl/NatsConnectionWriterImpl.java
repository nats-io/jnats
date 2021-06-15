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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

abstract class NatsConnectionWriterImpl implements Runnable {

    protected final NatsConnection connection;

    protected Future<Boolean> stopped;
    protected Future<DataPort> dataPortFuture;
    protected DataPort dataPort = null;
    protected final AtomicBoolean running;
    protected final AtomicBoolean reconnectMode;
    protected final ReentrantLock startStopLock;
    protected final long reconnectBufferSize;

    NatsConnectionWriterImpl(NatsConnection connection) {
        this.connection = connection;

        this.running = new AtomicBoolean(false);
        this.reconnectMode = new AtomicBoolean(false);
        this.startStopLock = new ReentrantLock();
        this.stopped = new CompletableFuture<>();
        ((CompletableFuture<Boolean>)this.stopped).complete(Boolean.TRUE); // we are stopped on creation

        reconnectBufferSize = connection.getOptions().getReconnectBufferSize();
    }

    abstract void start(Future<DataPort> dataPortFuture);
    abstract Future<Boolean> stop();

    void setReconnectMode(boolean tf) {
        reconnectMode.set(tf);
    }

    abstract long queuedBytes();

    boolean canQueueDuringReconnect(NatsMessage msg) {
        // don't over fill the send buffer while waiting to reconnect
        return (reconnectBufferSize < 0 || (queuedBytes() + msg.getSizeInBytes()) < reconnectBufferSize);
    }

    abstract boolean queue(NatsMessage msg);
    abstract void queueInternalMessage(NatsMessage msg);

    synchronized void flushBuffer() {
        // Since there is no connection level locking, we rely on synchronization
        // of the APIs here.
        try  {
            if (this.running.get()) {
                dataPort.flush();
            }
        } catch (Exception e) {
            // NOOP;
        }
    }
}
