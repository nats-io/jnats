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

import java.util.concurrent.Future;

class NatsConnectionWriter implements Runnable {

    private final NatsConnectionWriterImpl impl;

    NatsConnectionWriter(NatsConnection connection) {
        String implName = System.getProperty("NatsConnectionWriterImplName");

        if ("NatsConnectionWriterQueueImpl".equals(implName)) {
            impl = new NatsConnectionWriterQueueImpl(connection);
        }
        else {
            impl = new NatsConnectionWriterLatchImpl(connection);
        }
    }

    // Should only be called if the current thread has exited.
    // Use the Future from stop() to determine if it is ok to call this.
    // This method resets that future so mistiming can result in badness.
    void start(Future<DataPort> dataPortFuture) {
        impl.start(dataPortFuture);
    }

    // May be called several times on an error.
    // Returns a future that is completed when the thread completes, not when this
    // method does.
    Future<Boolean> stop() {
        return impl.stop();
    }

    @Override
    public void run() {
        impl.run();
    }

    void setReconnectMode(boolean tf) {
        impl.setReconnectMode(tf);
    }

    boolean canQueueDuringReconnect(NatsMessage msg) {
        return impl.canQueueDuringReconnect(msg);
    }

    boolean queue(NatsMessage msg) {
        return impl.queue(msg);
    }

    void queueInternalMessage(NatsMessage msg) {
        impl.queueInternalMessage(msg);
    }

    synchronized void flushBuffer() {
        impl.flushBuffer();
    }
}
