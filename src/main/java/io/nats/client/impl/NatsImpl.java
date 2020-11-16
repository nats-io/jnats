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

import java.io.IOException;

import io.nats.client.AuthHandler;
import io.nats.client.Connection;
import io.nats.client.Options;
import io.nats.client.Statistics;

/**
 * Adapter to impl package to minimize access leakage.
 */
public class NatsImpl {
    public static Connection createConnection(Options options, boolean reconnectOnConnect) throws IOException, InterruptedException {
        NatsConnection conn = new NatsConnection(options);
        conn.connect(reconnectOnConnect);
        return conn;
    }

    public static Statistics createEmptyStats() {
        return new NatsStatistics(false);
    }

    public static AuthHandler credentials(String credsFile) {
        return new FileAuthHandler(credsFile);
    }

    public static AuthHandler credentials(String jwtFile, String nkeyFile) {
        return new FileAuthHandler(jwtFile, nkeyFile);
    }

    public static AuthHandler staticCredentials(char[] jwt, char[] nkey) {
        return new StringAuthHandler(jwt, nkey);
    }
    
}