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

package io.nats.client;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

public class NUIDTests {
    @Test
    public void testDigits() {
        assertEquals(NUID.digits.length, NUID.base, "digits length does not match base modulo");
    }

    @Test
    public void testGlobalNUIDInit() {
        NUID nuid = NUID.getInstance();
        assertNotNull(nuid);
        assertNotNull(nuid.getPre(), "Expected prefix to be initialized");
        assertEquals(NUID.preLen, nuid.getPre().length);
        assertNotEquals(0, nuid.getSeq(), "Expected seq to be non-zero");
    }

    @Test
    public void testNUIDRollover() {
        NUID gnuid = NUID.getInstance();
        gnuid.setSeq(NUID.maxSeq);
        // copy
        char[] oldPre = Arrays.copyOf(gnuid.getPre(), gnuid.getPre().length);
        gnuid.next();
        assertNotEquals(oldPre, gnuid.getPre(), "Expected new pre, got the old one");
    }

    @Test
    public void testGUIDLen() {
        String nuid = new NUID().next();
        assertEquals(NUID.totalLen,
                nuid.length(), String.format("Expected len of %d, got %d", NUID.totalLen, nuid.length()));
    }

    @Test
    public void testGlobalGUIDLen() {
        String nuid = NUID.nextGlobal();
        assertEquals(NUID.totalLen,
                nuid.length(), String.format("Expected len of %d, got %d", NUID.totalLen, nuid.length()));
    }

    @Test
    public void testProperPrefix() {
        char min = (char) 255;
        char max = (char) 0;
        char[] digits = NUID.digits;
        for (char digit : digits) {
            if (digit < min) {
                min = digit;
            }
            if (digit > max) {
                max = digit;
            }
        }

        for (int i = 0; i < 1_000_000; i++) {
            NUID nuid = new NUID();
            for (int j = 0; j < NUID.preLen; j++) {
                if (nuid.pre[j] < min || nuid.pre[j] > max) {
                    fail(String.format(
                            "Iter %d. Valid range for bytes prefix: [%d..%d]\nIncorrect prefix at pos %d: %s",
                            i, (int) min, (int) max, j, new String(nuid.pre)));
                }
            }
        }
    }
}
