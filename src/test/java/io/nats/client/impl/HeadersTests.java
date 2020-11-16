package io.nats.client.impl;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

public class HeadersTests {
    private static final String KEY1 = "key1";
    private static final String KEY2 = "key2";
    private static final String KEY3 = "key3";
    private static final String VAL1 = "val1";
    private static final String VAL2 = "val2";
    private static final String VAL3 = "val3";
    private static final String EMPTY = "";

    @Test
    public void add_key_strings_works() {
        add(
                headers -> headers.add(KEY1, VAL1),
                headers -> headers.add(KEY1, VAL2),
                headers -> headers.add(KEY2, VAL3));
    }

    @Test
    public void add_key_collection_works() {
        add(
                headers -> headers.add(KEY1, Collections.singletonList(VAL1)),
                headers -> headers.add(KEY1, Collections.singletonList(VAL2)),
                headers -> headers.add(KEY2, Collections.singletonList(VAL3)));
    }

    private void add(
            Consumer<Headers> stepKey1Val1,
            Consumer<Headers> step2Key1Val2,
            Consumer<Headers> step3Key2Val3)
    {
        Headers headers = new Headers();

        stepKey1Val1.accept(headers);
        assertEquals(1, headers.size());
        assertTrue(headers.containsKey(KEY1));
        assertContainsExactly(headers.values(KEY1), VAL1);

        step2Key1Val2.accept(headers);
        assertEquals(1, headers.size());
        assertTrue(headers.containsKey(KEY1));
        assertContainsExactly(headers.values(KEY1), VAL1, VAL2);

        step3Key2Val3.accept(headers);
        assertEquals(2, headers.size());
        assertTrue(headers.containsKey(KEY1));
        assertTrue(headers.containsKey(KEY2));
        assertContainsExactly(headers.values(KEY1), VAL1, VAL2);
        assertContainsExactly(headers.values(KEY2), VAL3);
    }

    @Test
    public void set_key_strings_works() {
        set(
                headers -> headers.put(KEY1, VAL1),
                headers -> headers.put(KEY1, VAL2),
                headers -> headers.put(KEY2, VAL3));
    }

    @Test
    public void set_key_collection_works() {
        set(
                headers -> headers.put(KEY1, Collections.singletonList(VAL1)),
                headers -> headers.put(KEY1, Collections.singletonList(VAL2)),
                headers -> headers.put(KEY2, Collections.singletonList(VAL3)));
    }

    private void set(
            Consumer<Headers> stepKey1Val1,
            Consumer<Headers> step2Key1Val2,
            Consumer<Headers> step3Key2Val3)
    {
        Headers headers = new Headers();
        assertTrue(headers.isEmpty());

        stepKey1Val1.accept(headers);
        assertEquals(1, headers.size());
        assertTrue(headers.containsKey(KEY1));
        assertContainsExactly(headers.values(KEY1), VAL1);

        step2Key1Val2.accept(headers);
        assertEquals(1, headers.size());
        assertTrue(headers.containsKey(KEY1));
        assertContainsExactly(headers.values(KEY1), VAL2);

        step3Key2Val3.accept(headers);
        assertEquals(2, headers.size());
        assertTrue(headers.containsKey(KEY1));
        assertTrue(headers.containsKey(KEY2));
        assertContainsExactly(headers.values(KEY1), VAL2);
        assertContainsExactly(headers.values(KEY2), VAL3);
    }

    // TODO Check this test. also add invalid characters
    @Test
    public void keyCannotBeNullOrEmpty() {
        Headers headers = new Headers();
        assertThrows(IllegalArgumentException.class, () -> headers.put(null, VAL1));
        assertThrows(IllegalArgumentException.class, () -> headers.put(null, VAL1, VAL2));
        assertThrows(IllegalArgumentException.class, () -> headers.put(null, Collections.singletonList(VAL1)));
        assertThrows(IllegalArgumentException.class, () -> headers.put(EMPTY, VAL1));
        assertThrows(IllegalArgumentException.class, () -> headers.put(EMPTY, VAL1, VAL2));
        assertThrows(IllegalArgumentException.class, () -> headers.put(EMPTY, Collections.singletonList(VAL1)));
        assertThrows(IllegalArgumentException.class, () -> headers.add(null, VAL1));
        assertThrows(IllegalArgumentException.class, () -> headers.add(null, VAL1, VAL2));
        assertThrows(IllegalArgumentException.class, () -> headers.add(null, Collections.singletonList(VAL1)));
        assertThrows(IllegalArgumentException.class, () -> headers.add(EMPTY, VAL1));
        assertThrows(IllegalArgumentException.class, () -> headers.add(EMPTY, VAL1, VAL2));
        assertThrows(IllegalArgumentException.class, () -> headers.add(EMPTY, Collections.singletonList(VAL1)));
    }

    @Test
    public void valuesThatAreEmptyButAreAllowed() {
        Headers headers = new Headers();
        assertEquals(0, headers.size());

        headers.add(KEY1, "");
        assertEquals(1, headers.values(KEY1).size());

        headers.put(KEY1, "");
        assertEquals(1, headers.values(KEY1).size());

        headers = new Headers();
        headers.add(KEY1, VAL1, "", VAL2);
        assertEquals(3, headers.values(KEY1).size());

        headers.put(KEY1, VAL1, "", VAL2);
        assertEquals(3, headers.values(KEY1).size());
    }

    @Test
    public void valuesThatAreNullButAreIgnored() {
        Headers headers = new Headers();
        assertEquals(0, headers.size());

        headers.add(KEY1, VAL1, null, VAL2);
        assertEquals(2, headers.values(KEY1).size());

        headers.put(KEY1, VAL1, null, VAL2);
        assertEquals(2, headers.values(KEY1).size());

        headers.clear();
        assertEquals(0, headers.size());

        headers.add(KEY1);
        assertEquals(0, headers.size());

        headers.put(KEY1);
        assertEquals(0, headers.size());

        headers.add(KEY1, (String)null);
        assertEquals(0, headers.size());

        headers.put(KEY1, (String)null);
        assertEquals(0, headers.size());

        headers.add(KEY1, (Collection<String>)null);
        assertEquals(0, headers.size());

        headers.put(KEY1, (Collection<String> )null);
        assertEquals(0, headers.size());
    }

    @Test
    public void keyCharactersMustBePrintableExceptForColon() {
        Headers headers = new Headers();
        // ctrl characters, space and colon are not allowed
        for (char c = 0; c < 33; c++) {
            final String key = "key" + c;
            assertThrows(IllegalArgumentException.class, () -> headers.put(key, VAL1));
        }
        assertThrows(IllegalArgumentException.class, () -> headers.put("key:", VAL1));
        assertThrows(IllegalArgumentException.class, () -> headers.put("key" + (char)127, VAL1));

        // all other characters are good
        for (char c = 33; c < ':'; c++) {
            headers.put("key" + c, VAL1);
        }

        for (char c = ':' + 1; c < 127; c++) {
            headers.put("key" + c, VAL1);
        }
    }

    @Test
    public void valueCharactersMustBePrintableOrTab() {
        Headers headers = new Headers();
        // ctrl characters, except for tab not allowed
        for (char c = 0; c < 9; c++) {
            final String val = "val" + c;
            assertThrows(IllegalArgumentException.class, () -> headers.put(KEY1, val));
        }
        for (char c = 10; c < 32; c++) {
            final String val = "val" + c;
            assertThrows(IllegalArgumentException.class, () -> headers.put(KEY1, val));
        }
        assertThrows(IllegalArgumentException.class, () -> headers.put(KEY1, "val" + (char)127));

        // printable and tab are allowed
        for (char c = 32; c < 127; c++) {
            headers.put(KEY1, "" + c);
        }

        headers.put(KEY1, "val" + (char)9);
    }

    @Test
    public void removes_work() {
        Headers headers = testHeaders();
        headers.remove(KEY1);
        assertContainsKeysExactly(headers, KEY2, KEY3);

        headers = testHeaders();
        headers.remove(KEY2, KEY3);
        assertContainsKeysExactly(headers, KEY1);

        headers = testHeaders();
        headers.remove(Collections.singletonList(KEY1));
        assertContainsKeysExactly(headers, KEY2, KEY3);

        headers = testHeaders();
        headers.remove(Arrays.asList(KEY2, KEY3));
        assertContainsKeysExactly(headers, KEY1);
    }

    @Test
    public void equalsHashcodeClearSizeEmpty_work() {
        assertEquals(testHeaders(), testHeaders());
        assertEquals(testHeaders().hashCode(), testHeaders().hashCode());

        Headers headers1 = new Headers();
        headers1.put(KEY1, VAL1);
        Headers headers2 = new Headers();
        headers2.put(KEY2, VAL2);
        assertNotEquals(headers1, headers2);

        assertEquals(1, headers1.size());
        assertFalse(headers1.isEmpty());
        headers1.clear();
        assertEquals(0, headers1.size());
        assertTrue(headers1.isEmpty());
    }

    @Test
    public void serialize_deserialize() {
        Headers headers1 = new Headers();
        headers1.add(KEY1, VAL1);
        headers1.add(KEY1, VAL3);
        headers1.add(KEY2, VAL2);

        byte[] serialized = headers1.getSerialized();

        Headers headers2 = new Headers(serialized);

        assertEquals(headers1.size(), headers2.size());
        assertTrue(headers2.containsKey(KEY1));
        assertTrue(headers2.containsKey(KEY2));
        assertEquals(2, headers2.values(KEY1).size());
        assertEquals(1, headers2.values(KEY2).size());
        assertTrue(headers2.values(KEY1).contains(VAL1));
        assertTrue(headers2.values(KEY1).contains(VAL3));
        assertTrue(headers2.values(KEY2).contains(VAL2));
    }

    private Headers testHeaders() {
        Headers headers = new Headers();
        headers.put(KEY1, VAL1);
        headers.put(KEY2, VAL2);
        headers.put(KEY3, VAL3);
        return headers;
    }

    private void assertContainsExactly(List<String> actual, String... expected) {
        assertNotNull(actual);
        assertEquals(actual.size(), expected.length);
        for (String v : expected) {
            assertTrue(actual.contains(v));
        }
    }

    private void assertContainsKeysExactly(Headers header, String... expected) {
        assertEquals(header.size(), expected.length);
        for (String key : expected) {
            assertTrue(header.containsKey(key));
        }
    }
}
