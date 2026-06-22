package com.concurrency.j_jmm;

import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.L_Result;

@JCStressTest
@Outcome(
        id = "DATA",
        expect = Expect.ACCEPTABLE,
        desc = "Consumer successfully read the produced data."
)
@Outcome(
        id = "null",
        expect = Expect.ACCEPTABLE,
        desc = "Consumer ran before publication and saw an empty queue."
)
@Outcome(
        id = "STALE_NULL",
        expect = Expect.FORBIDDEN,
        desc = "Consumer saw the published head but not the element."
)
@State
public class B_CustomQueueFixStressTest {

    private final String[] buffer = new String[2];

    private volatile int head;
    private int tail;

    @Actor
    public void producer() {
        int h = head;
        buffer[h] = "DATA";
        head = h + 1;
    }

    @Actor
    public void consumer(L_Result result) {
        int t = tail;
        int h = head;

        if (h == t) {
            result.r1 = null;
            return;
        }

        String item = buffer[t];

        if (item == null) {
            result.r1 = "STALE_NULL";
            return;
        }

        result.r1 = item;
        tail = t + 1;
    }
}