package com.concurrency.j_jmm;

import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.L_Result; // Using L_Result for Object references

@JCStressTest
@Outcome(id = "DATA", expect = Expect.ACCEPTABLE, desc = "Consumer successfully read the produced data.")
@Outcome(id = "null", expect = Expect.ACCEPTABLE, desc = "Consumer read an empty queue safely.")
@Outcome(id = "STALE_NULL", expect = Expect.ACCEPTABLE_INTERESTING, desc = "BUG! Consumer saw updated head but element was null.")
@State
public class B_CustomQueueStressTest {

    private final String[] buffer = new String[2];
    private int head = 0;
    private int tail = 0;

    @Actor
    public void producer() {
        int h = head;
        buffer[h] = "DATA";
        head = h + 1;
    }

    @Actor
    public void consumer(L_Result r) {
        int t = tail;
        int h = head;

        if (h != t) {
            String item = buffer[t];
            if (item == null) {
                r.r1 = "STALE_NULL";
            } else {
                r.r1 = item;
                tail = t + 1;
            }
        } else {
            r.r1 = null; // Storing actual null into L_Result matches the string representation "null" in @Outcome
        }
    }
}