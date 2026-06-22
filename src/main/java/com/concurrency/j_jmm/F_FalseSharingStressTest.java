package com.concurrency.j_jmm;

import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.ZZ_Result;

@JCStressTest
@Outcome(id = "true, true", expect = Expect.ACCEPTABLE, desc = "Both actors incremented their fields safely.")
@State
public class F_FalseSharingStressTest {

    // INTENTIONAL PERFORMANCE BUG: These fields sit right next to each other.
    // They share the exact same 64-byte hardware cache line.
    public long counterA = 0;
    public long counterB = 0;

    @Actor
    public void actor1() {
        // High frequency write to field A forces cache line invalidation on actor 2's core
        for (int i = 0; i < 1_000_000; i++) {
            counterA++;
        }
    }

    @Actor
    public void actor2() {
        // High frequency write to field B forces cache line invalidation on actor 1's core
        for (int i = 0; i < 1_000_000; i++) {
            counterB++;
        }
    }

    @Arbiter
    public void checkResults(ZZ_Result r) {
        // Logically, the code always arrives at the correct answer
        r.r1 = (counterA == 1_000_000);
        r.r2 = (counterB == 1_000_000);
    }
}