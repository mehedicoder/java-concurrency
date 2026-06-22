package com.concurrency.j_jmm;

import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.II_Result;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

@JCStressTest
// ACCEPTABLE: Consumer ran first (sees state 0, data 0)
@Outcome(id = "0, 0", expect = Expect.ACCEPTABLE, desc = "Consumer read state before Producer published anything.")
// ACCEPTABLE: Consumer ran after producer finished safely (sees state 1, data 42)
@Outcome(id = "1, 42", expect = Expect.ACCEPTABLE, desc = "Consumer safely read published state and data.")
// FORBIDDEN: The JMM nightmare! Consumer saw the signal flag was active (1), but read stale data (0)!
@Outcome(id = "1, 0", expect = Expect.ACCEPTABLE_INTERESTING, desc = "BUG! Memory reordering occurred due to weak Acquire semantics.")
@State
public class C_VarHandleFenceStressTest {

    // Plain data variable (no volatile)
    private int data = 0;

    // Flag to signal that data is ready
    private int state = 0;

    private static final VarHandle STATE_HANDLE;
    static {
        try {
            STATE_HANDLE = MethodHandles.lookup()
                    .findVarHandle(C_VarHandleFenceStressTest.class, "state", int.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Actor
    public void producer() {
        data = 42;
        // Correctly uses 'setRelease' to prevent 'data = 42' from leaking past this point downwards
        STATE_HANDLE.setRelease(this, 1);
    }

    @Actor
    public void consumer(II_Result r) {
        // BUG: Using getOpaque only guarantees atomicity and program-order visibility of the flag itself,
        // but it does NOT create a memory barrier (Acquire) for unrelated plain variables like 'data'.
        // The CPU can aggressively pull the read of 'data' to happen BEFORE the read of 'state'.
        int s = (int) STATE_HANDLE.getOpaque(this);
        int d = data;

        r.r1 = s;
        r.r2 = d;
    }
}