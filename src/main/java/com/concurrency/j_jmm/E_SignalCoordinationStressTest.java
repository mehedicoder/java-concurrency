package com.concurrency.j_jmm;

import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.I_Result;

@JCStressTest
// ACCEPTABLE: Signaler fired, awaiter saw it and exited loop safely.
@Outcome(id = "1", expect = Expect.ACCEPTABLE, desc = "Awaiter successfully observed the signal and exited.")
// FORBIDDEN: The JIT optimized the spin loop into an infinite loop, causing a permanent thread stall.
@Outcome(id = "-1", expect = Expect.FORBIDDEN, desc = "BUG! Awaiter thread stalled completely due to compiler hoisting.")
@State
public class E_SignalCoordinationStressTest {

    // INTENTIONAL BUG: Not volatile!
    // The JIT compiler will optimize the reader loop by caching this value in a register.
    //private int signalReceived = 0;
    private volatile int signalReceived = 0; // FIXED: Marking this volatile prevents the JIT from hoisting the read.

    @Actor
    public void signaler() {
        signalReceived = 1; // Release the latch
    }

    @Actor
    public void awaiter(I_Result r) {
        int localSignal = 0;
        long timeoutCounter = 0;

        // Simulate a spin-wait loop checking the flag
        while (signalReceived == 0) {
            Thread.onSpinWait(); // Hint to the CPU we are spinning

            // Safety mechanism for the stress test:
            // If we loop too many times, assume we are permanently stalled.
            if (++timeoutCounter > 50_000_000L) {
                localSignal = -1; // Flag a terminal stall
                break;
            }
        }

        // If the loop exited naturally because it saw the update, localSignal remains 0,
        // so we check what the fresh reading actually is.
        if (localSignal != -1) {
            localSignal = signalReceived;
        }

        r.r1 = localSignal;
    }
}