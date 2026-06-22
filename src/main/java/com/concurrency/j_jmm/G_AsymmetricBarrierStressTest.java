package com.concurrency.j_jmm;

import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.II_Result;

@JCStressTest
// ACCEPTABLE: Actor 1 ran first and finished before Actor 2 read.
@Outcome(id = "1, 0", expect = Expect.ACCEPTABLE, desc = "Actor 1 successfully published its state before Actor 2 checked.")
// ACCEPTABLE: Actor 2 ran first and finished before Actor 1 read.
@Outcome(id = "0, 1", expect = Expect.ACCEPTABLE, desc = "Actor 2 successfully published its state before Actor 1 checked.")
// ACCEPTABLE: Sequential interleaved execution.
@Outcome(id = "1, 1", expect = Expect.ACCEPTABLE, desc = "Both actors managed to flush stores before reading.")
// FORBIDDEN: The Category 8 Ultimate Trap! Both actors saw 0, meaning both processors
// executed their reads out-of-order before flushing their internal store buffers!
@Outcome(id = "0, 0", expect = Expect.ACCEPTABLE_INTERESTING, desc = "BUG! Hardware store-buffering bypassed memory visibility entirely.")
@State
public class G_AsymmetricBarrierStressTest {

    // INTENTIONAL BUG: Plain integers with no hardware fence properties!
    private int flagA = 0;
    private int flagB = 0;

    @Actor
    public void actor1(II_Result r) {
        flagA = 1;          // Write local state
        r.r2 = flagB;       // Read foreign state (CPU reads stale 0 from cache while write sits in store buffer)
    }

    @Actor
    public void actor2(II_Result r) {
        flagB = 1;          // Write local state
        r.r1 = flagA;       // Read foreign state (CPU reads stale 0 from cache while write sits in store buffer)
    }
}