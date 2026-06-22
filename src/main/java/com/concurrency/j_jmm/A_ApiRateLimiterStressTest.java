package com.concurrency.j_jmm;

import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.ZZ_Result;

@JCStressTest
@Outcome(id = "true, true", expect = Expect.ACCEPTABLE_INTERESTING, desc = "Both allowed, but data race occurred!")
@Outcome(id = "true, false", expect = Expect.ACCEPTABLE, desc = "Sequential execution thread 1 then thread 2")
@Outcome(id = "false, true", expect = Expect.ACCEPTABLE, desc = "Sequential execution thread 2 then thread 1")
@State
public class A_ApiRateLimiterStressTest {

    // Target class configured with a hard limit of 1 for test predictability
    private final A_ApiRateLimiter limiter = new A_ApiRateLimiter();

    @Actor
    public void clientRequestA(ZZ_Result r) {
        r.r1 = limiter.allowRequest(); // Record outcome for Thread A
    }

    @Actor
    public void clientRequestB(ZZ_Result r) {
        r.r2 = limiter.allowRequest(); // Record outcome for Thread B
    }
}