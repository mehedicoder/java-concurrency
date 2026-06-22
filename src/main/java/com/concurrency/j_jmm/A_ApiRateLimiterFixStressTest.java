package com.concurrency.j_jmm;

import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.ZZ_Result;

@JCStressTest
@Outcome(
        id = "true, false",
        expect = Expect.ACCEPTABLE,
        desc = "Client A acquired the only permit."
)
@Outcome(
        id = "false, true",
        expect = Expect.ACCEPTABLE,
        desc = "Client B acquired the only permit."
)
@Outcome(
        id = "true, true",
        expect = Expect.FORBIDDEN,
        desc = "Both clients incorrectly acquired one shared permit."
)
@Outcome(
        id = "false, false",
        expect = Expect.FORBIDDEN,
        desc = "Both clients were rejected despite one available permit."
)
@State
public class A_ApiRateLimiterFixStressTest {

    private final A_ApiRateLimiterFix limiter =
            new A_ApiRateLimiterFix(1);

    @Actor
    public void clientRequestA(ZZ_Result result) {
        result.r1 = limiter.allowRequest();
    }

    @Actor
    public void clientRequestB(ZZ_Result result) {
        result.r2 = limiter.allowRequest();
    }
}