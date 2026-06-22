package com.concurrency.j_jmm;

import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.II_Result;

@JCStressTest
// ACCEPTABLE: Consumer ran before the publisher set the flag.
@Outcome(id = "0, 0", expect = Expect.ACCEPTABLE, desc = "Consumer read initial state safely.")
// ACCEPTABLE: Consumer ran after the publisher safely initialized both values.
@Outcome(id = "1, 100", expect = Expect.ACCEPTABLE, desc = "Consumer read fully constructed config.")
// INTERESTING / FORBIDDEN: The Portability Bug! Consumer saw the published flag as true,
// but read a completely uninitialized config value (0)!
@Outcome(id = "1, 0", expect = Expect.ACCEPTABLE_INTERESTING, desc = "HARDWARE DEPENDENT BUG! Visible on ARM64, invisible on x86.")
@State
public class D_HardwarePortabilityStressTest {

    // Plain fields representing a custom payload structure
    private int configValue = 0;
    private int published = 0;

    @Actor
    public void publisher() {
        configValue = 100; // Step 1: Initialize data
        published = 1;     // Step 2: Set flag

        // BUG: Because 'published' is a plain int and NOT volatile,
        // the compiler or a weak-memory CPU can flip these two write!
    }

    @Actor
    public void consumer(II_Result r) {
        int p = published;  // Step 3: Read flag
        int c = configValue; // Step 4: Read data

        r.r1 = p;
        r.r2 = c;
    }
}