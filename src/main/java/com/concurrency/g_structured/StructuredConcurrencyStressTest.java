package com.concurrency.g_structured;

import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.ZZ_Result;
import java.util.concurrent.StructuredTaskScope;

@JCStressTest
// ACCEPTABLE: One fork or the other safely saw or handled the token.
@Outcome(id = "true, false", expect = Expect.ACCEPTABLE, desc = "Fork A initialized, Fork B saw it or vice versa.")
@Outcome(id = "false, true", expect = Expect.ACCEPTABLE, desc = "Fork B initialized, Fork A saw it or vice versa.")
// FORBIDDEN: The JMM vulnerability! Both forks simultaneously saw 'token == null',
// entered the block, and caused a classic race condition inside a "safe" Scoped Value context.
@Outcome(id = "true, true", expect = Expect.FORBIDDEN, desc = "BUG! Simultaneous state mutations inside shared scoped values.")
@State
public class StructuredConcurrencyStressTest {

    // A shared ScopedValue container holding a mutable context payload
    public static final ScopedValue<RequestContext> CONTEXT = ScopedValue.newInstance();

    public static class RequestContext {
        // INTENTIONAL BUG: Unsynchronized plain variable inside an inherited scope context!
        public String securityToken = null;
    }

    @Actor
    public void executeStructuredBatch(ZZ_Result r) {
        RequestContext sharedContext = new RequestContext();

        // Bind the context to the current scope execution
        ScopedValue.where(CONTEXT, sharedContext).run(() -> {
            try (var scope = StructuredTaskScope.open(
                    StructuredTaskScope.Joiner.allSuccessfulOrThrow())) {

                // Fork A: Simulates background authentication task
                var forkA = scope.fork(() -> {
                    RequestContext ctx = CONTEXT.get();
                    if (ctx.securityToken == null) {
                        ctx.securityToken = "AUTH_A";
                        return true;
                    }
                    return false;
                });

                // Fork B: Simulates concurrent logging/auditing task needing authentication
                var forkB = scope.fork(() -> {
                    RequestContext ctx = CONTEXT.get();
                    if (ctx.securityToken == null) {
                        ctx.securityToken = "AUTH_B";
                        return true;
                    }
                    return false;
                });

                scope.join();

                r.r1 = forkA.get();
                r.r2 = forkB.get();
            } catch (Exception e) {
                r.r1 = false;
                r.r2 = false;
            }
        });
    }
}