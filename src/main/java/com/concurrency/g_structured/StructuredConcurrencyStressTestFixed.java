package com.concurrency.g_structured;

import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.ZZ_Result;
import java.util.concurrent.StructuredTaskScope;

@JCStressTest
@Outcome(id = "true, false", expect = Expect.ACCEPTABLE, desc = "Fork A initialized, Fork B saw it or vice versa.")
@Outcome(id = "false, true", expect = Expect.ACCEPTABLE, desc = "Fork B initialized, Fork A saw it or vice versa.")
@Outcome(id = "true, true", expect = Expect.FORBIDDEN, desc = "BUG! Simultaneous state mutations inside shared scoped values.")
@State
public class StructuredConcurrencyStressTestFixed {

    public static final ScopedValue<RequestContext> CONTEXT = ScopedValue.newInstance();

    public static class RequestContext {
        public String securityToken = null;
    }

    @Actor
    public void executeStructuredBatch(ZZ_Result r) {
        RequestContext sharedContext = new RequestContext();

        ScopedValue.where(CONTEXT, sharedContext).run(() -> {
            try (var scope = StructuredTaskScope.open(
                    StructuredTaskScope.Joiner.allSuccessfulOrThrow())) {

                var forkA = scope.fork(() -> {
                    RequestContext ctx = CONTEXT.get();
                    if (ctx.securityToken == null) {
                        ctx.securityToken = "AUTH_A";
                        return true;
                    }
                    return false;
                });

                var forkB = scope.fork(() -> {
                    RequestContext ctx = CONTEXT.get();
                    if (ctx.securityToken == null) {
                        ctx.securityToken = "AUTH_B";
                        return true;
                    }
                    return false;
                });

                scope.join();

                // FIXED: Guard against Subtask lifecycle state exceptions safely
                r.r1 = (forkA.state() == StructuredTaskScope.Subtask.State.SUCCESS) ? forkA.get() : false;
                r.r2 = (forkB.state() == StructuredTaskScope.Subtask.State.SUCCESS) ? forkB.get() : false;

            } catch (Throwable t) {
                // Catching Throwable handles both checked exceptions and structural runtime bugs
                r.r1 = false;
                r.r2 = false;
            }
        });
    }
}