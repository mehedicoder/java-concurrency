package com.concurrency.d_liveness;

import java.util.Random;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class CallRouteProcessor extends Thread {

    private final Lock primaryPortLock;
    private final Lock secondaryPortLock;
    private final Random backoffJitter = new Random();

    // Shared transactional metric representing global active connections remaining to bind
    private static int outstandingCallsToRoute = 500_000;

    public CallRouteProcessor(String workerName, Lock primaryPortLock, Lock secondaryPortLock) {
        super(workerName);
        this.primaryPortLock = primaryPortLock;
        this.secondaryPortLock = secondaryPortLock;
    }

    @Override
    public void run() {
        while (outstandingCallsToRoute > 0) {

            // Phase 1: Lock the inbound connection path
            primaryPortLock.lock();

            // Phase 2: Attempt to acquire the outbound connection path non-blockingly
            if (!secondaryPortLock.tryLock()) {
                // If contested, immediately step back and drop the initial lock to avoid deadlocking others
                System.out.format("[%s] Port collision detected! Backing off and releasing primary port trunk line.\n",
                        this.getName());

                primaryPortLock.unlock();

                // Introduce a tiny random backoff jitter to break up harmonic retry patterns (breaks the livelock)
                try {
                    Thread.sleep(backoffJitter.nextInt(3));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            } else {
                // If both resources are secured, finalize the transaction safely
                try {
                    if (outstandingCallsToRoute > 0) {
                        outstandingCallsToRoute--;

                        if (outstandingCallsToRoute % 50_000 == 0) {
                            System.out.format("[%s] Call successfully routed. Remaining call queue: %d\n",
                                    this.getName(), outstandingCallsToRoute);
                        }
                    }
                } catch (Exception e) {
                    System.err.format("[%s] Routing validation error: %s\n", this.getName(), e.getMessage());
                } finally {
                    // Critical section cleanup: ALWAYS guarantee releasing both resources
                    secondaryPortLock.unlock();
                    primaryPortLock.unlock();
                }
            }
        }
    }
}

public class D_LiveLock {
    public static void main(String[] args) {
        System.out.println("[VoIP Grid] Initializing multi-trunk concurrent call routing switches...");

        // Three shared physical network port locks requiring strict mutual exclusion boundaries
        Lock portSwitchA = new ReentrantLock();
        Lock portSwitchB = new ReentrantLock();
        Lock portSwitchC = new ReentrantLock();

        // Establish the circular dependency tree that triggers the conflict loop
        Thread routerAlpha = new CallRouteProcessor("VoIP-Router-US-East", portSwitchA, portSwitchB);
        Thread routerBeta  = new CallRouteProcessor("VoIP-Router-EU-West", portSwitchB, portSwitchC);
        Thread routerGamma = new CallRouteProcessor("VoIP-Router-APAC-South", portSwitchC, portSwitchA);

        routerAlpha.start();
        routerBeta.start();
        routerGamma.start();
    }
}