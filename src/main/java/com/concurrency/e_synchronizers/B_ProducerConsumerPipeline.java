package com.concurrency.e_synchronizers;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

class LogMessage {
    private final String logId;
    private final String payload;
    private final boolean isShutdownSignal;

    public LogMessage(String payload) {
        this.logId = UUID.randomUUID().toString();
        this.payload = payload;
        this.isShutdownSignal = false;
    }

    private LogMessage(boolean isShutdownSignal) {
        this.logId = "SYSTEM_SHUTDOWN";
        this.payload = "TERMINATION_MARKER";
        this.isShutdownSignal = isShutdownSignal;
    }

    public static LogMessage createShutdownSignal() {
        return new LogMessage(true);
    }

    public boolean isShutdownSignal() {
        return isShutdownSignal;
    }

    @Override
    public String toString() {
        return isShutdownSignal ? "[SHUTDOWN SIGNAL]" : String.format("[LogRecord-ID: %s]", logId.substring(0, 8));
    }
}

class TelemetryLogBuffer {
    private final BlockingQueue<LogMessage> buffer;
    private volatile boolean systemActive = true;

    public TelemetryLogBuffer(int maxCapacity) {
        this.buffer = new ArrayBlockingQueue<>(maxCapacity);
    }

    public void enqueueLog(LogMessage log) throws InterruptedException {
        if (!systemActive && !log.isShutdownSignal()) return;
        buffer.put(log);
        if (!log.isShutdownSignal()) {
            System.out.format("[%s] Enqueued. Queue Size: %d\n", Thread.currentThread().getName(), buffer.size());
        }
    }

    public LogMessage dequeueLog() throws InterruptedException {
        LogMessage log = buffer.take();
        if (!log.isShutdownSignal()) {
            System.out.format("[%s] Processed & Flushed: %s. Queue Size: %d\n", Thread.currentThread().getName(), log, buffer.size());
        }
        return log;
    }

    public void broadcastShutdown(int totalConsumers) {
        this.systemActive = false;
        try {
            // Deliver a unique shutdown token designated for every active consumer node in the cluster
            for (int i = 0; i < totalConsumers; i++) {
                buffer.put(LogMessage.createShutdownSignal());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public int getCurrentSize() {
        return this.buffer.size();
    }
}

public class B_ProducerConsumerPipeline {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("[Pipeline Launcher] Deploying scaled consumer cluster to counter production burst...\n");

        // Buffer size cap
        TelemetryLogBuffer sharedBuffer = new TelemetryLogBuffer(5);

        // Core continuous Consumer engine loop (Slow I/O: 1500ms)
        Runnable consumerTask = () -> {
            try {
                while (true) {
                    LogMessage log = sharedBuffer.dequeueLog();
                    if (log != null && log.isShutdownSignal()) {
                        System.out.format("[%s] Received shutdown signal token. Exiting loop safely.\n", Thread.currentThread().getName());
                        break;
                    }
                    Thread.sleep(1500);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };

        // Aggressive Producer loop (Fast Burst: 50ms, generating a batch of 20 logs each)
        Runnable producerTask = () -> {
            try {
                for (int i = 0; i < 20; i++) {
                    sharedBuffer.enqueueLog(new LogMessage("High Volume Production Metrics Packet"));
                    Thread.sleep(50);
                }
                System.out.format("[%s] Completed its burst batch allocation and spun down.\n", Thread.currentThread().getName());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };

        // EQUATION CONFIGURATION: # Producers (2) < # Consumers (7)
        int producerCount = 2;
        int consumerCount = 7;

        List<Thread> producerThreads = new ArrayList<>();
        List<Thread> consumerThreads = new ArrayList<>();

        // Spin up the fast producers
        for (int i = 1; i <= producerCount; i++) {
            Thread t = new Thread(producerTask, "Producer-Node-0" + i);
            producerThreads.add(t);
            t.start();
        }

        // Spin up the scaled consumer cluster to share the workload
        for (int i = 1; i <= consumerCount; i++) {
            Thread t = new Thread(consumerTask, "DB-Consumer-Service-0" + i);
            consumerThreads.add(t);
            t.start();
        }

        // 1. Wait for all fast producers to naturally complete their workloads
        for (Thread prod : producerThreads) {
            prod.join();
        }
        System.out.println("\n[System Process] All producers have completed. Initiating drain and worker shutdown sequence...");

        // 2. Broadcast exactly 7 shutdown signals so all 7 consumer threads can pop one and exit
        sharedBuffer.broadcastShutdown(consumerCount);

        // 3. Cleanly join consumer workers to finalize application lifecycles
        for (Thread cons : consumerThreads) {
            cons.join(2000);
        }

        System.out.println("\n[System Process] Production balance achieved. Cluster offline.");
    }
}