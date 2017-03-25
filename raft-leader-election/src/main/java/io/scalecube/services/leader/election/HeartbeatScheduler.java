package io.scalecube.services.leader.election;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class HeartbeatScheduler {

  private AtomicReference<ScheduledExecutorService> executor = new AtomicReference<ScheduledExecutorService>(null);

  private final Consumer callable;

  private int timeout = 10_000;

  private final Random rmd = new Random();

  public HeartbeatScheduler(final Consumer callable, int timeout) {
    this.callable = callable;
    if (timeout > 3000) {
      this.timeout = timeout;
    }
    this.timeout = rmd.nextInt(timeout - (timeout / 2)) + (timeout / 2);
  }

  public void start() {
    if (executor.get() == null || executor.get().isShutdown() || executor.get().isTerminated()){
      this.executor.set(Executors.newScheduledThreadPool(1));
    }
    
    
    executor.get().scheduleAtFixedRate(() -> {
      callable.accept(Void.TYPE);
    }, timeout, timeout, TimeUnit.MILLISECONDS);
  }

  public void stop() {
    executor.get().shutdown();
  }

  public void reset() {
    stop();
    start();
  }
}
