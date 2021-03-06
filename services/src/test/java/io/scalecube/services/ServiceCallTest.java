package io.scalecube.services;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.scalecube.services.a.b.testing.CanaryTestingRouter;
import io.scalecube.services.a.b.testing.GreetingServiceImplA;
import io.scalecube.services.a.b.testing.GreetingServiceImplB;
import io.scalecube.testlib.BaseTest;
import io.scalecube.transport.Message;

import org.junit.Ignore;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class ServiceCallTest extends BaseTest {

  private static final String CANARY_SERVICE = "io.scalecube.services.a.b.testing.CanaryService";
  public static final String SERVICE_NAME = "io.scalecube.services.GreetingService";
  private static AtomicInteger port = new AtomicInteger(4000);

  @Test
  public void test_local_async_no_params() {
    // Create microservices cluster.
    Microservices microservices = Microservices.builder()
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build();

    ServiceCall service = microservices.dispatcher().create();

    // call the service.
    CompletableFuture<Message> future = service.invoke(ServiceCall
        .request(SERVICE_NAME, "greetingNoParams").build());

    CountDownLatch timeLatch = new CountDownLatch(1);
    future.whenComplete((message, ex) -> {
      if (ex == null) {
        System.out.println(message);
        String result = message.data();
        assertTrue(result.equals("hello unknown"));
        // print the greeting.
        System.out.println("test_local_async_no_params :" + result);
      } else {
        // print the greeting.
        System.out.println(ex);
      }
      timeLatch.countDown();
    });

    await(timeLatch, 1, TimeUnit.SECONDS);
    microservices.cluster().shutdown();
  }

  @Test
  public void test_remote_async_greeting_no_params() {
    // Create microservices cluster.
    Microservices provider = Microservices.builder()
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build();

    // Create microservices cluster.
    Microservices consumer = Microservices.builder()
        .port(port.incrementAndGet())
        .seeds(provider.cluster().address())
        .build();

    ServiceCall service = consumer.dispatcher().create();

    // call the service.
    CompletableFuture<Message> future = service.invoke(ServiceCall
        .request(SERVICE_NAME, "greetingNoParams").build());

    CountDownLatch timeLatch = new CountDownLatch(1);
    future.whenComplete((message, ex) -> {
      if (ex == null) {
        System.out.println(message);
        String result = message.data();
        assertTrue(result.equals("hello unknown"));
        // print the greeting.
        System.out.println("test_local_async_no_params :" + result);
      } else {
        // print the greeting.
        System.out.println(ex);
      }
      timeLatch.countDown();
    });

    await(timeLatch, 1, TimeUnit.SECONDS);
    provider.cluster().shutdown();
    consumer.cluster().shutdown();
  }

  @Test
  public void test_remote_void_greeting() {
    // Create microservices instance.
    Microservices gateway = Microservices.builder()
        .port(port.incrementAndGet())
        .build();

    Microservices node1 = Microservices.builder()
        .seeds(gateway.cluster().address())
        .services(new GreetingServiceImpl())
        .build();

    ServiceCall service = gateway.dispatcher().create();

    // call the service.
    CompletableFuture<Message> future = service.invoke(ServiceCall
        .request(SERVICE_NAME, "greetingVoid")
        .data(new GreetingRequest("joe"))
        .build());

    CountDownLatch timeLatch = new CountDownLatch(1);
    future.whenComplete((success, error) -> {
      if (error == null) {
        System.out.println("void return: " + success);
        assertTrue(success.data() == null);
        timeLatch.countDown();
      }
    });
    // send and forget so we have no way to know what happen
    // but at least we didn't get exception :)
    System.out.println("test_remote_void_greeting done.");
    await(timeLatch, 1, TimeUnit.SECONDS);
    gateway.cluster().shutdown();
    node1.cluster().shutdown();
  }

  @Test
  public void test_local_void_greeting() {
    // Create microservices instance.
    ServiceCall service = Microservices.builder()
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build().dispatcher().create();

    // call the service.
    CompletableFuture<Message> future = service.invoke(ServiceCall
        .request(SERVICE_NAME, "greetingVoid")
        .data(new GreetingRequest("joe"))
        .build());

    CountDownLatch timeLatch = new CountDownLatch(1);
    future.whenComplete((success, error) -> {
      if (error == null) {
        System.out.println("void return: " + success);
        assertTrue(success.data() == null);
        timeLatch.countDown();
      }
    });
    // send and forget so we have no way to know what happen
    // but at least we didn't get exception :)
    System.out.println("test_local_void_greeting done.");
    await(timeLatch, 1, TimeUnit.SECONDS);
    
  }

  @Test
  public void test_remote_async_greeting_return_string() {
    // Create microservices cluster.
    Microservices provider = Microservices.builder()
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build();

    // Create microservices cluster.
    Microservices consumer = Microservices.builder()
        .port(port.incrementAndGet())
        .seeds(provider.cluster().address())
        .build();

    ServiceCall service = consumer.dispatcher().create();

    // call the service.
    CompletableFuture<Message> future = service.invoke(ServiceCall
        .request(SERVICE_NAME, "greeting")
        .data("joe")
        .build());

    CountDownLatch timeLatch = new CountDownLatch(1);
    future.whenComplete((result, ex) -> {
      if (ex == null) {
        // print the greeting.
        System.out.println("4. remote_async_greeting_return_string :" + result);
        assertTrue(result.data().equals(" hello to: joe"));
      } else {
        // print the greeting.
        System.out.println(ex);
      }
      timeLatch.countDown();
    });
    await(timeLatch, 1, TimeUnit.SECONDS);
    provider.cluster().shutdown();
    consumer.cluster().shutdown();
  }



  @Test
  public void test_local_async_greeting_return_GreetingResponse() {
    // Create microservices cluster.
    Microservices microservices = Microservices.builder()
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build();

    ServiceCall service = microservices.dispatcher().create();

    // call the service.
    CompletableFuture<Message> future = service.invoke(ServiceCall
        .request(SERVICE_NAME, "greetingRequest")
        .data(new GreetingRequest("joe"))
        .build());

    CountDownLatch timeLatch = new CountDownLatch(1);
    future.whenComplete((result, ex) -> {
      if (ex == null) {
        // print the greeting.
        System.out.println("test_local_async_greeting_return_GreetingResponse :" + result);
        GreetingResponse data = result.data();
        assertTrue(data.getResult().equals(" hello to: joe"));
      } else {
        // print the greeting.
        System.out.println(ex);
      }
      timeLatch.countDown();
    });
    await(timeLatch, 3, TimeUnit.SECONDS);
    microservices.cluster().shutdown();
  }

  @Test
  public void test_remote_async_greeting_return_GreetingResponse() {
    // Create microservices cluster.
    Microservices provider = Microservices.builder()
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build();

    // Create microservices cluster.
    Microservices consumer = Microservices.builder()
        .port(port.incrementAndGet())
        .seeds(provider.cluster().address())
        .build();

    ServiceCall service = consumer.dispatcher().create();

    // call the service.
    CompletableFuture<Message> future = service.invoke(ServiceCall
        .request(SERVICE_NAME, "greetingRequest")
        .data(new GreetingRequest("joe"))
        .build());

    CountDownLatch timeLatch = new CountDownLatch(1);
    future.whenComplete((result, ex) -> {
      if (ex == null) {
        // print the greeting.
        System.out.println("test_remote_async_greeting_return_GreetingResponse :" + result);
        GreetingResponse data = result.data();
        assertTrue(data.getResult().equals(" hello to: joe"));
      } else {
        // print the greeting.
        System.out.println(ex);
      }
      timeLatch.countDown();
    });
    await(timeLatch, 1, TimeUnit.SECONDS);
    provider.cluster().shutdown();
    consumer.cluster().shutdown();
  }

  @Test
  public void test_local_greeting_request_timeout_expires() {
    // Create microservices instance.
    ServiceCall service = Microservices.builder()
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build()
        .dispatcher()
        .timeout(Duration.ofSeconds(1))
        .create();

    // call the service.
    CompletableFuture<Message> future = service.invoke(ServiceCall
        .request(SERVICE_NAME, "greetingRequestTimeout")
        .data(new GreetingRequest("joe", Duration.ofSeconds(2)))
        .build(), Duration.ofMillis(1));

    CountDownLatch timeLatch = new CountDownLatch(1);

    future.whenComplete((success, error) -> {
      if (error != null) {
        // print the greeting.
        System.out.println("7. local_greeting_request_timeout_expires : " + error);
        assertTrue(error instanceof TimeoutException);
      } else {
        fail();
      }
      timeLatch.countDown();
    });

    await(timeLatch, 1, TimeUnit.SECONDS);
  }

  @Test
  public void test_remote_greeting_request_timeout_expires() {
    // Create microservices cluster.
    Microservices provider = Microservices.builder()
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build();

    // Create microservices cluster.
    Microservices consumer = Microservices.builder()
        .port(port.incrementAndGet())
        .seeds(provider.cluster().address())
        .build();

    ServiceCall service = consumer.dispatcher()
        .timeout(Duration.ofSeconds(1))
        .create();

    // call the service.
    CompletableFuture<Message> future = service.invoke(ServiceCall
        .request(SERVICE_NAME, "greetingRequestTimeout")
        .data(new GreetingRequest("joe", Duration.ofSeconds(4)))
        .build());

    CountDownLatch timeLatch = new CountDownLatch(1);

    future.whenComplete((success, error) -> {
      if (error != null) {
        // print the greeting.
        System.out.println("8. remote_greeting_request_timeout_expires : " + error);
        assertTrue(error instanceof TimeoutException);
        timeLatch.countDown();
      }
    });

    try {
      await(timeLatch, 10, TimeUnit.SECONDS);
    } catch (Exception ex) {
      fail();
    }
  }

  @Test
  public void test_local_async_greeting_return_Message() {
    // Create microservices cluster.
    Microservices microservices = Microservices.builder()
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build();


    ServiceCall service = microservices.dispatcher()
        .timeout(Duration.ofSeconds(1))
        .create();

    // call the service.
    CompletableFuture<Message> future = service.invoke(ServiceCall
        .request(SERVICE_NAME, "greetingMessage")
        .data(Message.builder().data("joe").build())
        .build(), Duration.ofMillis(1));


    CountDownLatch timeLatch = new CountDownLatch(1);
    future.whenComplete((result, ex) -> {
      if (ex == null) {
        assertTrue(result.data().equals(" hello to: joe"));
        // print the greeting.
        System.out.println("9. local_async_greeting_return_Message :" + result.data());
      } else {
        // print the greeting.
        System.out.println(ex);
      }
      timeLatch.countDown();
    });
    await(timeLatch, 1, TimeUnit.SECONDS);
    microservices.cluster().shutdown();
  }

  @Test
  public void test_remote_async_greeting_return_Message() {
    // Create microservices cluster.
    Microservices provider = Microservices.builder()
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build();

    // Create microservices cluster.
    Microservices consumer = Microservices.builder()
        .port(port.incrementAndGet())
        .seeds(provider.cluster().address())
        .build();

    ServiceCall service = consumer.dispatcher()
        .create();

    // call the service.
    CompletableFuture<Message> future = service.invoke(ServiceCall
        .request(SERVICE_NAME, "greetingMessage")
        .data(Message.builder().data("joe").build())
        .build());

    CountDownLatch timeLatch = new CountDownLatch(1);
    future.whenComplete((result, ex) -> {
      if (ex == null) {
        // print the greeting.
        System.out.println("10. remote_async_greeting_return_Message :" + result.data());
        // print the greeting.
        assertTrue(result.data().equals(" hello to: joe"));
      } else {
        // print the greeting.
        System.out.println("10 failed: " + ex);
        assertTrue(result.data().equals(" hello to: joe"));
      }
      timeLatch.countDown();
    });

    await(timeLatch, 20, TimeUnit.SECONDS);
    consumer.cluster().shutdown();
  }

  @Test
  public void test_round_robin_selection_logic() {
    Microservices gateway = createSeed();

    // Create microservices instance cluster.
    Microservices provider1 = Microservices.builder()
        .seeds(gateway.cluster().address())
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build();

    // Create microservices instance cluster.
    Microservices provider2 = Microservices.builder()
        .seeds(gateway.cluster().address())
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build();

    ServiceCall service = gateway.dispatcher()
        .create();

    // call the service.
    CompletableFuture<Message> result1 = service.invoke(ServiceCall
        .request(SERVICE_NAME, "greetingMessage")
        .data(Message.builder().data("joe").build())
        .build());

    CompletableFuture<Message> result2 = service.invoke(ServiceCall
        .request(SERVICE_NAME, "greetingMessage")
        .data(Message.builder().data("joe").build())
        .build());

    CompletableFuture<Void> combined = CompletableFuture.allOf(result1, result2);
    CountDownLatch timeLatch = new CountDownLatch(1);
    combined.whenComplete((v, x) -> {
      try {
        // print the greeting.
        System.out.println("11. round_robin_selection_logic :" + result1.get());
        System.out.println("11. round_robin_selection_logic :" + result2.get());

        boolean success = !result1.get().sender().equals(result2.get().sender());

        assertTrue(success);
      } catch (Throwable e) {
        assertTrue(false);
      }
      timeLatch.countDown();
    });
    await(timeLatch, 2, TimeUnit.SECONDS);

    provider2.cluster().shutdown();
    provider1.cluster().shutdown();

  }

  @Test
  public void test_async_greeting_return_string_service_not_found_error_case() {
    Microservices gateway = createSeed();

    // Create microservices instance cluster.
    Microservices provider1 = createProvider(gateway);

    ServiceCall service = provider1.dispatcher().create();

    CountDownLatch timeLatch = new CountDownLatch(1);
    try {
      // call the service.
      CompletableFuture<Message> future = service.invoke(ServiceCall
          .request(SERVICE_NAME, "unknown")
          .data(Message.builder().data("joe").build())
          .build());

    } catch (Exception ex) {
      assertTrue(ex.getMessage().equals("No reachable member with such service: unknown"));
      timeLatch.countDown();
    }

    await(timeLatch, 1, TimeUnit.SECONDS);
    gateway.cluster().shutdown();
    provider1.cluster().shutdown();
  }

  @Test
  public void test_naive_stress_not_breaking_the_system() throws InterruptedException {
    // Create microservices cluster member.
    Microservices provider = Microservices.builder()
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build();

    // Create microservices cluster member.
    Microservices consumer = Microservices.builder()
        .port(port.incrementAndGet())
        .seeds(provider.cluster().address())
        .build();

    ServiceCall service = consumer.dispatcher().create();



    // Init params
    int warmUpCount = 1_000;
    int count = 10_000;
    CountDownLatch warmUpLatch = new CountDownLatch(warmUpCount);

    // Warm up
    for (int i = 0; i < warmUpCount; i++) {
      // call the service.
      CompletableFuture<Message> future = service.invoke(ServiceCall
          .request(SERVICE_NAME, "greetingMessage")
          .data(Message.builder().data("naive_stress_test").build())
          .build());

      future.whenComplete((success, error) -> {
        if (error == null) {
          warmUpLatch.countDown();
        }
      });
    }
    warmUpLatch.await(30, TimeUnit.SECONDS);
    assertTrue(warmUpLatch.getCount() == 0);


    // Measure
    CountDownLatch countLatch = new CountDownLatch(count);
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < count; i++) {
      CompletableFuture<Message> future = service.invoke(ServiceCall
          .request(SERVICE_NAME, "greetingMessage")
          .data(Message.builder().data("naive_stress_test").build())
          .build());

      future.whenComplete((success, error) -> {
        if (error == null) {
          countLatch.countDown();
        }
      });
    }
    System.out.println("Finished sending " + count + " messages in " + (System.currentTimeMillis() - startTime));
    countLatch.await(60, TimeUnit.SECONDS);
    System.out.println("Finished receiving " + count + " messages in " + (System.currentTimeMillis() - startTime));
    assertTrue(countLatch.getCount() == 0);
  }

  /*
   * TODO [AK]: This test is unstable and need to be fixed and un-ignored, see builds:
   * https://travis-ci.org/scalecube/scalecube/builds/186099610
   */
  @Ignore
  @Test
  public void test_service_tags() {
    Microservices gateway = Microservices.builder()
        .port(port.incrementAndGet())
        .build();

    Microservices services1 = Microservices.builder()
        .port(port.incrementAndGet())
        .seeds(gateway.cluster().address())
        .services().service(new GreetingServiceImplA()).tag("Weight", "0.3").add()
        .build()
        .build();

    Microservices services2 = Microservices.builder()
        .port(port.incrementAndGet())
        .seeds(gateway.cluster().address())
        .services().service(new GreetingServiceImplB()).tag("Weight", "0.7").add()
        .build()
        .build();

    System.out.println( gateway.cluster().members());
    
    ServiceCall service = gateway.dispatcher()
        .router(CanaryTestingRouter.class)
        .create();

    AtomicInteger count = new AtomicInteger(0);
    AtomicInteger responses = new AtomicInteger(0);
    CountDownLatch timeLatch = new CountDownLatch(1);

    for (int i = 0; i < 100; i++) {
      // call the service.
      CompletableFuture<Message> future = service.invoke(ServiceCall
          .request(CANARY_SERVICE, "greeting")
          .data("joe")
          .build());

      future.whenComplete((success, error) -> {
        responses.incrementAndGet();
        if (success.data().toString().startsWith("B")) {
          count.incrementAndGet();
          if ((responses.get() == 100) && (60 < count.get() && count.get() < 80)) {
            timeLatch.countDown();
          }
        }
      });
    }

    
    await(timeLatch, 3, TimeUnit.SECONDS);
    assertTrue((responses.get() == 100) && (60 < count.get() && count.get() < 80));
    System.out.println("Service B was called: " + count.get()  + " times.");
  }

  @Test
  public void test_dispatcher_remote_greeting_request_completes_before_timeout() {
    Duration duration = Duration.ofSeconds(1);

    // Create microservices instance.
    Microservices gateway = Microservices.builder()
        .port(port.incrementAndGet())
        .build();

    Microservices.builder()
        .seeds(gateway.cluster().address())
        .services(new GreetingServiceImpl())
        .build();

    ServiceCall service = gateway.dispatcher().timeout(Duration.ofSeconds(3)).create();

    CompletableFuture<Message> result = service.invoke(ServiceCall.request(
        "io.scalecube.services.GreetingService", "greetingRequest")
        .data(new GreetingRequest("joe"))
        .build());

    CountDownLatch timeLatch = new CountDownLatch(1);
    result.whenComplete((success, error) -> {
      if (error == null) {
        System.out.println(success);
        GreetingResponse greetings = success.data();
        // print the greeting.
        System.out.println("1. greeting_request_completes_before_timeout : " + greetings.getResult());
        assertTrue(greetings.getResult().equals(" hello to: joe"));
        timeLatch.countDown();
      } else {
        System.out.println("1. FAILED! - greeting_request_completes_before_timeout reached timeout: " + error);
        assertTrue(error.toString(), false);
        timeLatch.countDown();
      }
    });
    await(timeLatch, 10, TimeUnit.SECONDS);
  }

  @Test
  public void test_dispatcher_local_greeting_request_completes_before_timeout() {

    Microservices gateway = Microservices.builder()
        .services(new GreetingServiceImpl())
        .build();

    ServiceCall service = gateway.dispatcher().timeout(Duration.ofSeconds(3)).create();

    CompletableFuture<Message> result = service.invoke(
        ServiceCall.request(
            "io.scalecube.services.GreetingService", "greetingRequest")
            .data(new GreetingRequest("joe"))
            .build());

    CountDownLatch timeLatch = new CountDownLatch(1);
    result.whenComplete((success, error) -> {
      if (error == null) {
        System.out.println(success);
        GreetingResponse greetings = success.data();
        // print the greeting.
        System.out.println("1. greeting_request_completes_before_timeout : " + greetings.getResult());
        assertTrue(greetings.getResult().equals(" hello to: joe"));
        timeLatch.countDown();
      } else {
        System.out.println("1. FAILED! - greeting_request_completes_before_timeout reached timeout: " + error);
        assertTrue(error.toString(), false);
        timeLatch.countDown();
      }
    });
    await(timeLatch, 10, TimeUnit.SECONDS);
  }

  private Microservices createProvider(Microservices gateway) {
    return Microservices.builder()
        .seeds(gateway.cluster().address())
        .port(port.incrementAndGet())
        .build();
  }

  private Microservices createSeed() {
    return Microservices.builder()
        .port(port.incrementAndGet())
        .build();
  }

  private void await(CountDownLatch timeLatch, long timeout, TimeUnit timeUnit) {
    try {
      timeLatch.await(timeout, timeUnit);
    } catch (InterruptedException e) {
      throw new AssertionError();
    }
  }
}
