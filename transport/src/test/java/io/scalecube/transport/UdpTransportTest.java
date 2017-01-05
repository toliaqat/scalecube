package io.scalecube.transport;

import io.scalecube.transport.udp.UdpTransportImpl;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class UdpTransportTest {

  @Test
  public void test_udp_transport() throws InterruptedException {

    UdpTransportImpl udpTransport1 = new UdpTransportImpl(TransportConfig.defaultConfig());
    udpTransport1.bind(Address.create("localhost", 4801));

    UdpTransportImpl udpTransport2 = new UdpTransportImpl(TransportConfig.defaultConfig());
    udpTransport2.bind(Address.create("localhost", 4802));

    CountDownLatch latch = new CountDownLatch(10000);
    
    udpTransport2.listen().subscribe(onNext -> {
      latch.countDown();
    });

    while(latch.getCount()!=0) {
      udpTransport1.send(udpTransport2.address(), Message.builder().data("hello").build());
    }
    
    latch.await(5, TimeUnit.SECONDS);
    System.out.println("done: " + latch.getCount());
  }

}
