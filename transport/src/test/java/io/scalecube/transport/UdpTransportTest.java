package io.scalecube.transport;

import org.junit.Test;

public class UdpTransportTest {

  @Test
  public void transport_udp_test() {
    TransportConfig config = TransportConfig.builder().build();
    Transport transport = Transport.bindAwait(config);
   
    TransportImpl impl = (TransportImpl) transport;
    impl.udp(transport.address());
    
    impl.udpListen().subscribe(x->{
      System.out.println(x);
    });
    
    try {
      impl.sendUdp(Message.builder().data("hello").build(), transport.address());
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    System.out.println("done");
  }
}
