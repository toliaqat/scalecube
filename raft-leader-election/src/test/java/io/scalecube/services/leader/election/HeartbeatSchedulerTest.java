package io.scalecube.services.leader.election;

import org.junit.Test;

public class HeartbeatSchedulerTest {

  @Test
  public void testHeartbeatScheduler() {
   HeartbeatScheduler scheduler = new HeartbeatScheduler(a->{
     System.out.println("executed");
   },10_000);
   
   
   scheduler.start();
   scheduler.reset();
   System.out.println("STARTED");
   
   scheduler.stop();
   System.out.println("STOPPED");
  }

  
}
