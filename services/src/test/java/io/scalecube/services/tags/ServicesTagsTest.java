package io.scalecube.services.tags;

import io.scalecube.services.Microservices;

import org.junit.Test;

public class ServicesTagsTest {

  @Test
  public void test_service_info() {
    
    Microservices node = Microservices.builder().services()
        .service(new CalculatorService()).tag("type", "A").add().build()
        .build();
     
    
    
  }
  
}
