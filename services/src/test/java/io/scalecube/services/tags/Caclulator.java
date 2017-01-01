package io.scalecube.services.tags;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;

import java.util.concurrent.CompletableFuture;

@Service("scalecube.calculator")
public interface Caclulator {

  @ServiceMethod("add")
  public CompletableFuture<Integer> add(CalcRequest request);
}
