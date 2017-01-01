package io.scalecube.services.tags;

import java.util.concurrent.CompletableFuture;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class CalculatorService implements Caclulator {

  @Override
  public CompletableFuture<Integer> add(CalcRequest request) {

    int left = request.left();
    int right = request.right();

    return completedFuture(left + right);
  }

}
