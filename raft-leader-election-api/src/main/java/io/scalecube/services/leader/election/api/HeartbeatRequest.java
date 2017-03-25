package io.scalecube.services.leader.election.api;


public class HeartbeatRequest {

  private byte[] term;

  public HeartbeatRequest(byte[] term) {
    this.term = term;
  }

  public byte[] term() {
    return term;
  }
  
}

