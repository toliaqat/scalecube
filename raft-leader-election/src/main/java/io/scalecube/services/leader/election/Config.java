package io.scalecube.services.leader.election;

public class Config {

  
  private int heartbeatInterval = 2000;
  private int timeout = 3000;
  
  public Config() {
  }

  public int timeout() {
    return timeout;
  }

  public int heartbeatInterval() {
    return heartbeatInterval;
  }
}
