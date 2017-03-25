package io.scalecube.services.leader.election;

import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.ServiceHeaders;
import io.scalecube.services.ServiceInstance;
import io.scalecube.services.leader.election.api.HeartbeatRequest;
import io.scalecube.services.leader.election.api.HeartbeatResponse;
import io.scalecube.services.leader.election.api.Leader;
import io.scalecube.services.leader.election.api.LeaderElectionService;
import io.scalecube.services.leader.election.api.VoteRequest;
import io.scalecube.services.leader.election.api.VoteResponse;
import io.scalecube.services.leader.election.clock.LogicalClock;
import io.scalecube.services.leader.election.clock.LogicalTimestamp;
import io.scalecube.services.leader.election.state.State;
import io.scalecube.services.leader.election.state.StateMachine;
import io.scalecube.transport.Message;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class RaftLeaderElection implements LeaderElectionService {

  private State state = State.FOLLOWER;

  private final StateMachine stateMachine;

  private final Microservices microservices;

  private final LogicalClock clock = new LogicalClock();

  private final AtomicReference<LogicalTimestamp> currentTerm = new AtomicReference<LogicalTimestamp>();

  private final HeartbeatScheduler scheduler;

  private final ServiceCall dispatcher;

  private final String candidateId;

  public RaftLeaderElection(Microservices microservices) {
    this.microservices = microservices;
    this.microservices.proxy().api(RaftLeaderElection.class).create();
    this.stateMachine = initStateMachine();
    this.currentTerm.set(clock.tick());
    this.scheduler = new HeartbeatScheduler(sendHeartbeat(), 10);
    this.dispatcher = microservices.dispatcher().create();
    this.candidateId = microservices.cluster().member().id();
  }

  @Override
  public CompletableFuture<HeartbeatResponse> onHeartbeat(HeartbeatRequest request) {

    scheduler.reset();

    LogicalTimestamp term = LogicalTimestamp.fromBytes(request.term());
    if (term.isAfter(currentTerm.get())) {
      currentTerm.set(term);
      stateMachine.transition(State.FOLLOWER, term);
    }
    return null;
  }

  @Override
  public CompletableFuture<VoteResponse> onRequestVote(VoteRequest request) {
    LogicalTimestamp term = LogicalTimestamp.fromBytes(request.term());
    return CompletableFuture.completedFuture(
        new VoteResponse(currentTerm.get().isBefore(term)));
  }

  @Override
  public CompletableFuture<Leader> leader() {

    return null;
  }


  /**
   * find all leader election services that are remote and send current term to all of them.
   * 
   * @return consumer.
   */
  private Consumer sendHeartbeat() {

    List<ServiceInstance> services = findPeers();

    return heartbeat -> {
      services.forEach(instance -> {
        if (LeaderElectionService.SERVICE_NAME.equals(instance.serviceName())
            && !instance.isLocal()) {

          try {
            dispatcher.invoke(composeRequest("heartbeat",
                new HeartbeatRequest(currentTerm.get().toBytes())), instance);
          } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }
      });
    };
  }

  private void sendElectionCampaign() {
    List<ServiceInstance> services = findPeers();
    CountDownLatch voteLatch = new CountDownLatch(((1 + services.size()) / 2));

    services.forEach(instance -> {
      try {
        dispatcher.invoke(composeRequest("vote", new VoteRequest(
            currentTerm.get().toBytes(),
            candidateId)), instance).whenComplete((success, error) -> {
              VoteResponse vote = success.data();
              if (vote.granted()) {
                voteLatch.countDown();
              }
            });
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    });

    try {
      voteLatch.await(3, TimeUnit.SECONDS);
      stateMachine.transition(State.LEADER, currentTerm);
    } catch (InterruptedException e) {
      stateMachine.transition(State.FOLLOWER, currentTerm);
    }
  }



  private StateMachine initStateMachine() {

    StateMachine sm = StateMachine.builder()
        .init(State.FOLLOWER)
        .addTransition(State.INACTIVE, State.CANDIDATE)
        .addTransition(State.INACTIVE, State.LEADER)
        .build();

    sm.on(State.FOLLOWER, becomeFollower());
    sm.on(State.CANDIDATE, becomeCandidate());
    sm.on(State.LEADER, becomeLeader());

    return sm;

  }

  /**
   * node becomes leader when most of the peers granted a vote on election process.
   * @return
   */
  private Consumer becomeLeader() {
    return leader-> {
      scheduler.start();
    };
  }

  /**
   * node becomes candidate when no heartbeats received until timeout has reached. when at state candidate node is
   * ticking the term and sending vote requests to all peers. peers grant vote to candidate if peers current term is
   * older than node new term
   * 
   * @return
   */
  private Consumer becomeCandidate() {
    return election -> {
      scheduler.stop();
      currentTerm.set(clock.tick());
      sendElectionCampaign();
    };
  }

  /**
   * node became follower when it initiates
   * @return
   */
  private Consumer becomeFollower() {
    return follower -> {
      scheduler.stop();
    };
  }



  private Message composeRequest(String action, Object data) {
    return Message.builder()
        .header(ServiceHeaders.SERVICE_REQUEST, LeaderElectionService.SERVICE_NAME)
        .header(ServiceHeaders.METHOD, action)
        .data(data).build();
  }

  private List<ServiceInstance> findPeers() {
    return this.microservices.services().stream()
        .filter(instance -> LeaderElectionService.SERVICE_NAME.equals(instance.serviceName())
            && !instance.isLocal())
        .collect(Collectors.toList());
  }
}
