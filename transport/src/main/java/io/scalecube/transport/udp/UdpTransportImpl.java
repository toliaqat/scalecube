package io.scalecube.transport.udp;

import static com.google.common.base.Preconditions.checkState;

import io.scalecube.transport.Address;
import io.scalecube.transport.Message;
import io.scalecube.transport.MessageCodec;
import io.scalecube.transport.NetworkEmulator;
import io.scalecube.transport.Transport;
import io.scalecube.transport.TransportConfig;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.util.concurrent.DefaultThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadFactory;

public class UdpTransportImpl implements Transport {

  private static final Logger LOGGER = LoggerFactory.getLogger(UdpTransportImpl.class);

  private final Subject<Message, Message> incomingMessageSubject = PublishSubject.<Message>create().toSerialized();

  private final Bootstrap bootstrap = new Bootstrap();
  private UdpHandler udpHandler;
  private boolean stopped;
  private Address address;

  private ChannelFuture channelFuture;

  private final EventLoopGroup group;

  private TransportConfig config;

  private ConcurrentMap<Address, Channel> udpChannels = new ConcurrentHashMap<>();

  public UdpTransportImpl(TransportConfig config) {
    this.udpHandler = new UdpHandler(incomingMessageSubject);
    this.group = createEventLoopGroup(config.getBossThreads(), new DefaultThreadFactory("sc-udp", true));
    this.config = config;
  }

  public CompletableFuture<Transport> bind(Address address) {
    final CompletableFuture<Transport> result = new CompletableFuture<>();

    // Listen address
    this.address = address;

    ChannelFuture channel = bootstrap.channel(NioDatagramChannel.class)
        .option(ChannelOption.SO_REUSEADDR, true)
        .option(ChannelOption.SO_BROADCAST, true)
        .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
        .group(group)
        // .handler(udpHandler)
        .handler(new ChannelInitializer<DatagramChannel>() {
          @Override
          protected void initChannel(DatagramChannel ch) throws Exception {
            ChannelPipeline pipeline = ch.pipeline();
            pipeline.addLast(udpHandler);
          }
        }).bind(address.host(), address.port()).addListener((ChannelFutureListener) channelFuture -> {
          if (channelFuture.isSuccess()) {
            LOGGER.info("UDP Bound to: {}", address);
            result.complete(this);
          }
        });

    this.channelFuture = channel;
    return result;
  }

  @Override
  public Address address() {
    return address;
  }

  @Override
  public void stop() {
    this.stopped = true;
    for(Channel ch : udpChannels.values()){
      ch.close();
    }
    udpChannels.clear();
  }

  @Override
  public void stop(CompletableFuture<Void> promise) {
    this.stopped = true;
    for(Channel ch : udpChannels.values()){
      ch.close();
    }
    udpChannels.clear();
  }

  @Override
  public boolean isStopped() {
    return this.stopped;
  }

  @Override
  public void send(Address address, Message message) {
    CompletableFuture<Void> result = new CompletableFuture<Void>();
    send(address, message, result);
  }

  @Override
  public void send(Address address, Message message, CompletableFuture<Void> promise) {
    checkState(!this.stopped, "UDP transport is stopped");
    InetSocketAddress endpoint = new InetSocketAddress(address.host(), address.port());
    
    Channel channel = udpChannels.computeIfAbsent(address, addr -> connect(endpoint));

    ByteBuf bb = Unpooled.buffer();
    MessageCodec.serialize(message, bb);

    channel.writeAndFlush(new DatagramPacket(bb, endpoint))
        .addListener((ChannelFuture channelFuture) -> {
          if (channelFuture.isSuccess()) {
            promise.complete(null);
          } else {
            promise.completeExceptionally(channelFuture.cause());
          }
        });
  }



  private Channel connect(InetSocketAddress endpoint) {
    CompletableFuture<Channel> promise = new CompletableFuture<>();

    channelFuture.channel().connect(endpoint)
        .addListener((ChannelFuture channelFuture) -> {
          if (channelFuture.isSuccess()) {
            promise.complete(channelFuture.channel());
          } else {
            promise.completeExceptionally(channelFuture.cause());
          }
        });
    
    try {
      return promise.get();
    } catch (InterruptedException | ExecutionException e) {
      return null;
    }
  }

  @Override
  public Observable<Message> listen() {
    checkState(!stopped, "Transport is stopped");
    return incomingMessageSubject.onBackpressureBuffer().asObservable();
  }

  @Override
  public NetworkEmulator networkEmulator() {
    return null;
  }

  public Transport udp() {
    return this;
  }

  /**
   * @return {@link EpollEventLoopGroup} or {@link NioEventLoopGroup} object dep on {@link #isEpollSupported()} call.
   */
  private EventLoopGroup createEventLoopGroup(int threadNum, ThreadFactory threadFactory) {
    return isEpollSupported()
        ? new EpollEventLoopGroup(threadNum, threadFactory)
        : new NioEventLoopGroup(threadNum, threadFactory);
  }

  private boolean isEpollSupported() {
    return EpolltUtils.isEpollSuported() && config.isEnableEpoll();
  }

}
