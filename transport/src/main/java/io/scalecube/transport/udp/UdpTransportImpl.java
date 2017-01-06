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
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.epoll.EpollChannelOption;
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
import java.net.UnknownHostException;
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

  private final EventLoopGroup group;

  private TransportConfig config;

  NettyUDPClient client;

  public UdpTransportImpl(TransportConfig config) {
    this.udpHandler = new UdpHandler(incomingMessageSubject);
    this.group = createEventLoopGroup(config.getBossThreads(), new DefaultThreadFactory("sc-udp", true));
    this.config = config;
  }

  /**
   * bind the transport to a specific udp address.
   * 
   * @param address to bind and listen.
   * @return UDP transport.
   */
  public CompletableFuture<Transport> bind(Address address) {
    final CompletableFuture<Transport> result = new CompletableFuture<>();

    this.address = address; // Listen address

    bootstrap.channel(NioDatagramChannel.class)
        .option(ChannelOption.SO_REUSEADDR, true)
        .option(ChannelOption.SO_BROADCAST, true)
        .option(ChannelOption.SO_RCVBUF, 2048 * 2)
        .option(ChannelOption.SO_SNDBUF, 2048 * 2)
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

    try {
      client = new NettyUDPClient(new ChannelInitializer<DatagramChannel>() {
        @Override
        protected void initChannel(DatagramChannel ch) throws Exception {
        }
      }, group);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    return result;
  }

  @Override
  public Address address() {
    return address;
  }

  @Override
  public void stop() {
    this.stopped = true;
  }

  @Override
  public void stop(CompletableFuture<Void> promise) {
    this.stopped = true;
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
  public void send(Address target, Message message, CompletableFuture<Void> promise) {
    checkState(!this.stopped, "UDP transport is stopped");
    checkState(target != null, "address is null");
    checkState(message != null, "message is null");

    message.setSender(this.address);

    ByteBuf bb = Unpooled.buffer();
    MessageCodec.serialize(message, bb);
    System.out.println(message);
    DatagramChannel channel = getChannel(target);
    if (channel != null)
      channel.write(bb);

  }

  private DatagramChannel getChannel(Address target) {
    try {
      return client.createChannel(target);
    } catch (UnknownHostException | InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
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
    return EpollUtils.isEpollSuported() && config.isEnableEpoll();
  }

}
