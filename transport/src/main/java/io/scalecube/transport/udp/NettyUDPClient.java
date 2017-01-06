package io.scalecube.transport.udp;

import io.scalecube.transport.Address;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ChannelFactory;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;

public class NettyUDPClient {

  /**
   * The boss executor which will provide threads to Netty {@link ChannelFactory} for reading from the NIO selectors.
   */
  private final EventLoopGroup boss;
  /**
   * For UDP there can only be one pipelineFactory per {@link Bootstrap}. This factory is hence part of the client
   * class.
   */
  private final ChannelInitializer<DatagramChannel> pipelineFactory;


  /**
   * Creates a new instance of the {@link NettyUDPClient}.
   * 
   * @param serverAddress The remote servers address. This address will be used when any of the default write/connect
   *        methods are used.
   * @param pipelineFactory The Netty factory used for creating a pipeline. For UDP, this pipeline factory should not
   *        have any stateful i.e non share-able handlers in it. Since Netty only has one channel for <b>ALL</b> UPD
   *        traffic.
   * @param boss The {@link EventLoopGroup} used for creating boss threads.
   * @throws UnknownHostException
   */
  public NettyUDPClient(final ChannelInitializer<DatagramChannel> pipelineFactory,
      final EventLoopGroup boss) throws UnknownHostException,
      Exception {
    this.boss = boss;
    this.pipelineFactory = pipelineFactory;
  }

  /**
   * Creates a new datagram channel instance using the {@link NioDatagramChannel} by binding to local host.
   * 
   * @param address The host machine (for e.g. 'localhost') to which it needs to bind to. This is <b>Not</b> the
   *        remote server hostname.
   * @return The newly created instance of the datagram channel.
   * @throws UnknownHostException
   */
  public DatagramChannel createChannel(Address address)
      throws UnknownHostException, InterruptedException {
    Bootstrap udpBootstrap = new Bootstrap();
    udpBootstrap.group(boss).channel(NioDatagramChannel.class)
        .option(ChannelOption.SO_BROADCAST, true)
        .option(ChannelOption.SO_RCVBUF, 2048 * 2)
        .option(ChannelOption.SO_SNDBUF, 2048 * 2)
        .handler(pipelineFactory);
    
    DatagramChannel datagramChannel = (DatagramChannel) udpBootstrap
        .bind(new InetSocketAddress(address.host(),0)).sync().channel();
    
    return datagramChannel;
  }

  public EventLoopGroup getBoss() {
    return boss;
  }

  public ChannelInitializer<DatagramChannel> getPipelineFactory() {
    return pipelineFactory;
  }
}
