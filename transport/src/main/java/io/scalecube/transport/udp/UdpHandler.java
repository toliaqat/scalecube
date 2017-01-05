package io.scalecube.transport.udp;

import io.scalecube.transport.Message;
import io.scalecube.transport.MessageCodec;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;

import rx.subjects.Subject;

@ChannelHandler.Sharable
public class UdpHandler extends SimpleChannelInboundHandler<DatagramPacket> {

  private Subject<Message, Message> incomingUdpSubject;

  public UdpHandler(Subject<Message, Message> incomingUdpSubject) {
    this.incomingUdpSubject = incomingUdpSubject;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg) throws Exception {
    incomingUdpSubject.onNext(MessageCodec.deserialize(msg.content()));
  }

}
