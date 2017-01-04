package io.scalecube.transport;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import io.netty.util.CharsetUtil;

import rx.subjects.Subject;

@ChannelHandler.Sharable
public class UdpHandler extends SimpleChannelInboundHandler<DatagramPacket> {

    private Subject<Message, Message> incomingUdpSubject;


    public UdpHandler(Subject<Message, Message> incomingUdpSubject) {
      this.incomingUdpSubject =  incomingUdpSubject;
    }
  

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg) throws Exception {
      String response = msg.content().toString(CharsetUtil.UTF_8);
      Message message = MessageCodec.deserialize(msg.content());
      incomingUdpSubject.onNext(message);
    }
}