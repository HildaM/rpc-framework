package com.quan.remoting.transport.netty.client;

import com.quan.enums.CompressTypeEnum;
import com.quan.enums.SerializationTypeEnum;
import com.quan.remoting.constants.RpcConstants;
import com.quan.remoting.dto.RpcMessage;
import com.quan.remoting.dto.RpcRequest;
import com.quan.remoting.dto.RpcResponse;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.ReferenceCountUtil;
import io.protostuff.Rpc;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;

/**
 * Description:
 * date: 2023/02/21 下午 6:59
 *
 * @author Quan
 */

@Slf4j
public class NettyRpcClientHandler extends ChannelInboundHandlerAdapter {
    private final UnprocessedRequests unprocessedRequests;
    private final NettyRpcClient nettyRpcClient;

    public NettyRpcClientHandler() {
        this.unprocessedRequests = SingletonFactory.getInstance(UnprocessedRequests.class);
        this.nettyRpcClient = SingletonFactory.getInstance(NettyRpcClient.class);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        try {
            log.info("Client receive msg: [{}]", msg);
            if (msg instanceof RpcMessage) {
                RpcMessage tmp = (RpcMessage) msg;
                byte messageType = tmp.getMessageType();
                if (messageType == RpcConstants.HEARTBEAT_RESPONSE_TYPE) {
                    log.info("Heart [{}]", tmp.getData());
                }
                // 如果是响应报文，则检查之前的请求是否被完成
                else if (messageType == RpcConstants.RESPONSE_TYPE) {
                    RpcResponse<Object> rpcResponse = (RpcResponse<Object>) tmp.getData();
                    // 从未处理信息中移除
                    unprocessedRequests.complete(rpcResponse);
                }
            }
        } finally {
            // 释放msg内存空间
            ReferenceCountUtil.release(msg);
        }
    }


    // 捕获用户事件
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleState state = ((IdleStateEvent) evt).state();
            // 接收到数据写入消息
            if (state == IdleState.WRITER_IDLE) {
                log.info("Write idle happen [{}]", ctx.channel());

                // 获取发生事件的渠道
                Channel channel = nettyRpcClient.getChannel((InetSocketAddress) ctx.channel().remoteAddress());
                // 建立心跳包信息
                RpcMessage rpcMessage = new RpcMessage();
                rpcMessage.setCodec(SerializationTypeEnum.PROTOSTUFF.getCode());
                rpcMessage.setCompress(CompressTypeEnum.GZIP.getCode());
                rpcMessage.setMessageType(RpcConstants.HEARTBEAT_REQUEST_TYPE);
                rpcMessage.setData(RpcConstants.PING);

                // 发送消息
                // 对心跳包连接监听，则CLOSE_ON_FAILURE，关闭连接
                channel.writeAndFlush(rpcMessage).addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
            }
        }
    }

    // 异常捕获
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("Client catch exception: ", cause);
        cause.printStackTrace();
        ctx.close();
    }
}

