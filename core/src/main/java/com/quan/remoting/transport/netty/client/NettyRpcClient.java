package com.quan.remoting.transport.netty.client;

import com.quan.extension.ExtensionLoader;
import com.quan.remoting.dto.RpcRequest;
import com.quan.remoting.transport.RpcRequestTransport;
import com.quan.remoting.transport.netty.codec.RpcMessageDecoder;
import com.quan.remoting.transport.netty.codec.RpcMessageEncoder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.timeout.IdleStateHandler;

import java.util.concurrent.TimeUnit;

/**
 * Description:
 * date: 2023/02/17 下午 4:30
 *
 * @author Quan
 */
public class NettyRpcClient implements RpcRequestTransport {
    // 服务发现
    private final ServiceDiscovery serviceDiscovery;

    private final UnprocessedRequests unprocessedRequests;

    private final ChannelProvider channelProvider;

    private final Bootstrap bootstrap;

    private final EventLoopGroup eventLoopGroup;

    public NettyRpcClient() {
        eventLoopGroup = new NioEventLoopGroup();   // 同步非阻塞事件驱动

        bootstrap = new Bootstrap();
        bootstrap.group(eventLoopGroup)
                .channel(NioSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))   // 日志级别
                // 连接超时时间，超过该时间建立不上则连接失败
                // 如果15秒内没有数据发送，则发送一次心跳包
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        // 自定义序列化编解码器
                        ChannelPipeline p = ch.pipeline();
                        // If no data is sent to the server within 15 seconds, a heartbeat request is sent
                        p.addLast(new IdleStateHandler(0, 5, 0, TimeUnit.SECONDS));
                        p.addLast(new RpcMessageEncoder());     // RpcRequyest -> Bytebuf
                        p.addLast(new RpcMessageDecoder());     // Bytebuf -> RpcResponse
                        p.addLast(new NettyRpcClientHandler());
                    }
                });

        this.serviceDiscovery = ExtensionLoader.getExtensionLoader(ServiceDiscovery.class).getExtension("zk");
        this.unprocessedRequests = SingletonFactory.getInstance(UnprocessedRequests.class);
        this.channelProvider = SingletonFactory.getInstance(ChannelProvider.class);
    }


    @Override
    public Object sendRpcRequest(RpcRequest rpcRequest) {
        return null;
    }
}
