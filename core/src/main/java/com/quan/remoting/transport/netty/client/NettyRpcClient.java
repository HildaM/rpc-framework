package com.quan.remoting.transport.netty.client;

import com.quan.enums.CompressTypeEnum;
import com.quan.enums.SerializationTypeEnum;
import com.quan.extension.ExtensionLoader;
import com.quan.registry.ServiceDiscovery;
import com.quan.remoting.constants.RpcConstants;
import com.quan.remoting.dto.RpcMessage;
import com.quan.remoting.dto.RpcRequest;
import com.quan.remoting.dto.RpcResponse;
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
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static io.netty.bootstrap.Bootstrap.doConnect;

/**
 * Description:
 * date: 2023/02/17 下午 4:30
 *
 * @author Quan
 */

@Slf4j
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


    // 获取指定的channel渠道
    public Channel getChannel(InetSocketAddress inetSocketAddress) {
        // channelProvider: 中介者模式
        Channel channel = channelProvider.get(inetSocketAddress);
        if (channel == null) {
            // 不存在连接，则新建一个
            channel = doConnect(inetSocketAddress);
            channelProvider.set(inetSocketAddress, channel);
        }
        return channel;
    }

    // 建立网络连接，并创建channel
    @SneakyThrows
    public Channel doConnect(InetSocketAddress inetSocketAddress) {
        // ?
        CompletableFuture<Channel> completableFuture = new CompletableFuture<>();
        bootstrap.connect(inetSocketAddress).addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                log.info("The client has connected [{}] successful!", inetSocketAddress.toString());
                completableFuture.complete(future.channel());
            } else {
                throw new IllegalStateException();
            }
        });

        return completableFuture.get();
    }

    // 建立连接
    @Override
    public Object sendRpcRequest(RpcRequest rpcRequest) {
        // 采用异步调用的方式建立
        CompletableFuture<RpcResponse<Object>> resultFuture = new CompletableFuture<>();
        // 注册中心获取IP
        InetSocketAddress inetSocketAddress = serviceDiscovery.lookupService(rpcRequest);

        Channel channel = getChannel(inetSocketAddress);
        if (channel.isActive()) {
            // 记录未被服务端处理的信息
            unprocessedRequests.put(rpcRequest.getRequestId(), resultFuture);
            // 组装信息
            RpcMessage rpcMessage = RpcMessage.builder()
                    .data(rpcRequest)
                    .codec(SerializationTypeEnum.HESSIAN.getCode())
                    .compress(CompressTypeEnum.GZIP.getCode())
                    .messageType(RpcConstants.REQUEST_TYPE)
                    .build();

            channel.writeAndFlush(rpcMessage).addListener((ChannelFutureListener) future -> {
                if (future.isSuccess()) {
                    log.info("Client send message: [{}]", rpcMessage);
                } else {
                    future.channel().close();
                    resultFuture.completeExceptionally(future.cause());
                    log.error("Send failed:", future.cause());
                }
            });
        } else {
            throw new IllegalStateException();
        }

        return resultFuture;
    }
}
