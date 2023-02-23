package com.quan.registry;

import com.quan.extension.SPI;
import com.quan.remoting.dto.RpcRequest;

import java.net.InetSocketAddress;

/**
 * Description:
 * date: 2023/02/23 下午 7:20
 *
 * @author Quan
 */
@SPI
public interface ServiceDiscovery {
    InetSocketAddress lookupService(RpcRequest rpcRequest);
}
