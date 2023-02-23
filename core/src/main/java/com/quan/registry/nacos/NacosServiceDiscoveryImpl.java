package com.quan.registry.nacos;

import com.alibaba.nacos.api.config.annotation.NacosValue;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingFactory;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.quan.enums.RpcErrorMessageEnum;
import com.quan.exception.RpcException;
import com.quan.registry.ServiceDiscovery;
import com.quan.remoting.dto.RpcRequest;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * Description:
 * date: 2023/02/23 下午 8:02
 *
 * @author Quan
 */

@Slf4j
public class NacosServiceDiscoveryImpl implements ServiceDiscovery {

    @NacosValue(value = "${nacos.server:}", autoRefreshed = true)
    private static String SERVER_ADDR;
    private static final NamingService namingService;

    static {
        try {
            namingService = NamingFactory.createNamingService(SERVER_ADDR);
        } catch (NacosException e) {
            log.error("连接到Nacos时有错误发生: ", e);
            throw new RpcException(RpcErrorMessageEnum.CLIENT_CONNECT_SERVER_FAILURE);
        }
    }

    @Override
    public InetSocketAddress lookupService(RpcRequest rpcRequest) {
        try {
            List<Instance> instances = namingService.getAllInstances(rpcRequest.getRpcServiceName());
            Instance instance = instances.get(0);
            return new InetSocketAddress(instance.getIp(), instance.getPort());
        } catch (NacosException e) {
            log.error("获取服务时有错误发生:", e);
        }
        return null;
    }
}
