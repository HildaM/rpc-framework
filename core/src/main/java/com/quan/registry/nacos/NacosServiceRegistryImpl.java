package com.quan.registry.nacos;

import com.alibaba.nacos.api.config.annotation.NacosValue;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingFactory;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.quan.enums.RpcErrorMessageEnum;
import com.quan.exception.RpcException;
import com.quan.registry.ServiceRegistry;
import com.quan.remoting.dto.RpcRequest;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * Description:
 * date: 2023/02/23 下午 7:49
 *
 * @author Quan
 */

@Slf4j
public class NacosServiceRegistryImpl implements ServiceRegistry {

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
    public void registerService(String serviceName, InetSocketAddress inetSocketAddress) {
        try {
            namingService.registerInstance(serviceName, inetSocketAddress.getHostName(), inetSocketAddress.getPort());
        } catch (NacosException e) {
            log.error("注册服务时有错误发生:", e);
            throw new RpcException(RpcErrorMessageEnum.REGISTER_SERVICE_FAILED);
        }
    }
}
