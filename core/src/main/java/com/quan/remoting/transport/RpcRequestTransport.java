package com.quan.remoting.transport;

import com.quan.extension.SPI;
import com.quan.remoting.dto.RpcRequest;

/*
 * @author Quan
 * @createTime 2023/02/17 下午 12:03
 * @desc 发送RPC请求
 */
@SPI
public interface RpcRequestTransport {
    /**
     * send rpc request to server and get result
     *
     * @param rpcRequest message body
     * @return data from server
     */
    Object sendRpcRequest(RpcRequest rpcRequest);
}
