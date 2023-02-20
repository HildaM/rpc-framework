package com.quan.remoting.transport.netty.codec;

/**
 * Description:
 * date: 2023/02/20 下午 6:34
 *
 * @author Quan
 */

import com.quan.compress.Compress;
import com.quan.enums.CompressTypeEnum;
import com.quan.enums.SerializationTypeEnum;
import com.quan.extension.ExtensionLoader;
import com.quan.remoting.constants.RpcConstants;
import com.quan.remoting.dto.RpcMessage;
import com.quan.serialize.Serializer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>
 * custom protocol decoder
 * <p>
 * <pre>
 *   0     1     2     3     4        5     6     7     8         9          10      11     12  13  14   15 16
 *   +-----+-----+-----+-----+--------+----+----+----+------+-----------+-------+----- --+-----+-----+-------+
 *   |   magic   code        |version | full length         | messageType| codec|compress|    RequestId       |
 *   +-----------------------+--------+---------------------+-----------+-----------+-----------+------------+
 *   |                                                                                                       |
 *   |                                         body                                                          |
 *   |                                                                                                       |
 *   |                                        ... ...                                                        |
 *   +-------------------------------------------------------------------------------------------------------+
 * 4B  magic code（魔法数）   1B version（版本）   4B full length（消息长度）    1B messageType（消息类型）
 * 1B compress（压缩类型） 1B codec（序列化类型）    4B  requestId（请求的Id）
 * body（object类型数据）
 **/

@Slf4j
public class RpcMessageEncoder extends MessageToByteEncoder<RpcMessage> {
    // 原子类没有并发安全问题
    private static final AtomicInteger ATOMIC_INTEGER = new AtomicInteger(0);

    @Override
    protected void encode(ChannelHandlerContext ctx, RpcMessage rpcMessage, ByteBuf out) {
        try {
            // 填充协议中的信息
            out.writeBytes(RpcConstants.MAGIC_NUMBER);
            out.writeByte(RpcConstants.VERSION);
            // 预留4B空间给length信息
            out.writerIndex(out.writerIndex() + 4);

            byte messateType = rpcMessage.getMessageType();
            out.writeByte(messateType);
            out.writeByte(rpcMessage.getCodec());
            out.writeByte(CompressTypeEnum.GZIP.getCode());
            out.writeInt(ATOMIC_INTEGER.getAndIncrement()); // 使用atomic变量自增

            // 构建body信息
            byte[] bodyBytes = null;
            int fullLength = RpcConstants.HEAD_LENGTH;
            // 非心跳报文：fullLength = head length + body length
            if (messateType != RpcConstants.HEARTBEAT_REQUEST_TYPE && messateType != RpcConstants.HEARTBEAT_RESPONSE_TYPE) {
                // 序列化RpcMessage
                String codecName = SerializationTypeEnum.getName(rpcMessage.getCodec());
                log.info("Codec name: [{}] ", codecName);

                // TODO ExtensionLoader 作用？？？
                // 获取序列化工具
                Serializer serializer = ExtensionLoader.getExtensionLoader(Serializer.class).getExtension(codecName);
                // 序列化
                bodyBytes = serializer.serialize(rpcMessage.getData());

                // 压缩bodyBytes
                String compressName = CompressTypeEnum.getName(rpcMessage.getCompress());
                Compress compress = ExtensionLoader.getExtensionLoader(Compress.class).getExtension(compressName);
                bodyBytes = compress.compress(bodyBytes);

                fullLength += bodyBytes.length;
            }

            if (bodyBytes != null) {
                out.writeBytes(bodyBytes);
            }

            // 写回fullLength
            int writeIndex = out.writerIndex();
            // 重置out指针
            out.writerIndex(writeIndex - fullLength + RpcConstants.MAGIC_NUMBER.length + 1);
            out.writeInt(fullLength);

            out.writerIndex(writeIndex);

        } catch (Exception e) {
            log.error("Encode request error!", e);
        }
    }

}
