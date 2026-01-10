package ech0.codec;

import ech0.model.NetHeader;
import io.vertx.mutiny.core.buffer.Buffer;
import lombok.experimental.UtilityClass;
import org.jspecify.annotations.NonNull;

import java.nio.charset.StandardCharsets;

@UtilityClass
public class ProtocolCodec {

  public final byte MAGIC = 0x42;
  public final byte VERSION = 1;

  // Encode: Header + Payload
  public Buffer encode(@NonNull String topic, int partition, byte @NonNull [] payload) {
    byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
    int payloadLen = payload.length;

    Buffer buffer = Buffer.buffer();
    buffer.appendByte(MAGIC);                  // magic
    buffer.appendByte(VERSION);                // version
    buffer.appendByte((byte) 0x01);             // msgType = PUBLISH
    buffer.appendByte((byte) 0x00);             // flags
    buffer.appendShort((short) topicBytes.length); // topicLen
    buffer.appendInt(partition);               // partition
    buffer.appendInt(payloadLen);              // payloadLen
    buffer.appendInt(0);                       // reserved
    buffer.appendBytes(topicBytes);            // topic
    buffer.appendBytes(payload);               // payload
    return buffer;
  }

  // Decode: Buffer -> Header + Topic + Payload
  public DecodedMessage decode(@NonNull Buffer buffer) {
    int pos = 0;
    byte magic = buffer.getByte(pos++);
    if (magic != MAGIC) throw new RuntimeException("Invalid magic");

    byte version = buffer.getByte(pos++);
    byte msgType = buffer.getByte(pos++);
    byte flags = buffer.getByte(pos++);
    short topicLen = buffer.getShort(pos);
    pos += 2;
    int partition = buffer.getInt(pos);
    pos += 4;
    int payloadLen = buffer.getInt(pos);
    pos += 4;
    int reserved = buffer.getInt(pos);
    pos += 4;

    byte[] topicBytes = buffer.getBytes(pos, pos + topicLen);
    String topic = new String(topicBytes, StandardCharsets.UTF_8);
    pos += topicLen;

    byte[] payload = buffer.getBytes(pos, pos + payloadLen);

    NetHeader header = new NetHeader(magic, version, msgType, flags, topicLen, partition, payloadLen, reserved);
    return new DecodedMessage(header, topic, payload);
  }

  public record DecodedMessage(NetHeader header, String topic, byte[] payload) {
  }
}
