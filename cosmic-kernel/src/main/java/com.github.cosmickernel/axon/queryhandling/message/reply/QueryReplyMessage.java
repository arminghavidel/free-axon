package com.github.cosmickernel.axon.queryhandling.message.reply;

import org.axonframework.messaging.MetaData;
import org.axonframework.messaging.RemoteExceptionDescription;
import org.axonframework.messaging.RemoteHandlingException;
import org.axonframework.queryhandling.GenericQueryResponseMessage;
import org.axonframework.queryhandling.QueryExecutionException;
import org.axonframework.queryhandling.QueryResponseMessage;
import org.axonframework.serialization.SerializedMetaData;
import org.axonframework.serialization.SerializedObject;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.SimpleSerializedObject;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

public class QueryReplyMessage implements Serializable {

    protected String queryIdentifier;
    protected byte[] serializedMetaData;
    protected String payloadType;
    protected String payloadRevision;
    protected byte[] serializedPayload;
    protected String exceptionType;
    protected String exceptionRevision;
    protected byte[] serializedException;

    protected QueryReplyMessage() {
    }

    public QueryReplyMessage(String queryIdentifier, QueryResponseMessage<?> queryResponseMessage, Serializer serializer) {
        this.queryIdentifier = queryIdentifier;

        SerializedObject<byte[]> metaData = queryResponseMessage.serializeMetaData(serializer, byte[].class);
        this.serializedMetaData = metaData.getData();

        SerializedObject<byte[]> payload = queryResponseMessage.serializePayload(serializer, byte[].class);
        this.serializedPayload = payload.getData();
        this.payloadType = payload.getType().getName();
        this.payloadRevision = payload.getType().getRevision();

        SerializedObject<byte[]> exception = queryResponseMessage.serializeExceptionResult(serializer, byte[].class);
        this.serializedException = exception.getData();
        this.exceptionType = exception.getType().getName();
        this.exceptionRevision = exception.getType().getRevision();
    }

    public QueryResponseMessage<?> getQueryResponseMessage(Serializer serializer) {
        Object payload = deserializePayload(serializer);
        RemoteExceptionDescription exceptionDescription = deserializeException(serializer);
        SerializedMetaData<byte[]> metadata =
                new SerializedMetaData<>(this.serializedMetaData, byte[].class);
        MetaData metaData = serializer.deserialize(metadata);

        if (exceptionDescription != null) {
            return new GenericQueryResponseMessage<>(new QueryExecutionException("The remote handler threw an exception",
                    new RemoteHandlingException(exceptionDescription),
                    payload),
                    metaData);
        }
        return new GenericQueryResponseMessage<>(payload, metaData);
    }

    public String getQueryIdentifier() {
        return queryIdentifier;
    }

    public byte[] getSerializedMetaData() {
        return serializedMetaData;
    }

    public String getPayloadType() {
        return payloadType;
    }

    public String getPayloadRevision() {
        return payloadRevision;
    }

    public byte[] getSerializedPayload() {
        return serializedPayload;
    }

    public String getExceptionType() {
        return exceptionType;
    }

    public String getExceptionRevision() {
        return exceptionRevision;
    }

    public byte[] getSerializedException() {
        return serializedException;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryReplyMessage that = (QueryReplyMessage) o;
        return Objects.equals(queryIdentifier, that.queryIdentifier) && Arrays.equals(serializedMetaData, that.serializedMetaData) && Objects.equals(payloadType, that.payloadType) && Objects.equals(payloadRevision, that.payloadRevision) && Arrays.equals(serializedPayload, that.serializedPayload) && Objects.equals(exceptionType, that.exceptionType) && Objects.equals(exceptionRevision, that.exceptionRevision) && Arrays.equals(serializedException, that.serializedException);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(queryIdentifier, payloadType, payloadRevision, exceptionType, exceptionRevision);
        result = 31 * result + Arrays.hashCode(serializedMetaData);
        result = 31 * result + Arrays.hashCode(serializedPayload);
        result = 31 * result + Arrays.hashCode(serializedException);
        return result;
    }

    @Override
    public String toString() {
        return "QueryReplyMessage{" +
                "queryIdentifier='" + queryIdentifier + '\'' +
                ", serializedMetaData=" + Arrays.toString(serializedMetaData) +
                ", payloadType='" + payloadType + '\'' +
                ", payloadRevision='" + payloadRevision + '\'' +
                ", serializedPayload=" + Arrays.toString(serializedPayload) +
                ", exceptionType='" + exceptionType + '\'' +
                ", exceptionRevision='" + exceptionRevision + '\'' +
                ", serializedException=" + Arrays.toString(serializedException) +
                '}';
    }

    private Object deserializePayload(Serializer serializer) {
        return serializer.deserialize(new SimpleSerializedObject<>(serializedPayload,
                byte[].class,
                payloadType,
                payloadRevision));
    }

    private RemoteExceptionDescription deserializeException(Serializer serializer) {
        return serializer.deserialize(new SimpleSerializedObject<>(serializedException,
                byte[].class,
                exceptionType,
                exceptionRevision));
    }
}
