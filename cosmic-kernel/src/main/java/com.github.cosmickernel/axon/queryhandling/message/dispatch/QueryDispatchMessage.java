package com.github.cosmickernel.axon.queryhandling.message.dispatch;

import org.axonframework.messaging.GenericMessage;
import org.axonframework.messaging.MetaData;
import org.axonframework.messaging.responsetypes.ResponseType;
import org.axonframework.queryhandling.GenericQueryMessage;
import org.axonframework.queryhandling.QueryMessage;
import org.axonframework.serialization.SerializedMetaData;
import org.axonframework.serialization.SerializedObject;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.SimpleSerializedObject;

import java.util.Arrays;
import java.util.Objects;

public class QueryDispatchMessage {

    protected String queryIdentifier;
    protected byte[] serializedMetaData;
    protected String payloadType;
    protected String payloadRevision;
    protected byte[] serializedPayload;
    protected String queryName;
    protected String responseType;
    protected byte[] serializedResponseType;
    protected boolean expectReply;

    protected QueryDispatchMessage() {
    }

    protected QueryDispatchMessage(QueryMessage<?, ?> queryMessage, Serializer serializer, boolean expectReply) {
        this.queryName = queryMessage.getQueryName();
        this.queryIdentifier = queryMessage.getIdentifier();
        SerializedObject<byte[]> metaData = queryMessage.serializeMetaData(serializer, byte[].class);
        this.serializedMetaData = metaData.getData();
        SerializedObject<byte[]> payload = queryMessage.serializePayload(serializer, byte[].class);
        this.payloadType = payload.getType().getName();
        this.payloadRevision = payload.getType().getRevision();
        this.serializedPayload = payload.getData();
        this.expectReply = expectReply;
        this.serializedResponseType = serializer.serialize(queryMessage.getResponseType(), byte[].class).getData();
        this.responseType = queryMessage.getResponseType().getClass().getName();
    }

    public QueryMessage<?, ?> getQueryMessage(Serializer serializer) {
        SimpleSerializedObject<byte[]> serializedPayload =
                new SimpleSerializedObject<>(this.serializedPayload, byte[].class, payloadType, payloadRevision);
        final Object payload = serializer.deserialize(serializedPayload);
        SimpleSerializedObject<byte[]> serializedResponseType =
                new SimpleSerializedObject<>(this.serializedResponseType, byte[].class, responseType, null);
        final Object responseTypeObject = serializer.deserialize(serializedResponseType);
        SerializedMetaData<byte[]> serializedMetaData = new SerializedMetaData<>(this.serializedMetaData, byte[].class);
        final MetaData metaData = serializer.deserialize(serializedMetaData);
        return new GenericQueryMessage(new GenericMessage<>(queryIdentifier, payload, metaData), queryName, (ResponseType<?>) responseTypeObject);
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

    public String getQueryName() {
        return queryName;
    }

    public byte[] getSerializedResponseType() {
        return serializedResponseType;
    }

    public boolean isExpectReply() {
        return expectReply;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryDispatchMessage that = (QueryDispatchMessage) o;
        return expectReply == that.expectReply && Objects.equals(queryIdentifier, that.queryIdentifier) && Arrays.equals(serializedMetaData, that.serializedMetaData) && Objects.equals(payloadType, that.payloadType) && Objects.equals(payloadRevision, that.payloadRevision) && Arrays.equals(serializedPayload, that.serializedPayload) && Objects.equals(queryName, that.queryName) && Arrays.equals(serializedResponseType, that.serializedResponseType);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(queryIdentifier, payloadType, payloadRevision, queryName, expectReply);
        result = 31 * result + Arrays.hashCode(serializedMetaData);
        result = 31 * result + Arrays.hashCode(serializedPayload);
        result = 31 * result + Arrays.hashCode(serializedResponseType);
        return result;
    }


}
