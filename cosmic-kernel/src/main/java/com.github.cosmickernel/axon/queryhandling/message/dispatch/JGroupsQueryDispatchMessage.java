package com.github.cosmickernel.axon.queryhandling.message.dispatch;

import org.axonframework.queryhandling.QueryMessage;
import org.axonframework.serialization.Serializer;
import org.jgroups.util.Streamable;

import java.io.*;

public class JGroupsQueryDispatchMessage extends QueryDispatchMessage implements Streamable, Externalizable {

    private static final long serialVersionUID = -8792911964758889674L;

    public JGroupsQueryDispatchMessage() {
    }

    public JGroupsQueryDispatchMessage(QueryMessage<?, ?> queryMessage, Serializer serializer, boolean expectReply) {
        super(queryMessage, serializer, expectReply);
    }

    public void writeTo(DataOutput out) throws IOException {
        out.writeUTF(this.queryName);
        out.writeUTF(this.queryIdentifier);
        out.writeBoolean(this.expectReply);
        out.writeUTF(this.payloadType);
        out.writeUTF(this.responseType);
        out.writeUTF(this.payloadRevision == null ? "_null" : this.payloadRevision);
        out.writeInt(this.serializedPayload.length);
        out.write(this.serializedPayload);
        out.writeInt(this.serializedMetaData.length);
        out.write(this.serializedMetaData);
        out.writeInt(this.serializedResponseType.length);
        out.write(this.serializedResponseType);
    }

    public void readFrom(DataInput in) throws IOException {
        this.queryName = in.readUTF();
        this.queryIdentifier = in.readUTF();
        this.expectReply = in.readBoolean();
        this.payloadType = in.readUTF();
        this.responseType = in.readUTF();
        this.payloadRevision = in.readUTF();
        if ("_null".equals(this.payloadRevision)) {
            this.payloadRevision = null;
        }

        this.serializedPayload = new byte[in.readInt()];
        in.readFully(this.serializedPayload);
        this.serializedMetaData = new byte[in.readInt()];
        in.readFully(this.serializedMetaData);
        this.serializedResponseType = new byte[in.readInt()];
        in.readFully(this.serializedResponseType);
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        this.writeTo(out);
    }

    public void readExternal(ObjectInput in) throws IOException {
        this.readFrom(in);
    }
}
