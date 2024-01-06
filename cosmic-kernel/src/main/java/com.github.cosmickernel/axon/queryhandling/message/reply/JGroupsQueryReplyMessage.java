package com.github.cosmickernel.axon.queryhandling.message.reply;

import org.axonframework.queryhandling.QueryResponseMessage;
import org.axonframework.serialization.Serializer;
import org.jgroups.util.Streamable;

import java.io.*;

public class JGroupsQueryReplyMessage extends QueryReplyMessage implements Streamable, Externalizable {

    private static final long serialVersionUID = 6955710928767199410L;
    private static final String NULL = "_null";

    public JGroupsQueryReplyMessage() {
    }

    public JGroupsQueryReplyMessage(String queryIdentifier, QueryResponseMessage<?> queryResponseMessage, Serializer serializer) {
        super(queryIdentifier, queryResponseMessage, serializer);
    }

    public void writeTo(DataOutput out) throws IOException {
        out.writeUTF(this.queryIdentifier);
        out.writeInt(this.serializedMetaData.length);
        out.write(this.serializedMetaData);
        this.write(out, this.payloadType, this.payloadRevision, this.serializedPayload);
        this.write(out, this.exceptionType, this.exceptionRevision, this.serializedException);
    }

    public void readFrom(DataInput in) throws IOException {
        this.queryIdentifier = in.readUTF();
        this.serializedMetaData = new byte[in.readInt()];
        in.readFully(this.serializedMetaData);
        this.payloadType = in.readUTF();
        if (NULL.equals(this.payloadType)) {
            this.payloadType = null;
        } else {
            this.payloadRevision = in.readUTF();
            if (NULL.equals(this.payloadRevision)) {
                this.payloadRevision = null;
            }

            this.serializedPayload = new byte[in.readInt()];
            in.readFully(this.serializedPayload);
        }

        this.exceptionType = in.readUTF();
        if (NULL.equals(this.exceptionType)) {
            this.exceptionType = null;
        } else {
            this.exceptionRevision = in.readUTF();
            if (NULL.equals(this.exceptionRevision)) {
                this.exceptionRevision = null;
            }

            this.serializedException = new byte[in.readInt()];
            in.readFully(this.serializedException);
        }

    }

    public void writeExternal(ObjectOutput out) throws IOException {
        this.writeTo(out);
    }

    public void readExternal(ObjectInput in) throws IOException {
        this.readFrom(in);
    }

    private void write(DataOutput out, String type, String revision, byte[] serialized) throws IOException {
        if (type == null) {
            out.writeUTF(NULL);
        } else {
            out.writeUTF(type);
            out.writeUTF(revision == null ? NULL : revision);
            out.writeInt(serialized.length);
            out.write(serialized);
        }

    }
}
