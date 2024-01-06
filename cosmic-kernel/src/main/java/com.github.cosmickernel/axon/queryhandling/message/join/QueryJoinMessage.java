package com.github.cosmickernel.axon.queryhandling.message.join;

import com.github.cosmickernel.axon.queryhandling.messagefilter.QueryMessageFilter;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class QueryJoinMessage implements Externalizable {

    private static final long serialVersionUID = 1456658552741424773L;
    private QueryMessageFilter messageFilter;
    private boolean expectReply;
    private int loadFactor;
    private int order;

    public QueryJoinMessage() {
    }

    public QueryJoinMessage(int loadFactor, QueryMessageFilter messageFilter, int order, boolean expectReply) {
        this.loadFactor = loadFactor;
        this.messageFilter = messageFilter;
        this.order = order;
        this.expectReply = expectReply;
    }

    public int getLoadFactor() {
        return this.loadFactor;
    }

    public boolean isExpectReply() {
        return this.expectReply;
    }

    public int getOrder() {
        return this.order;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(this.loadFactor);
        out.writeObject(this.messageFilter);
        out.writeInt(this.order);
        out.writeBoolean(this.expectReply);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.loadFactor = in.readInt();
        this.messageFilter = (QueryMessageFilter) in.readObject();
        this.order = in.readInt();
        this.expectReply = in.readBoolean();
    }

    public QueryMessageFilter messageFilter() {
        return this.messageFilter;
    }
}
