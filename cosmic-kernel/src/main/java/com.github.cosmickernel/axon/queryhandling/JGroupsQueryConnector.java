package com.github.cosmickernel.axon.queryhandling;

import com.github.cosmickernel.axon.queryhandling.callback.QueryCallback;
import com.github.cosmickernel.axon.queryhandling.callback.QueryCallbackRepository;
import com.github.cosmickernel.axon.queryhandling.callback.QueryCallbackWrapper;
import com.github.cosmickernel.axon.queryhandling.consistenthash.QueryConsistentHash;
import com.github.cosmickernel.axon.queryhandling.consistenthash.QueryConsistentHashChangeListener;
import com.github.cosmickernel.axon.queryhandling.exception.QueryBusConnectorCommunicationException;
import com.github.cosmickernel.axon.queryhandling.exception.QueryConnectionFailedException;
import com.github.cosmickernel.axon.queryhandling.exception.QueryMembershipUpdateFailedException;
import com.github.cosmickernel.axon.queryhandling.exception.QueryServiceRegistryException;
import com.github.cosmickernel.axon.queryhandling.member.QueryMember;
import com.github.cosmickernel.axon.queryhandling.member.SimpleQueryMember;
import com.github.cosmickernel.axon.queryhandling.message.dispatch.JGroupsQueryDispatchMessage;
import com.github.cosmickernel.axon.queryhandling.message.join.QueryJoinMessage;
import com.github.cosmickernel.axon.queryhandling.message.reply.JGroupsQueryReplyMessage;
import com.github.cosmickernel.axon.queryhandling.messagefilter.DenyAll;
import com.github.cosmickernel.axon.queryhandling.messagefilter.QueryMessageFilter;
import com.github.cosmickernel.axon.queryhandling.routingstrategy.AnnotationQueryRoutingStrategy;
import com.github.cosmickernel.axon.queryhandling.routingstrategy.QueryRoutingStrategy;
import org.axonframework.common.AxonThreadFactory;
import org.axonframework.common.BuilderUtils;
import org.axonframework.common.ObjectUtils;
import org.axonframework.common.Registration;
import org.axonframework.messaging.MessageHandler;
import org.axonframework.messaging.MessageHandlerInterceptor;
import org.axonframework.queryhandling.GenericQueryResponseMessage;
import org.axonframework.queryhandling.QueryBus;
import org.axonframework.queryhandling.QueryMessage;
import org.axonframework.queryhandling.QueryResponseMessage;
import org.axonframework.serialization.Serializer;
import org.jgroups.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

public class JGroupsQueryConnector implements QueryRouter, QueryBusConnector, Receiver {

    private static final Logger logger = LoggerFactory.getLogger(JGroupsQueryConnector.class);

    private final Object monitor = new Object();
    private final QueryBus localSegment;
    private final JChannel channel;
    private final String clusterName;
    private final Serializer serializer;
    private final QueryRoutingStrategy routingStrategy;
    private final QueryConsistentHashChangeListener consistentHashChangeListener;
    private ExecutorService executorService;
    private final boolean executorProvided;
    private final QueryCallbackRepository<Address> callbackRepository = new QueryCallbackRepository<>();
    private final JoinCondition joinedCondition = new JoinCondition();
    private final Map<Address, VersionedMember> members = new ConcurrentHashMap<>();
    private final AtomicReference<QueryConsistentHash> consistentHash = new AtomicReference<>(new QueryConsistentHash());
    private final AtomicInteger membershipVersion = new AtomicInteger(0);

    private View currentView;
    private int loadFactor = 0;
    private QueryMessageFilter queryFilter;

    protected JGroupsQueryConnector(Builder builder) {
        this.queryFilter = DenyAll.INSTANCE;
        builder.validate();
        this.localSegment = builder.localSegment;
        this.channel = builder.channel;
        this.clusterName = builder.clusterName;
        this.serializer = builder.serializer;
        this.routingStrategy = builder.routingStrategy;
        this.consistentHashChangeListener = builder.consistentHashChangeListener;
        ExecutorService executor = builder.executorService;
        if (executor == null) {
            this.executorProvided = false;
        } else {
            this.executorService = executor;
            this.executorProvided = true;
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public void updateMembership(int loadFactor, QueryMessageFilter queryFilter) {
        this.loadFactor = loadFactor;
        this.queryFilter = queryFilter;
        this.broadCastMembership(this.membershipVersion.getAndIncrement(), false);
    }

    protected void broadCastMembership(int updateVersion, boolean expectReply) throws QueryServiceRegistryException {

        try {
            if (this.channel.isConnected()) {
                Address localAddress = this.channel.getAddress();
                logger.info("Broadcasting membership from {}", localAddress);
                this.sendMyConfigurationTo(null, expectReply, updateVersion);
            }
        } catch (Exception e) {
            throw new QueryServiceRegistryException("Could not broadcast local membership details to the cluster", e);
        }
    }

    public void connect() throws Exception {

        if (this.channel.getClusterName() != null && !this.clusterName.equals(this.channel.getClusterName())) {
            throw new QueryConnectionFailedException("Already joined cluster: " + this.channel.getClusterName());
        } else {
            if (!this.executorProvided) {
                this.executorService = Executors.newCachedThreadPool(new AxonThreadFactory("JGroupsQueryConnector(" + this.clusterName + ")"));
            }

            this.channel.setReceiver(this);
            this.channel.connect(this.clusterName);
            Address localAddress = this.channel.getAddress();
            String localName = localAddress.toString();
            SimpleQueryMember<Address> localMember = new SimpleQueryMember<>(localName, localAddress, true, null);
            this.members.put(localAddress, new VersionedMember(localMember, this.membershipVersion.getAndIncrement()));
            this.updateConsistentHash(ch -> ch.with(localMember, this.loadFactor, this.queryFilter));
        }
    }

    public void disconnect() {
        this.channel.disconnect();
        if (!this.executorProvided) {
            this.executorService.shutdown();
        }

    }

    @Override
    public synchronized void viewAccepted(View view) {

        if (this.currentView == null) {
            this.currentView = view;
            logger.info("Local segment ({}) joined the cluster. Broadcasting configuration.", this.channel.getAddress());

            try {
                this.broadCastMembership(this.membershipVersion.get(), true);
                this.joinedCondition.markJoined();
            } catch (Exception e) {
                throw new QueryMembershipUpdateFailedException("Failed to broadcast my settings", e);
            }
        } else if (!view.equals(this.currentView)) {
            Address[][] diff = View.diff(this.currentView, view);
            Address[] joined = diff[0];
            Address[] left = diff[1];
            this.currentView = view;
            Address localAddress = this.channel.getAddress();
            Arrays.stream(left).forEach(lm -> this.updateConsistentHash(ch -> {
                VersionedMember member = this.members.get(lm);
                return member == null ? ch : ch.without(member);
            }));
            Arrays.stream(left).forEach(key -> {
                this.members.remove(key);
                this.callbackRepository.cancelCallbacks(key);
            });
            Arrays.stream(joined).filter(member -> !member.equals(localAddress)).forEach(
                    member -> this.sendMyConfigurationTo(member, true, this.membershipVersion.get()));
        }

        this.currentView = view;
    }

    @Override
    public void suspect(Address suspectedMember) {
        logger.warn("Member is suspect: {}", suspectedMember);
    }

    public void receive(Message msg) {

        this.executorService.execute(() -> {
            Object message = msg.getObject();
            if (message instanceof QueryJoinMessage queryJoinMessage) {
                this.processQueryJoinMessage(msg, queryJoinMessage);
            } else if (message instanceof JGroupsQueryDispatchMessage jGroupsQueryDispatchMessage) {
                this.processQueryDispatchMessage(msg, jGroupsQueryDispatchMessage);
            } else if (message instanceof JGroupsQueryReplyMessage jGroupsQueryReplyMessage) {
                this.processQueryReplyMessage(jGroupsQueryReplyMessage);
            } else {
                logger.warn("Received unknown query message: {}", message.getClass().getName());
            }
        });
    }

    private void processQueryReplyMessage(JGroupsQueryReplyMessage message) {
        QueryCallbackWrapper callbackWrapper = this.callbackRepository.fetchAndRemove(message.getQueryIdentifier());
        if (callbackWrapper == null) {
            logger.warn("Received a callback for a message that has either already received a callback, or which was not sent through this node. Ignoring.");
        } else {
            callbackWrapper.reportResult(message.getQueryResponseMessage(this.serializer));
        }

    }

    private void processQueryDispatchMessage(Message msg, JGroupsQueryDispatchMessage message) {

        try {
            QueryMessage<?, ?> queryMessage = message.getQueryMessage(this.serializer);
            CompletableFuture<?> query = this.localSegment.query(queryMessage);
            this.sendReply(msg.getSrc(), message.getQueryIdentifier(), GenericQueryResponseMessage.asResponseMessage(query));
        } catch (Exception e) {
            this.sendReply(msg.getSrc(), message.getQueryIdentifier(), GenericQueryResponseMessage.asResponseMessage(e));
        }
    }

    private <R> void sendReply(Address address, String queryIdentifier, QueryResponseMessage<R> queryResponseMessage) {

        JGroupsQueryReplyMessage reply;
        try {
            reply = new JGroupsQueryReplyMessage(queryIdentifier, queryResponseMessage, this.serializer);
        } catch (Exception e) {
            logger.warn(String.format("Could not serialize query reply [%s]. Sending back NULL.", queryResponseMessage), e);
            reply = new JGroupsQueryReplyMessage(queryIdentifier, GenericQueryResponseMessage.asResponseMessage(e), this.serializer);
        }

        try {
            this.channel.send(address, reply);
        } catch (Exception e) {
            logger.error("Could not send reply", e);
        }

    }

    private void processQueryJoinMessage(Message message, QueryJoinMessage joinMessage) {

        String joinedMember = message.getSrc().toString();
        View view = this.channel.getView();

        if (view != null && view.containsMember(message.getSrc())) {
            int factor = joinMessage.getLoadFactor();
            QueryMessageFilter filter = joinMessage.messageFilter();
            SimpleQueryMember<Address> member = new SimpleQueryMember<>(joinedMember, message.getSrc(), false, s -> {
            });

            synchronized (this.monitor) {
                int order = (this.members.compute(
                        member.endpoint(),
                        (k, v) -> v != null && v.order() > joinMessage.getOrder() ? v : new VersionedMember(member, joinMessage.getOrder())
                )).order();
                if (joinMessage.getOrder() != order) {
                    logger.info("Received outdated update. Discarding it.");
                    return;
                }
                this.updateConsistentHash(ch -> ch.with(member, factor, filter));
            }

            if (joinMessage.isExpectReply() && !this.channel.getAddress().equals(message.getSrc())) {
                this.sendMyConfigurationTo(member.endpoint(), false, this.membershipVersion.get());
            }

            if (logger.isInfoEnabled() && !message.getSrc().equals(this.channel.getAddress())) {
                logger.info("{} joined with load factor: {}", joinedMember, factor);
            } else {
                logger.debug("Got my own ({}) join message for load factor: {}", joinedMember, factor);
            }

            if (logger.isDebugEnabled()) {
                logger.debug("Got a network of members: {}", this.members.values());
            }
        } else {
            logger.warn("Received join message from '{}', but it's not in my current view of the cluster.", message.getSrc());
        }

    }

    private void updateConsistentHash(UnaryOperator<QueryConsistentHash> consistentHashUpdate) {
        this.consistentHashChangeListener.onConsistentHashChanged(this.consistentHash.updateAndGet(consistentHashUpdate));
    }

    private void sendMyConfigurationTo(Address endpoint, boolean expectReply, int order) {

        try {
            logger.info("Sending my configuration to {}.", ObjectUtils.getOrDefault(endpoint, "all nodes"));
            Message returnJoinMessage = new Message(endpoint, new QueryJoinMessage(this.loadFactor, this.queryFilter, order, expectReply));
            returnJoinMessage.setFlag(Message.Flag.OOB);
            this.channel.send(returnJoinMessage);
        } catch (Exception e) {
            logger.warn("An exception occurred while sending membership information to newly joined member: {}", endpoint);
        }

    }

    public boolean awaitJoined() throws InterruptedException {
        this.joinedCondition.await();
        return this.joinedCondition.isJoined();
    }

    public boolean awaitJoined(long timeout, TimeUnit timeUnit) throws InterruptedException, TimeoutException {
        this.joinedCondition.await(timeout, timeUnit);
        return this.joinedCondition.isJoined();
    }

    public String getNodeName() {
        return this.channel.getName();
    }

    protected QueryConsistentHash getConsistentHash() {
        return this.consistentHash.get();
    }

    @Override
    public <Q, R> void send(QueryMember destination, QueryMessage<?, ?> query, QueryCallback<? super Q, R> callback) throws Exception {
        this.callbackRepository.store(query.getIdentifier(), new QueryCallbackWrapper(destination.getConnectionEndpoint(Address.class).orElse(this.channel.address()), query, callback));
        this.channel.send(this.resolveAddress(destination), new JGroupsQueryDispatchMessage(query, this.serializer, true));
    }

    protected Address resolveAddress(QueryMember destination) {
        return destination.getConnectionEndpoint(Address.class).orElseThrow(
                () -> new QueryBusConnectorCommunicationException("The target member doesn't expose a JGroups endpoint"));
    }

    public Set<QueryMember> findDestination(QueryMessage<?, ?> message) {
        String routingKey = this.routingStrategy.getRoutingKey(message);
        return (this.consistentHash.get()).getMembers(routingKey, message);
    }

    public Registration registerHandlerInterceptor(MessageHandlerInterceptor<? super QueryMessage<?, ?>> handlerInterceptor) {
        return this.localSegment.registerHandlerInterceptor(handlerInterceptor);
    }

    @Override
    public <R> Registration subscribe(String queryName, Type type, MessageHandler<? super QueryMessage<?, R>> handler) {
        return this.localSegment.subscribe(queryName, type, handler);
    }

    private static class VersionedMember implements QueryMember {
        private final SimpleQueryMember<Address> member;
        private final int version;

        public VersionedMember(SimpleQueryMember<Address> member, int version) {
            this.member = member;
            this.version = version;
        }

        public int order() {
            return this.version;
        }

        public String name() {
            return this.member.name();
        }

        public <T> Optional<T> getConnectionEndpoint(Class<T> protocol) {
            return this.member.getConnectionEndpoint(protocol);
        }

        public boolean local() {
            return this.member.local();
        }

        public void suspect() {
            this.member.suspect();
        }
    }

    private static final class JoinCondition {
        private final CountDownLatch joinCountDown;
        private volatile boolean success;

        private JoinCondition() {
            this.joinCountDown = new CountDownLatch(1);
        }

        public void await() throws InterruptedException {
            this.joinCountDown.await();
        }

        public void await(long timeout, TimeUnit timeUnit) throws InterruptedException, TimeoutException {
            if (!this.joinCountDown.await(timeout, timeUnit)) throw new TimeoutException();
        }

        private void markJoined() {
            this.success = true;
            this.joinCountDown.countDown();
        }

        public boolean isJoined() {
            return this.success;
        }
    }

    public static class Builder {
        private QueryBus localSegment;
        private JChannel channel;
        private String clusterName;
        private Serializer serializer;
        private QueryRoutingStrategy routingStrategy = new AnnotationQueryRoutingStrategy();
        private QueryConsistentHashChangeListener consistentHashChangeListener = QueryConsistentHashChangeListener.noOp();
        private ExecutorService executorService;

        public Builder localSegment(QueryBus localSegment) {
            BuilderUtils.assertNonNull(localSegment, "The localSegment may not be null");
            this.localSegment = localSegment;
            return this;
        }

        public Builder channel(JChannel channel) {
            BuilderUtils.assertNonNull(channel, "JChannel may not be null");
            this.channel = channel;
            return this;
        }

        public Builder clusterName(String clusterName) {
            this.assertClusterName(clusterName, "The clusterName may not be null or empty");
            this.clusterName = clusterName;
            return this;
        }

        public Builder serializer(Serializer serializer) {
            BuilderUtils.assertNonNull(serializer, "Serializer may not be null");
            this.serializer = serializer;
            return this;
        }

        public Builder routingStrategy(QueryRoutingStrategy routingStrategy) {
            BuilderUtils.assertNonNull(routingStrategy, "RoutingStrategy may not be null");
            this.routingStrategy = routingStrategy;
            return this;
        }

        public Builder consistentHashChangeListener(QueryConsistentHashChangeListener consistentHashChangeListener) {
            BuilderUtils.assertNonNull(consistentHashChangeListener, "ConsistentHashChangeListener may not be null");
            this.consistentHashChangeListener = consistentHashChangeListener;
            return this;
        }

        public Builder executorService(ExecutorService executorService) {
            this.executorService = executorService;
            return this;
        }

        public JGroupsQueryConnector build() {
            return new JGroupsQueryConnector(this);
        }

        protected void validate() {
            BuilderUtils.assertNonNull(this.localSegment, "The localSegment is a hard requirement and should be provided");
            BuilderUtils.assertNonNull(this.channel, "The JChannel is a hard requirement and should be provided");
            this.assertClusterName(this.clusterName, "The clusterName is a hard requirement and should be provided");
            BuilderUtils.assertNonNull(this.serializer, "The Serializer is a hard requirement and should be provided");
            BuilderUtils.assertNonNull(this.routingStrategy, "The RoutingStrategy is a hard requirement and should be provided");
            BuilderUtils.assertNonNull(this.consistentHashChangeListener, "The ConsistentHashChangeListener is a hard requirement and should be provided");
        }

        private void assertClusterName(String clusterName, String exceptionMessage) {
            BuilderUtils.assertThat(clusterName, name -> Objects.nonNull(name) && !"".equals(name), exceptionMessage);
        }
    }
}
