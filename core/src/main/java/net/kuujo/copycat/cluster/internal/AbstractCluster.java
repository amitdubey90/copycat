/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.cluster.internal;

import net.kuujo.copycat.EventListener;
import net.kuujo.copycat.cluster.*;
import net.kuujo.copycat.cluster.internal.coordinator.ClusterCoordinator;
import net.kuujo.copycat.cluster.internal.coordinator.MemberCoordinator;
import net.kuujo.copycat.cluster.internal.manager.ClusterManager;
import net.kuujo.copycat.cluster.internal.manager.LocalMemberManager;
import net.kuujo.copycat.cluster.internal.manager.MemberManager;
import net.kuujo.copycat.raft.RaftContext;
import net.kuujo.copycat.util.serializer.KryoSerializer;
import net.kuujo.copycat.util.serializer.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Stateful cluster.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class AbstractCluster implements ClusterManager, Observer {
  private static final String JOIN_TOPIC = "*";
  private static final long MEMBER_INFO_EXPIRE_TIME = 1000 * 60;

  private final Logger LOGGER = LoggerFactory.getLogger(getClass());
  protected final int id;
  protected final ClusterCoordinator coordinator;
  protected final Serializer serializer;
  protected final ScheduledExecutorService executor;
  protected final Executor userExecutor;
  private final Serializer internalSerializer = new KryoSerializer();
  private Thread thread;
  private CoordinatedLocalMember localMember;
  private final CoordinatedMembers members;
  private final Map<String, MemberInfo> membersInfo = new HashMap<>();
  private final Router router;
  private final RaftContext context;
  private final Set<EventListener<MembershipEvent>> membershipListeners = new CopyOnWriteArraySet<>();
  @SuppressWarnings("rawtypes")
  private final Map<String, MessageHandler> broadcastHandlers = new ConcurrentHashMap<>(1024);
  @SuppressWarnings("rawtypes")
  private final Map<String, Set<EventListener>> broadcastListeners = new ConcurrentHashMap<>(1024);
  private final Set<EventListener<ElectionEvent>> electionListeners = new CopyOnWriteArraySet<>();
  private String electedLeader;
  private ScheduledFuture<?> gossipTimer;
  private final Random random = new Random();

  protected AbstractCluster(int id, ClusterCoordinator coordinator, RaftContext context, Router router, Serializer serializer, ScheduledExecutorService executor, Executor userExecutor) {
    this.id = id;
    this.coordinator = coordinator;
    this.serializer = serializer;
    this.executor = executor;
    this.userExecutor = userExecutor;

    // Always create a local member based on the local member URI.
    MemberInfo localMemberInfo = new MemberInfo(coordinator.member().uri(), context.getActiveMembers().contains(coordinator.member().uri()) ? Member.Type.ACTIVE : Member.Type.PASSIVE, Member.Status.ALIVE);
    this.localMember = new CoordinatedLocalMember(id, localMemberInfo, coordinator.member(), serializer, executor);
    membersInfo.put(localMemberInfo.uri(), localMemberInfo);

    // Create a map of coordinated members based on the context's listed replicas. Additional members will be added
    // only via the gossip protocol.
    Map<String, CoordinatedMember> members = new ConcurrentHashMap<>();
    members.put(localMember.uri(), localMember);
    for (String replica : context.getActiveMembers()) {
      if (!replica.equals(localMember.uri())) {
        MemberCoordinator memberCoordinator = coordinator.member(replica);
        if (memberCoordinator != null) {
          members.put(replica, new CoordinatedMember(id, new MemberInfo(replica, Member.Type.ACTIVE, Member.Status.ALIVE), memberCoordinator, serializer, executor));
        } else {
          throw new ClusterException("Invalid replica " + replica);
        }
      }
    }
    this.members = new CoordinatedMembers(members, this);
    this.router = router;
    this.context = context;
    try {
      executor.submit(() -> this.thread = Thread.currentThread()).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new ClusterException(e);
    }
  }

  @Override
  public synchronized void update(Observable o, Object arg) {
    RaftContext context = (RaftContext) o;
    if (context.getLeader() != null && (electedLeader == null || !electedLeader.equals(context.getLeader()))) {
      electedLeader = context.getLeader();
      Member member = member(context.getLeader());
      if (member != null) {
        ElectionEvent event = new ElectionEvent(ElectionEvent.Type.COMPLETE, context.getTerm(), member);
        for (EventListener<ElectionEvent> listener : electionListeners) {
          listener.accept(event);
        }
      }
    }
  }

  /**
   * Checks that cluster logic runs on the correct thread.
   */
  private void checkThread() {
    if (Thread.currentThread() != thread) {
      throw new IllegalStateException("Cluster not running on the correct thread");
    }
  }

  /**
   * Sends member join requests.
   */
  private void sendJoins() {
    sendJoins(getGossipMembers());
  }

  /**
   * Sends member join requests to the given set of members.
   */
  private void sendJoins(Collection<CoordinatedMember> gossipMembers) {
    checkThread();

    // Increment the local member version.
    localMember.info().version(localMember.info().version() + 1);

    // For a random set of three members, send all member info.
    Collection<MemberInfo> members = new ArrayList<>(membersInfo.values());
    for (CoordinatedMember member : gossipMembers) {
      if (!member.uri().equals(member().uri())) {
        member.<Collection<MemberInfo>, Collection<MemberInfo>>send(JOIN_TOPIC, id, members, internalSerializer, executor).whenCompleteAsync((membersInfo, error) -> {
          // If the response was successfully received then indicate that the member is alive and update all member info.
          // Otherwise, indicate that communication with the member failed. This information will be used to determine
          // whether the member should be considered dead by informing other members that it appears unreachable.
          checkThread();
          if (isOpen()) {
            if (error == null) {
              member.info().succeed();
              updateMemberInfo(membersInfo);
            } else {
              member.info().fail(localMember.uri());
            }
          }
        }, executor);
      }
    }
  }

  /**
   * Receives member join requests.
   */
  private CompletableFuture<Collection<MemberInfo>> handleJoin(Collection<MemberInfo> members) {
    checkThread();
    // Increment the local member version.
    localMember.info().version(localMember.info().version() + 1);
    updateMemberInfo(members);
    return CompletableFuture.completedFuture(new ArrayList<>(membersInfo.values()));
  }

  /**
   * Updates member info for all members.
   */
  private void updateMemberInfo(Collection<MemberInfo> membersInfo) {
    checkThread();

    // Iterate through the member info and use it to update local member information.
    membersInfo.forEach(memberInfo -> {

      // If member info for the given URI is already present, update the member info based on versioning. Otherwise,
      // if the member info isn't already present then add it.
      MemberInfo info = this.membersInfo.get(memberInfo.uri());
      if (info == null) {
        info = memberInfo;
        this.membersInfo.put(memberInfo.uri(), memberInfo);
      } else {
        info.update(memberInfo);
      }

      // Check whether the member info update should result in any member clients being added to or removed from the
      // cluster. If the updated member state is ALIVE or SUSPICIOUS, make sure the member client is open in the cluster.
      // Otherwise, if the updated member state is DEAD then make sure it has been removed from the cluster.
      final MemberInfo updatedInfo = info;
      if (updatedInfo.state() == Member.Status.ALIVE || updatedInfo.state() == Member.Status.SUSPICIOUS) {
        synchronized (members.members) {
          if (!members.members.containsKey(updatedInfo.uri())) {
            CoordinatedMember member = createMember(updatedInfo);
            if (member != null) {
              members.members.put(member.uri(), member);
              context.addMember(member.uri());
              LOGGER.info("{} - {} joined the cluster", context.getLocalMember(), member.uri());
              membershipListeners.forEach(listener -> listener.accept(new MembershipEvent(MembershipEvent.Type.JOIN, member)));
              sendJoins(members.members.values());
            }
          }
        }
      } else {
        synchronized (members.members) {
          CoordinatedMember member = members.members.remove(updatedInfo.uri());
          if (member != null) {
            context.removeMember(member.uri());
            LOGGER.info("{} - {} left the cluster", context.getLocalMember(), member.uri());
            membershipListeners.forEach(listener -> listener.accept(new MembershipEvent(MembershipEvent.Type.LEAVE, member)));
            sendJoins(members.members.values());
          }
        }
      }
    });
    cleanMemberInfo();
  }

  /**
   * Creates a coordinated cluster member.
   *
   * @param info The coordinated member info.
   * @return The coordinated member.
   */
  protected abstract CoordinatedMember createMember(MemberInfo info);

  /**
   * Cleans expired member info for members that have been dead for MEMBER_INFO_EXPIRE_TIME milliseconds.
   */
  private synchronized void cleanMemberInfo() {
    checkThread();
    Iterator<Map.Entry<String, MemberInfo>> iterator = membersInfo.entrySet().iterator();
    while (iterator.hasNext()) {
      MemberInfo info = iterator.next().getValue();
      if (info.state() == Member.Status.DEAD && System.currentTimeMillis() > info.changed() + MEMBER_INFO_EXPIRE_TIME) {
        iterator.remove();
      }
    }
  }

  /**
   * Gets a list of members with which to gossip.
   */
  private Collection<CoordinatedMember> getGossipMembers() {
    try (Stream<CoordinatedMember> membersStream = this.members.members.values().stream();
         Stream<CoordinatedMember> activeStream = membersStream.filter(member -> !member.uri().equals(localMember.uri())
           && (localMember.type() == Member.Type.ACTIVE && member.type() == Member.Type.PASSIVE)
           || (localMember.type() == Member.Type.PASSIVE && member.type() == Member.Type.ACTIVE)
           && (member.state() == Member.Status.SUSPICIOUS || member.state() == Member.Status.ALIVE))) {

      List<CoordinatedMember> activeMembers = activeStream.collect(Collectors.toList());

      // Create a random list of three active members.
      Collection<CoordinatedMember> randomMembers = new HashSet<>(3);
      for (int i = 0; i < Math.min(activeMembers.size(), 3); i++) {
        randomMembers.add(activeMembers.get(random.nextInt(Math.min(activeMembers.size(), 3))));
      }
      return randomMembers;
    }
  }

  @Override
  public MemberManager leader() {
    return context.getLeader() != null ? member(context.getLeader()) : null;
  }

  @Override
  public long term() {
    return context.getTerm();
  }

  @Override
  public MemberManager member(String uri) {
    return members.members.get(uri);
  }

  @Override
  public LocalMemberManager member() {
    return localMember;
  }

  @Override
  public Members members() {
    return members;
  }

  @Override
  public synchronized <T> Cluster broadcast(String topic, T message) {
    for (Member member : members) {
      member.send(topic, message);
    }
    return this;
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public synchronized <T> Cluster addBroadcastListener(String topic, EventListener<T> listener) {
    broadcastListeners.computeIfAbsent(topic, t -> new CopyOnWriteArraySet<>()).add(listener);
    if (!broadcastHandlers.containsKey(topic)) {
      MessageHandler<T, Void> handler = message -> {
        broadcastListeners.get(topic).forEach(l -> l.accept(message));
        return CompletableFuture.completedFuture(null);
      };
      broadcastHandlers.put(topic, handler);
      member().registerHandler(topic, handler);
    }
    return this;
  }

  @Override
  @SuppressWarnings("rawtypes")
  public synchronized <T> Cluster removeBroadcastListener(String topic, EventListener<T> listener) {
    Set<EventListener> listeners = broadcastListeners.get(topic);
    if (listeners != null) {
      listeners.remove(listener);
      if (listeners.isEmpty()) {
        broadcastListeners.remove(topic);
        broadcastHandlers.remove(topic);
        member().unregisterHandler(topic);
      }
    }
    return this;
  }

  @Override
  public Cluster addMembershipListener(EventListener<MembershipEvent> listener) {
    membershipListeners.add(listener);
    return this;
  }

  @Override
  public Cluster removeMembershipListener(EventListener<MembershipEvent> listener) {
    membershipListeners.remove(listener);
    return this;
  }

  @Override
  public Cluster addElectionListener(EventListener<ElectionEvent> listener) {
    electionListeners.add(listener);
    return this;
  }

  @Override
  public Cluster removeElectionListener(EventListener<ElectionEvent> listener) {
    electionListeners.remove(listener);
    return this;
  }

  @Override
  public synchronized CompletableFuture<ClusterManager> open() {
    return CompletableFuture.runAsync(() -> {
      router.createRoutes(this, context);
      context.addObserver(this);
    }, executor)
      .thenCompose(v -> localMember.open())
      .thenRun(() -> localMember.registerHandler(JOIN_TOPIC, id, this::handleJoin, internalSerializer, executor))
      .thenRun(() -> {
        gossipTimer = executor.scheduleAtFixedRate(this::sendJoins, 0, 1, TimeUnit.SECONDS);
      }).thenApply(m -> this);
  }

  @Override
  public boolean isOpen() {
    return localMember.isOpen();
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    localMember.close();
    router.destroyRoutes(this, context);
    context.deleteObserver(this);
    localMember.unregisterHandler(JOIN_TOPIC, id);
    if (gossipTimer != null) {
      gossipTimer.cancel(false);
      gossipTimer = null;
    }
    return localMember.close();
  }

  @Override
  public boolean isClosed() {
    return localMember.isClosed();
  }

  @Override
  public String toString() {
    return String.format("%s[members=%s]", getClass().getSimpleName(), members());
  }

}
