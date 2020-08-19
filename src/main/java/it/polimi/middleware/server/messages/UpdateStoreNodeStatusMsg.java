package it.polimi.middleware.server.messages;

import akka.actor.ActorRef;

/**
 * Used to update the status of a StoreNode wrt the "copy hierarchy", meaning the fact that the node could become a
 * leader or update its position in the copy order
 * Stores 2 ActorRefs, related to the next and previous neighbours in a list.
 * Also stores the information of which is the leader replica, used if is requested to copy the data from the leader
 * replica
 */
public class UpdateStoreNodeStatusMsg extends ServerMessage {

    private static final long serialVersionUID = 88124090000123206L;

    private final ActorRef previousReplica, nextReplica, leaderReplica;
    private final boolean isLeader, isLast, requestDataFromLeader;


    /**
     * Update the status of a StoreNode, specifying its neighbors, if is the leader and/or the last of the replicas,
     * and get a reference of the leader replica, useful to get a copy of its
     * @param previousReplica the replica coming before this one in the replicas' list (ActorRef.noSender() if no one is)
     * @param nextReplica the replica coming after this one in the replicas' list (ActorRef.noSender() if no one is)
     * @param isLeader true if this is the leader replica. If true this shouldn't have a previousReplica, since is the
     *                 most important replica
     * @param isLast true if this is the last replica in the list. If true this shouldn't have a nextReplica, since is
     *               the least important replica
     * @param requestDataFromLeader If true the node will override its data with the one it will receive by asking
     *                              to the leader replica
     * @param leaderReplica the ref to the leader replica
     */
    public UpdateStoreNodeStatusMsg(ActorRef previousReplica, ActorRef nextReplica, boolean isLeader, boolean isLast, boolean requestDataFromLeader, ActorRef leaderReplica) {
        this.previousReplica = previousReplica;
        this.nextReplica = nextReplica;
        this.isLeader = isLeader;
        this.isLast = isLast;
        this.requestDataFromLeader = requestDataFromLeader;
        this.leaderReplica = leaderReplica;
    }

    public ActorRef getNextReplica() {
        return nextReplica;
    }

    public ActorRef getPreviousReplica() {
        return previousReplica;
    }

    public boolean isLeader() {
        return isLeader;
    }

    public boolean isLast() {
        return isLast;
    }

    public ActorRef getLeaderReplica() {
        return leaderReplica;
    }

    public boolean requestDataFromLeader() {
        return requestDataFromLeader;
    }
}
