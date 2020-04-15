package it.polimi.middleware.server.actors;


import akka.actor.AbstractActorWithStash;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.polimi.middleware.messages.GetMsg;
import it.polimi.middleware.server.messages.UpdateStoreNodeStatusMsg;
import it.polimi.middleware.server.store.ValueData;

import java.util.HashMap;

public class StoreNode extends AbstractActorWithStash {

    private final int hashSpacePartition, partitionNumber, nodeNumber;

    //reference to other actors useful
    private boolean isLeader, isLast;
    private ActorRef nextReplica, previousReplica, storeManager;

    private HashMap<String, ValueData> data;

    /**
     * New StoreNode with the specified hashPartition.
     * @param hashSpacePartition indicates in how many parts the hashSpace was divided
     * @param partitionNumber indicates which part of the divided hashSpace is assigned to this node
     * @param nodeNumber identifies the node among the one with the same copies of data
     * @param isLeader reference to every node's manager, the store manager
     * @param storeManager the storeManager ActorRef, to which they refer
     */
    public StoreNode(int hashSpacePartition, int partitionNumber, int nodeNumber, boolean isLeader, ActorRef storeManager) {
        this.hashSpacePartition = hashSpacePartition;
        this.partitionNumber = partitionNumber;
        this.nodeNumber = nodeNumber;
        this.isLeader = isLeader;
        this.storeManager = storeManager;

        nextReplica = ActorRef.noSender();
        previousReplica = ActorRef.noSender();

        data = new HashMap<>();
    }

    @Override
    public Receive createReceive() {
        return inactive();
    }

    //Behaviors TODO
    //when inactive listen for messages updating its information about status and neighbors, stash all the others
    private Receive inactive() {
        return receiveBuilder()
                .match(UpdateStoreNodeStatusMsg.class, this::onUpdateStoreNodeStatusMessage)
                .matchAny(msg -> stash())
                .build();
    }

    private Receive active() {
        return receiveBuilder()
                .match(GetMsg.class, this::onGetMsg)
                .build();
    }


    private void onUpdateStoreNodeStatusMessage(UpdateStoreNodeStatusMsg msg) {
        this.isLeader = msg.isLeader();
        this.isLast = msg.isLast();
        this.previousReplica = msg.getPreviousReplica();
        this.nextReplica = msg.getNextReplica();
        if(msg.requestDataFromLeader()) {
            //TODO send a message to the leader replica asking for a copy of its data
            // msg.getLeaderReplica().tell(...);
        }
    }

    private void onGetMsg(GetMsg getMsg) {

    }

    public static Props props(int hashSpacePartition, int partitionNumber, int nodeNumber) {
        return Props.create(StoreNode.class, hashSpacePartition, partitionNumber, nodeNumber);
    }
}
