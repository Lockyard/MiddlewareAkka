package it.polimi.middleware.server.actors;


import akka.actor.AbstractActorWithStash;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.pattern.Patterns;
import akka.util.Timeout;
import it.polimi.middleware.messages.GetMsg;
import it.polimi.middleware.messages.PutMsg;
import it.polimi.middleware.messages.ReplyGetMsg;
import it.polimi.middleware.server.messages.DataValidationMsg;
import it.polimi.middleware.server.messages.UpdateStoreNodeStatusMsg;
import it.polimi.middleware.server.messages.ValidDataRequestMsg;
import it.polimi.middleware.server.store.ValueData;
import scala.concurrent.Await;
import scala.concurrent.Future;

import java.time.Duration;
import java.util.HashMap;
import java.util.concurrent.TimeoutException;


//TODO qol general for this class is non-block some message such as validData and DataValidationRequest/Reply
public class StoreNode extends AbstractActorWithStash {

    /**
     * In seconds, the timeout for when requesting data to other StoreNodes. Default is 3
     */
    private static long TIMEOUT_ON_REQUEST_DATA = 3;

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
                .match(GetMsg.class, this::onGetMessage)
                .match(PutMsg.class, this::onPutMessage)
                .match(DataValidationMsg.class, this::onDataValidationMessage)
                .matchAny(this::onUnknownMessage)
                .build();
    }


    /**
     * Update the store node with the update message, then this node becomes active
     * @param msg the update message
     */
    private void onUpdateStoreNodeStatusMessage(UpdateStoreNodeStatusMsg msg) {
        this.isLeader = msg.isLeader();
        this.isLast = msg.isLast();
        this.previousReplica = msg.getPreviousReplica();
        this.nextReplica = msg.getNextReplica();
        if(msg.requestDataFromLeader()) {
            //TODO send a message to the leader replica asking for a copy of its data
            // msg.getLeaderReplica().tell(...);
        }
        getContext().become(active());
    }


    /**
     * On a get message a store node checks if has a valid ValueData. If yes, returns it.
     * If not, ask to the previous replica in the hierarchical-list to have a valid one. On return of a valid
     * valueData also update the content of the ValueData in its memory.
     * @param getMsg
     */
    private void onGetMessage(GetMsg getMsg) {

        //TODO check if data.get can return null
        ValueData vd = data.get(getMsg.getKey());
        //if requested data is of the same newness in this replica then return it. Otherwise ask it to the previous replica
        if(vd.getNewness() == getMsg.getNewness()) {
            sender().tell(new ReplyGetMsg(vd.getValue()), self());
        }
        //if requested data is not valid, ask to the previous replica for it, and also update this valueData in the process
        else {
            Timeout timeout = Timeout.create(Duration.ofSeconds(TIMEOUT_ON_REQUEST_DATA));
            Future<Object> dataReplyFuture = Patterns.ask(previousReplica,
                    new ValidDataRequestMsg(getMsg.getKey()), timeout);

            try {
                //TODO qol is avoid blocking and implement e.g. a pool of futures and a thread which checks it
                //TODO and clean it every [timeout] seconds. CompletableFuture seems to work except
                //TODO ask().toCompletableFuture() doesn't exist for some reason as method, although is in the doc
                ValueData newVD = (ValueData)Await.result(dataReplyFuture, timeout.duration());

                data.get(getMsg.getKey()).update(newVD);

                sender().tell(new ReplyGetMsg(vd.getValue()), self());

            } catch (InterruptedException | TimeoutException | ClassCastException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * On put message, update the ValueData in the memory at the key specified.
     * if this replica is leader, then the new ValueData is considered valid, otherwise is set to invalid
     * @param putMsg
     */
    private void onPutMessage(PutMsg putMsg) {
        //if is leader or if it comes from its previous replica update the old valueData with the one in the put
        if(isLeader || sender().compareTo(previousReplica) == 0)
            data.get(putMsg.getKey()).updateValueDataIfNewer(putMsg.getVal(), putMsg.getNewness());

        //if is not the last in the hierarchy, forward the message with itself as sender
        if(!isLast) {
            nextReplica.tell(putMsg, self());
        }
    }

    /**
     * The replica listen only for its previous replica.
     * If that is the sender, update and validate the data it sent
     * @param dvMsg the message containing the ValueData to update
     */
    private void onDataValidationMessage(DataValidationMsg dvMsg) {

    }


    private void onUnknownMessage(Object unknownMsg) {
        //simply log the unknown message
        System.out.println("Unkown message received by " + self().path().name() +": " + unknownMsg);
    }




    public static Props props(int hashSpacePartition, int partitionNumber, int nodeNumber) {
        return Props.create(StoreNode.class, hashSpacePartition, partitionNumber, nodeNumber);
    }

    ///////STATICS
    public static void setTimeoutOnRequestData(long timeout) {
        TIMEOUT_ON_REQUEST_DATA = timeout;
    }
}
