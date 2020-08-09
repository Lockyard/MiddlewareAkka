package it.polimi.middleware.server.actors;


import akka.actor.AbstractActorWithStash;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.pattern.Patterns;
import akka.util.Timeout;
import it.polimi.middleware.messages.GetMsg;
import it.polimi.middleware.messages.PutMsg;
import it.polimi.middleware.messages.ReplyGetMsg;
import it.polimi.middleware.messages.ReplyPutMsg;
import it.polimi.middleware.server.messages.DataValidationMsg;
import it.polimi.middleware.server.messages.UpdateStoreNodeStatusMsg;
import it.polimi.middleware.server.messages.ValidDataRequestMsg;
import it.polimi.middleware.server.store.ValueData;
import it.polimi.middleware.util.Logger;
import scala.Option;
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

    @Override
    public void preRestart(Throwable reason, Option<Object> message) throws Exception {
        super.preRestart(reason, message);
        getContext().become(inactive());
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
        Logger.std.log(Logger.LogLevel.VERBOSE, "["+self().path().name()+"] updated status. Leader: " + isLeader + ", last: " +isLast +
                ", previousReplica: " + previousReplica + ", nextReplica: " +nextReplica );

        getContext().become(active());
        unstashAll();
    }


    /**
     * On a get message a store node checks if has a valid ValueData. If yes, returns it.
     * If not, ask to the previous replica in the hierarchical-list to have a valid one. On return of a valid
     * valueData also update the content of the ValueData in its memory.
     * @param getMsg the get message
     */
    private void onGetMessage(GetMsg getMsg) {
        //if there is some datum is in memory at the specified key, get it
        if(data.containsKey(getMsg.getKey())) {
            ValueData vd = data.get(getMsg.getKey());
            //if it's ok wrt newness, reply and don't request the datum to the previous replica
            if(getMsg.getNewness() == vd.getNewness()) {
                sender().tell(new ReplyGetMsg(getMsg.getKey(), vd.getValue()), self());
            }
            //if message's newness is higher, this node has yet to be updated on that datum. Ask the datum to the next replica
            else if (getMsg.getNewness() > vd.getNewness()){
                askToPreviousReplicaForData(getMsg.getKey(), getMsg.getNewness());
            } //if message's newness is less than this node datum's newness, check the history of last writes and recover the
            //requested value
            else {
                checkHistoryForMessage(getMsg.getKey(), getMsg.getNewness());
            }
        }
        //if no datum is in, if the newness requested is the default one, return null content. otherwise, ask it to
        //the previous replica
        else {
            if(getMsg.getNewness() == ValueData.DEFAULT_NEWNESS) {
                sender().tell(new ReplyGetMsg(getMsg.getKey(), null), self());
            } else {
                askToPreviousReplicaForData(getMsg.getKey(), getMsg.getNewness());
            }
        }
    }



    /**
     * On put message, update the ValueData in the memory at the key specified, only if this replica is leader
     * or the sender of the putMsg was this' previous replica.
     * Then reply if it's ok according to the number of writes requested in the put message.
     * Finally send this put message to the next replica in the hierarchical list
     * @param putMsg
     */
    private void onPutMessage(PutMsg putMsg) {
        //update the old valueData with the one in the put
        if(data.containsKey(putMsg.getKey())) {
            data.get(putMsg.getKey()).updateIfNewer(putMsg.getVal(), putMsg.getNewness());
            Logger.std.log(Logger.LogLevel.DEBUG, "> [" + self().path().name() + "]: updated " + putMsg);
        } else {
            data.put(putMsg.getKey(), new ValueData(putMsg.getVal(), putMsg.getNewness()));
            Logger.std.log(Logger.LogLevel.DEBUG, "> [" + self().path().name() + "]: stored " + putMsg);
        }

        //if the putMessage allows this copy to reply (since already K-writes of the data occurred), or if this
        //is the last replica, then reply
        if (putMsg.shouldReplyAfterThisWrite() || (isLast && putMsg.requiresReply())) {
            sender().tell(new ReplyPutMsg(putMsg.getKey(), putMsg.getVal(), true), self());
        }

        //if is not the last in the hierarchy, forward to next replica
        if(!isLast) {
            nextReplica.forward(putMsg, context());
            Logger.std.log(Logger.LogLevel.VERBOSE, "> ["+self().path().name() + "]:Forwarded putMsg to next replica");
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
        Logger.std.log(Logger.LogLevel.VERBOSE, "Unknown message received by " + self().path().name() +": " + unknownMsg);
    }




    public static Props props(int hashSpacePartition, int partitionNumber, int nodeNumber, boolean isLeader, ActorRef storeManager) {
        return Props.create(StoreNode.class, hashSpacePartition, partitionNumber, nodeNumber, isLeader, storeManager);
    }


    ///Other private methods

    /**
     * Ask for a specific datum, given the key, to the previous replica.
     * Updates the datum in memory if newer and reply to the user if consistent wrt newness, otherwise will proceed
     * to recover the old datum.
     * @param key
     */
    private void askToPreviousReplicaForData(String key, long newness) {
        Logger.std.log(Logger.LogLevel.VERBOSE, "["+self().path().name()+"] requesting value with newness " +newness + " @K:" +key);
        Timeout timeout = Timeout.create(Duration.ofSeconds(TIMEOUT_ON_REQUEST_DATA));
        Future<Object> dataReplyFuture = Patterns.ask(previousReplica,
                new ValidDataRequestMsg(key), timeout);

        try {
            //TODO qol is avoid blocking and implement e.g. a pool of futures and a thread which checks it
            //TODO and clean it every [timeout] seconds. CompletableFuture seems to work except
            //TODO ask().toCompletableFuture() doesn't exist for some reason as method, although is in the doc

            ValueData newVD = (ValueData)Await.result(dataReplyFuture, timeout.duration());

            //it could still be possible, when it won't await, that the received datum is now older than one passed in the meanwhile
            data.get(key).updateIfNewer(newVD);

            if(newness == newVD.getNewness()) {
                sender().tell(new ReplyGetMsg(key, newVD.getValue()), self());
            }
            //if even now newness is not coherent, the value is lost
            else {

            }


        } catch (InterruptedException | TimeoutException | ClassCastException e) {
            e.printStackTrace();
        }
    }

    private void checkHistoryForMessage(String key, long newness) {
        //TODO implement a history which stores the last N put operations, to recover old values no more in data
    }

    ///////STATICS
    public static void setTimeoutOnRequestData(long timeout) {
        TIMEOUT_ON_REQUEST_DATA = timeout;
    }
}
