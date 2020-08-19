package it.polimi.middleware.server.actors;


import akka.actor.AbstractActorWithStash;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.polimi.middleware.messages.*;
import it.polimi.middleware.server.messages.*;
import it.polimi.middleware.server.store.ValueData;
import it.polimi.middleware.util.Logger;
import scala.Option;

import static akka.pattern.Patterns.ask;
import static akka.pattern.Patterns.pipe;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;


//TODO qol general for this class is non-block some message such as validData and DataValidationRequest/Reply
public class StoreNode extends AbstractActorWithStash {

    private Duration timeout;
    /**
     * In seconds, the timeout for when requesting data to other StoreNodes. Default is 5
     */
    private static long TIMEOUT_ON_REQUEST_DATA = 5;

    private int hashSpacePartition;

    private ActorRef storeManager;

    /**
     * The main data contained inside this StoreNode
     */
    private HashMap<String, ValueData> data;

    /**
     * This "map" states, for each partition, which nodes have data of that partition, and in which order
     * they are authoritative. Hence is possible to get who's leader and who is the last to be updated
     * Every node has the same map, which is updated uniquely by the store manager on events such
     * new nodes up and nodes down. Index of list is the "key", indicating the partition
     */
    private List<List<ActorRef>> nodesOfPartition;

    /**
     * Set of all the storeNodes working in the store system now
     */
    private Set<ActorRef> storeNodesSet;

    /**
     * Client Operation ID map. Key is the client ID, as value for each client there is a map
     * whose key are integer representing a partition, and for each partition as value there is the
     * logical ID of the last operation done by that client on that partition.
     */
    private HashMap<Long, HashMap<Integer, Integer>> clientOpIDMap;

    /**
     * The set of the partitions assigned to this node
     */
    private Set<Integer> assignedPartitions;

    /**
     * the set of all assigned clients to this node
     */
    private Set<Long> assignedClientIDs;

    //round robin index
    private int rrIndex=0;


    /**
     * New StoreNode with the specified hashPartition.
     * @param storeManager the storeManager ActorRef, to which they refer
     */
    public StoreNode(ActorRef storeManager) {
        this.storeManager = storeManager;

        data = new HashMap<>();
        nodesOfPartition = new ArrayList<>();
        clientOpIDMap = new HashMap<>();
        assignedPartitions = new HashSet<>();
        storeNodesSet = new HashSet<>();
        assignedClientIDs = new HashSet<>();

        timeout = Duration.ofSeconds(TIMEOUT_ON_REQUEST_DATA);
    }

    /**
     * New StoreNode with the specified hashPartition.
     * @param storeManager the storeManager ActorRef, to which they refer
     * @param timeoutSeconds the timeout in seconds when asking to other nodes
     */
    public StoreNode(ActorRef storeManager, long timeoutSeconds) {
        this.storeManager = storeManager;

        data = new HashMap<>();
        nodesOfPartition = new ArrayList<>();
        clientOpIDMap = new HashMap<>();
        assignedPartitions = new HashSet<>();
        storeNodesSet = new HashSet<>();
        assignedClientIDs = new HashSet<>();

        timeout = Duration.ofSeconds(timeoutSeconds);
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
                .match(ActivateNodeMsg.class, this::onActivateNodeMessage)
                .matchAny(msg -> stash())
                .build();
    }

    private Receive active() {
        return receiveBuilder()
                .match(GetMsg.class, this::onGetMessage)
                .match(PutMsg.class, this::onPutMessage)
                .match(ClientAssignMsg.class, this::onClientAssignMessage)
                .matchAny(this::onUnknownMessage)
                .build();
    }


    /**
     * Update the store node with the update message
     * @param msg the update message
     */
    private void onUpdateStoreNodeStatusMessage(UpdateStoreNodeStatusMsg msg) {
        //TODO

    }


    private void onActivateNodeMessage(ActivateNodeMsg msg) {
        Logger.std.dlog(self().path().name() + " received as nodesOfPartitions: " + msg.getNodesOfPartition()
                + "\nsize: " +msg.getNodesOfPartition().size());
        hashSpacePartition = msg.getNodesOfPartition().size();
        nodesOfPartition = msg.getNodesOfPartition();

        //add all the actors in the lists to the set, and all the partitions assigned to this node
        for (int i = 0; i < nodesOfPartition.size(); i++) {
            storeNodesSet.addAll(nodesOfPartition.get(i));
            if(nodesOfPartition.get(i).contains(self()))
                assignedPartitions.add(i);
        }
        Logger.std.dlog(self().path().name() + " received partitions: " + assignedPartitions);
        Logger.std.dlog("Final hashmap for node " +self().path().name()+":\n" + toStringNodesOfPartition());


        if(msg.mustRequestData()) {
            //TODO
        }

        getContext().become(active());
        unstashAll();
    }


    /**
     * Get Messages comes from clients. check if id corresponds to one assigned to this node, and if ok get the datum.
     * If datum is not here or there are consistency issues, ask to other nodes
     * @param getMsg the get message from the client
     */
    private void onGetMessage(GetMsg getMsg) {
        Logger.std.dlog(self().path().name() + " received get message with key " + getMsg.getKey() +
                " from " +sender().path().name());
        //check client id or if request comes from another node in the system
        if(assignedClientIDs.contains(getMsg.getClientID()) || storeNodesSet.contains(sender())) {
            int partition = getMsg.getKey().hashCode() % hashSpacePartition;
            //if datum is assigned to this replica
            if(assignedPartitions.contains(partition)) {
                //if there is a value, get it
                if(data.containsKey(getMsg.getKey())) {
                    ValueData vd = data.get(getMsg.getKey());
                    sender().tell(new ReplyGetMsg(getMsg.getKey(), vd.getValue()), self());
                }
                //there is no datum, return null
                else {
                    sender().tell(new ReplyGetMsg(getMsg.getKey(), null), self());
                }
            }
            // if datum is  not assigned to this replica, ask to another replica to which has it assigned
            else {
                List<ActorRef> nodesOfThisPartition = nodesOfPartition.get(partition);
                roundRobin(nodesOfThisPartition.size());
                //ask and pipe the answer
                CompletableFuture<Object> future = ask(nodesOfThisPartition.get(rrIndex), getMsg, timeout).toCompletableFuture();
                pipe(future, getContext().dispatcher()).to(sender());
            }
        }
        //
        else {
            sender().tell(new ReplyErrorMsg("The client requesting the operation has not authorization" +
                    " on this node"), self());
        }
        //if there is some datum is in memory at the specified key, get it
        if(data.containsKey(getMsg.getKey())) {
            ValueData vd = data.get(getMsg.getKey());
        }
        else {

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
        Logger.std.dlog(self().path().name() + ": put message received with key: " + putMsg.getKey() + "" +
                ", val: " +putMsg.getVal() + " from " +sender().path().name());

        //if is a legit request: from client assigned to this node or another node of the system
        if(assignedClientIDs.contains(putMsg.getClientID()) || storeNodesSet.contains(sender())) {

            int partition = putMsg.getKey().hashCode() % hashSpacePartition;
            List<ActorRef> nodesOfKey = nodesOfPartition.get(partition);

            //if this node have assigned the partition of that key
            if(nodesOfKey.contains(self())) {

                //if this is the leader of the partition of that datum, write and propagate
                if(nodesOfKey.get(0).equals(self())) {
                    insertData(putMsg);
                    if(nodesOfKey.size() >= 2) {
                        CompletableFuture<Object> future = ask(nodesOfKey.get(1), putMsg, timeout.multipliedBy(nodesOfKey.size()-1)).toCompletableFuture();
                        pipe(future, getContext().dispatcher()).to(sender());
                    }
                }

                //if is not leader but the put comes from the leader, then update data, and forward if there are
                //replicas after this one, or reply to the leader if is the last replica
                else if(nodesOfKey.get(0).equals(sender())) {
                    Logger.std.dlog(self().path().name() + "is not leader, put request from leader");
                    //update data
                    insertData(putMsg);

                    //if is last node, reply to the leader
                    if(nodesOfKey.get(nodesOfKey.size()-1).equals(self())) {
                        sender().tell(new ReplyPutMsg(putMsg.getKey(), putMsg.getVal(), true), self());
                    } //if is not the last node, then forward the leader's request to the next
                    else {
                        nodesOfKey.get(nodesOfKey.indexOf(self())+1).forward(putMsg, getContext());
                    }

                }
                //then this is a client access point but this node has no leadership on that key. ask to the leader
                else {
                    CompletableFuture<Object> future = ask(nodesOfKey.get(0), putMsg, timeout.multipliedBy(nodesOfKey.size())).toCompletableFuture();
                    pipe(future, getContext().dispatcher()).to(sender());
                }
            }
            //if the node doesn't have this key assigned, ask to the leader of the key to perform a put
             else {
                CompletableFuture<Object> future = ask(nodesOfKey.get(0), putMsg, timeout.multipliedBy(nodesOfKey.size())).toCompletableFuture();
                pipe(future, getContext().dispatcher()).to(sender());
            }
        }
        //else an actor with no rights tried to do a put on this node. answer error
        else {
            sender().tell(new ReplyErrorMsg("The client requesting the operation has not authorization" +
                    " on this node"), self());
        }
        
    }

    private void onClientAssignMessage(ClientAssignMsg msg) {
        assignedClientIDs.add(msg.getClientID());
        sender().tell(new GreetingReplyMsg(self(), msg.getNodesAssigned(), msg.getClientID()), self());
    }


    private void onUnknownMessage(Object unknownMsg) {
        //simply log the unknown message
        Logger.std.log(Logger.LogLevel.VERBOSE, "Unknown message received by " + self().path().name() +": " + unknownMsg);
    }




    public static Props props(ActorRef storeManager, long timeoutSeconds) {
        return Props.create(StoreNode.class, storeManager, timeoutSeconds);
    }


    ///Other private methods

    private void insertData(PutMsg putMsg) {
        data.put(putMsg.getKey(), new ValueData(putMsg.getVal()));
    }

    private String toStringNodesOfPartition() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < nodesOfPartition.size(); i++) {
            sb.append("P:" + i + " - [");
            for (int j = 0; j < nodesOfPartition.get(i).size(); j++) {
                sb.append(nodesOfPartition.get(i).get(j).path().name());
                if(j != nodesOfPartition.get(i).size()-1)
                    sb.append(", ");
                else
                    sb.append(("]\n"));
            }
        }
        return sb.toString();
    }

    private void checkHistoryForMessage(String key, long newness) {
        //TODO implement a history which stores the last N put operations, to recover old values no more in data
    }

    /**
     * Do a round robin in modulo mod
     * @param mod the modulo
     */
    private void roundRobin(int mod) {
        rrIndex = (rrIndex+1) % mod;
    }

    public void setTimeout(int seconds) {
        timeout = Duration.ofSeconds(seconds);
    }

    ///////STATICS
    public static void setTimeoutOnRequestData(long timeout) {
        TIMEOUT_ON_REQUEST_DATA = timeout;
    }
}
