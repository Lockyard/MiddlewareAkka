package it.polimi.middleware.server.actors;


import akka.actor.AbstractActorWithStash;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
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
    private final Cluster cluster = Cluster.get(getContext().system());

    private int nodeNumber = -1;

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
    private HashMap<Integer, HashMap<String, ValueData>> dataPerPartition;

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

    private Set<ActorRef> unreachableNodes;

    /**
     * Client Operation ID map. Key is the client ID, as value for each client there is a map
     * whose key are integer representing a partition, and for each partition as value there is the
     * logical ID of the last operation done by that client on that partition.
     */
    private Map<Long, Map<Integer, Integer>> clientOpIDMap;

    /**
     * The set of the partitions assigned to this node
     */
    private final Set<Integer> assignedPartitions;

    /**
     * the set of all assigned clients to this node
     */
    private final Set<Long> assignedClientIDs;

    //round robin index
    private int rrIndex=0;


    /**
     * New StoreNode with the specified hashPartition.
     * @param storeManager the storeManager ActorRef, to which they refer
     */
    public StoreNode(ActorRef storeManager) {
        this.storeManager = storeManager;

        dataPerPartition = new HashMap<>();
        nodesOfPartition = new ArrayList<>();
        clientOpIDMap = new HashMap<>();
        assignedPartitions = new HashSet<>();
        storeNodesSet = new HashSet<>();
        assignedClientIDs = new HashSet<>();
        unreachableNodes = new HashSet<>();

        timeout = Duration.ofSeconds(TIMEOUT_ON_REQUEST_DATA);
    }

    /**
     * New StoreNode with the specified hashPartition.
     * @param timeoutSeconds the timeout in seconds when asking to other nodes
     */
    public StoreNode(long timeoutSeconds) {

        dataPerPartition = new HashMap<>();
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

    // Subscribe to cluster
    @Override
    public void preStart() {
        cluster.subscribe(self(), ClusterEvent.initialStateAsEvents(), ClusterEvent.MemberEvent.class, ClusterEvent.MemberUp.class);
        //cluster.join(cluster.selfAddress());
    }

    @Override
    public void preRestart(Throwable reason, Option<Object> message) throws Exception {
        super.preRestart(reason, message);
        cluster.subscribe(self(), ClusterEvent.initialStateAsEvents(), ClusterEvent.MemberUp.class,
                ClusterEvent.UnreachableMember.class, ClusterEvent.MemberEvent.class, ClusterEvent.ReachableMember.class);
        getContext().become(inactive());
    }

    // Re-subscribe when restart
    @Override
    public void postStop() {
        cluster.unsubscribe(self());
    }

    //Behaviors TODO
    //when inactive listen for messages updating its information about status and neighbors, stash all the others
    private Receive inactive() {
        return receiveBuilder()
                .match(GrantAccessToStoreMsg.class, this::onGrantAccessToStoreMsg)
                .match(ActivateNodeMsg.class, this::onActivateNodeMessage)
                .match(ClusterEvent.MemberEvent.class, msg -> {Logger.std.dlog("Node" + nodeNumber + " received message " +msg);})
                .matchAny(msg -> stash())
                .build();
    }

    private Receive active() {
        return receiveBuilder()
                .match(GetMsg.class, this::tryOnGetMessage)
                .match(PutMsg.class, this::tryOnPutMessage)
                .match(UpdateStoreNodeStatusMsg.class, this::onUpdateStoreNodeStatusMessage)
                .match(ClientAssignMsg.class, this::onClientAssignMessage)
                .match(ClusterEvent.UnreachableMember.class, this::onUnreachableMember)
                .match(ClusterEvent.ReachableMember.class, this::onReachableMember)
                .match(ClusterEvent.MemberEvent.class, msg -> {Logger.std.dlog("Node" + nodeNumber + " received message " +msg);})
                .matchAny(this::onUnknownMessage)
                .build();
    }


    private void onReachableMember(ClusterEvent.ReachableMember rm) {
        Logger.std.dlog("Detected reachable member " + rm.member().address());
        if(rm.member().hasRole("storenode")) {
            for (ActorRef node :
                    storeNodesSet) {
                if(node.path().address().equals(rm.member().address())) {
                    unreachableNodes.remove(node);
                }
            }
        }
    }

    private void onUnreachableMember(ClusterEvent.UnreachableMember um) {
        Logger.std.dlog("Detected unreachable member " +um.member().address());
        if(um.member().hasRole("storenode")) {
            for (ActorRef node :
                    storeNodesSet) {
                if(node.path().address().equals(um.member().address())) {
                    unreachableNodes.add(node);
                }
            }
        }
    }


    private void onGrantAccessToStoreMsg(GrantAccessToStoreMsg msg) {
        storeManager = msg.getStoreManagerRef();
        nodeNumber = msg.getNodeNumber();
        if(msg.mustRequestActivation())
            storeManager.tell(new RequestActivateMsg(self()), self());
    }


    /**
     * Update the store node with the update message
     * @param msg the update message
     */
    private void onUpdateStoreNodeStatusMessage(UpdateStoreNodeStatusMsg msg) {
        if(msg.getNodesOfPartition().size() != hashSpacePartition) {
            //TODO if dynamic partitions will be implemented
            Logger.std.dlog("[WARNING] Partitions number is changed! Old is " +hashSpacePartition + ", new: " +
                    msg.getNodesOfPartition().size() +". This case is not yet managed!");
        }

        //re-write the set of nodes in the system
        storeNodesSet = new HashSet<>();

        nodesOfPartition = msg.getNodesOfPartition();

        for (int i = 0; i < msg.getNodesOfPartition().size(); i++) {
            //if this node is assigned to a partition to which wasn't assigned before, add that partition
            if(msg.getNodesOfPartition().get(i).contains(self()) && !assignedPartitions.contains(i)) {
                Logger.std.ilog("Node"+nodeNumber+" adding partition " + i);
                assignNewPartition(i);
            }
            //else if this node is no longer assigned to a partition, remove it
            else if (!msg.getNodesOfPartition().get(i).contains(self()) && assignedPartitions.contains(i)) {
                Logger.std.ilog("Node" +nodeNumber+ " removing partition " + i);
                removePartitionAssignment(i);
            }

            //for each partition, assign to the new set of nodes all the nodes
            storeNodesSet.addAll(msg.getNodesOfPartition().get(i));
        }

        Logger.std.ilog("Node"+nodeNumber+"'s new partitions: " +assignedPartitions +
                "\n Full partition: " + toStringNodesOfPartition());

    }


    private void onActivateNodeMessage(ActivateNodeMsg msg) {
        Logger.std.dlog("Node" +nodeNumber+ " received as nodesOfPartitions: " + msg.getNodesOfPartition()
                + "\nsize: " +msg.getNodesOfPartition().size());
        hashSpacePartition = msg.getNodesOfPartition().size();
        nodesOfPartition = msg.getNodesOfPartition();

        //add all the actors in the lists to the set, and all the partitions assigned to this node
        for (int i = 0; i < nodesOfPartition.size(); i++) {
            storeNodesSet.addAll(nodesOfPartition.get(i));
            if(nodesOfPartition.get(i).contains(self()))
                assignedPartitions.add(i);
        }

        for (Integer assignedPartition :
                assignedPartitions) {
            dataPerPartition.put(assignedPartition, new HashMap<>());
        }

        Logger.std.dlog("Node" +nodeNumber+ " received partitions: " + assignedPartitions);
        Logger.std.dlog("Final hashmap for Node" +nodeNumber+":\n" + toStringNodesOfPartition());


        if(msg.mustRequestData()) {
            //TODO
        }

        getContext().become(active());
        unstashAll();
    }

    private void tryOnGetMessage(GetMsg msg) {
        try {
            onGetMessage(msg);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * Get Messages comes from clients. check if id corresponds to one assigned to this node, and if ok get the datum.
     * If datum is not here or there are consistency issues, ask to other nodes
     * @param getMsg the get message from the client
     */
    private void onGetMessage(GetMsg getMsg) {
        Logger.std.dlog("Node" +nodeNumber+ " received get message with key " + getMsg.getKey() +
                " from " +sender().path().name());
        //check client id or if request comes from another node in the system
        if(assignedClientIDs.contains(getMsg.getClientID()) || storeNodesSet.contains(getMsg.sender())) {
            int partition = partitionOf(getMsg.getKey());
            //if datum is assigned to this replica
            if(dataPerPartition.containsKey(partition)) {
                //if there is a value, get it
                getMsg.sender().tell(new ReplyGetMsg(getMsg.getKey(),
                        getDataValue(getMsg.getKey(), partition)), self());
            }
            // if datum is  not assigned to this replica, ask to another replica to which has it assigned
            else {

                List<ActorRef> nodesOfThisPartition = nodesOfPartition.get(partition);
                roundRobin(nodesOfThisPartition.size());

                Logger.std.dlog("Datum with key " + getMsg.getKey() + ", of P:" +partition +
                        " is not assigned to this node. Asking to node " + nodesOfThisPartition.get(rrIndex));
                //set self as sender
                getMsg.setSender(self());
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
    }

    private void tryOnPutMessage(PutMsg msg) {
        try {
            onPutMessage(msg);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    /**
     * On put message, update the ValueData in the memory at the key specified, only if this replica is leader
     * or the sender of the putMsg was this' previous replica.
     * Then reply if it's ok according to the number of writes requested in the put message.
     * Finally send this put message to the next replica in the hierarchical list
     * @param putMsg the put message
     */
    private void onPutMessage(PutMsg putMsg) {
        //decrease alive time (in steps of passage) of the message and eventually answer with an error
        if(putMsg.reduceAliveSteps()) {
            Logger.std.ilog("Node" +nodeNumber+ " received a put message to be killed. It has been discarded");
            sender().tell(new ReplyErrorMsg("Put message was forwarded too many times and has been killed"), self());
            return;
        }

        Logger.std.dlog("Node" +nodeNumber+ " received put message " + putMsg.toString());

        //if is a legit request: from client assigned to this node or another node of the system
        if(assignedClientIDs.contains(putMsg.getClientID()) || storeNodesSet.contains(putMsg.sender())) {

            int partition = partitionOf(putMsg.getKey());
            List<ActorRef> nodesOfKey = nodesOfPartition.get(partition);

            //if this node have assigned the partition of that key
            if(nodesOfKey.contains(self())) {

                //if this is the leader of the partition of that datum, write and propagate
                if(nodesOfKey.get(0).equals(self())) {
                    insertData(putMsg, partition);
                    if(nodesOfKey.size() >= 2) {
                        putMsg.setSender(self());
                        CompletableFuture<Object> future = ask(nodesOfKey.get(1), putMsg, timeout.multipliedBy(nodesOfKey.size()-1)).toCompletableFuture();
                        pipe(future, getContext().dispatcher()).to(sender());
                    }
                }

                //if is not leader but the put comes from the leader, then update data, and forward if there are
                //replicas after this one, or reply to the leader if is the last replica
                else if(nodesOfKey.get(0).equals(putMsg.sender())) {
                    Logger.std.dlog("Node" +nodeNumber+ "is not leader, put request from leader");
                    //update data
                    insertData(putMsg, partition);

                    //if is last node, reply to the leader
                    if(nodesOfKey.get(nodesOfKey.size()-1).equals(self())) {
                        sender().tell(new ReplyPutMsg(putMsg.getKey(), putMsg.getVal(), true), self());
                    } //if is not the last node, then forward the leader's request to the next
                    else {
                        nodesOfKey.get(nodesOfKey.indexOf(self())+1).forward(putMsg, getContext());
                    }
                }
                //else this is a client access point but this node has no leadership on that key. ask to the leader
                else {
                    putMsg.setSender(self());
                    CompletableFuture<Object> future = ask(nodesOfKey.get(0), putMsg, timeout.multipliedBy(nodesOfKey.size())).toCompletableFuture();
                    pipe(future, getContext().dispatcher()).to(sender());
                }
            }
            //if the node doesn't have this key assigned, ask to the leader of the key to perform a put
             else {
                 putMsg.setSender(self());
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
        Logger.std.log(Logger.LogLevel.VERBOSE, "Unknown message received by Node" + nodeNumber+ ": " + unknownMsg);
    }




    public static Props props(long timeoutSeconds) {
        return Props.create(StoreNode.class, timeoutSeconds);
    }


    ///Other private methods

    private int partitionOf(String key) {
        return key.hashCode() % hashSpacePartition;
    }

    private boolean hasData(String key, int partition) {
        if(!dataPerPartition.containsKey(partition) || !dataPerPartition.get(partition).containsKey(key))
            return false;
        return true;
    }

    /**
     * Get the String value for a key if exists, null otherwise
     * @param key the key of the datum
     * @param partition the partition related to the key
     * @return the string if present in memory, null if not present
     */
    private String getDataValue(String key, int partition) {
        if(!dataPerPartition.containsKey(partition) || !dataPerPartition.get(partition).containsKey(key))
            return null;
        return dataPerPartition.get(partition).get(key).getValue();
    }

    private void insertData(PutMsg putMsg, int partition) {
        if(!dataPerPartition.containsKey(partition))
            dataPerPartition.put(partition, new HashMap<>());

        dataPerPartition.get(partition).put(putMsg.getKey(), new ValueData(putMsg.getVal()));
    }


    private void assignNewPartition(int partition) {
        dataPerPartition.put(partition, new HashMap<>());
        assignedPartitions.add(partition);
    }

    private void removePartitionAssignment(int partitionRemoved) {
        dataPerPartition.remove(partitionRemoved);
        assignedPartitions.remove(partitionRemoved);
    }

    private String toStringNodesOfPartition() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < nodesOfPartition.size(); i++) {
            sb.append("P:" + i + " - [");
            for (int j = 0; j < nodesOfPartition.get(i).size(); j++) {
                sb.append(formatNameForActorRef(nodesOfPartition.get(i).get(j)));
                if(j != nodesOfPartition.get(i).size()-1)
                    sb.append(", ");
                else
                    sb.append(("]\n"));
            }
        }
        return sb.toString();
    }

    private String formatNameForActorRef(ActorRef ref) {

        //if both are nonempty then the ref is not this (or so it appears)
        if(ref.path().address().host().nonEmpty() && ref.path().address().port().nonEmpty()) {
            return ref.path().name() + "@" +ref.path().address().host().get() + ":" + ref.path().address().port().get();
        } else {
            return "Node"+nodeNumber;
        }
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
