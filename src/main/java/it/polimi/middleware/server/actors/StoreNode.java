package it.polimi.middleware.server.actors;


import akka.actor.AbstractActorWithStash;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import it.polimi.middleware.messages.*;
import it.polimi.middleware.server.management.HistoryKeeper;
import it.polimi.middleware.server.messages.*;
import it.polimi.middleware.server.store.ValueData;
import it.polimi.middleware.util.Logger;
import scala.Option;

import static akka.pattern.Patterns.ask;
import static akka.pattern.Patterns.pipe;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;


public class StoreNode extends AbstractActorWithStash {
    private final Cluster cluster = Cluster.get(getContext().system());

    private int nodeNumber = -1;

    /**
     * The progressive id for updates coming from storemanager, to determine older ones
     */
    private long currentUpdateID = 0;

    private Duration timeout;

    private final int clientThresholdNotification;

    private int lastClientNotificationAmount = 0;

    private int hashSpacePartition;

    private ActorRef storeManager;

    /**
     * The main data contained inside this StoreNode
     */
    private final HashMap<Integer, HashMap<String, ValueData>> dataPerPartition;

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
    private Map<Long, Map<Integer, Long>> clientOpIDMap;

    /**
     * The set of the partitions assigned to this node
     */
    private final Set<Integer> assignedPartitions;

    /**
     * Set of partitions to be removed from memory when update of nodes is complete
     */
    private final Set<Integer> partitionsToRemoveOnUpdateComplete;

    /**
     * the set of all assigned clients to this node
     */
    private final Set<Long> assignedClientIDs;

    /**
     * the map mapping a client (actor) to its ID. Contains all clients served by this node
     */
    private final Map<ActorRef, Long> clientToIDMap;


    private final Map<Integer, Long> updateValuePerPartition;

    //round robin index
    private int rrIndex=0;

    private boolean isInRequestingDataState = false;

    private Set<Integer> partitionsRequired;

    private HistoryKeeper historyKeeper;


    /**
     * New StoreNode with the specified hashPartition.
     */
    public StoreNode() {

        Config conf = ConfigFactory.load("conf/store.conf");
        clientThresholdNotification = conf.getInt("store.node.clientThresholdNotification");
        timeout = Duration.ofSeconds(conf.getInt("store.connection.stdTimeout"));
        historyKeeper = new HistoryKeeper(conf.getInt("store.node.historySize"));

        dataPerPartition = new HashMap<>();
        nodesOfPartition = new ArrayList<>();
        clientOpIDMap = new HashMap<>();
        assignedPartitions = new HashSet<>();
        storeNodesSet = new HashSet<>();
        assignedClientIDs = new HashSet<>();
        unreachableNodes = new HashSet<>();
        partitionsRequired = new HashSet<>();
        partitionsToRemoveOnUpdateComplete = new HashSet<>();
        clientToIDMap = new HashMap<>();
        updateValuePerPartition = new HashMap<>();
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
                .match(UpdateStoreNodeStatusMsg.class, this::onUpdateStoreNodeStatusMessage)
                .match(PartitionRequestMsg.class, this::onPartitionRequestMsg)
                .match(PartitionRequestReplyMsg.class, this::onPartitionRequestReplyMsg)
                .match(CheckDataConsistencyMsg.class, this::onCheckDataConsistencyMsg)
                .match(DataConsistencyOkMsg.class, this::onDataConsistencyOkMsg)
                .match(UpdateAllCompleteMsg.class, this::onUpdateAllCompleteMsg)
                .match(ClusterEvent.MemberEvent.class, msg -> {Logger.std.dlog("Node" + nodeNumber + " received message " +msg);})
                .match(Terminated.class, this::onTerminatedClient)
                .matchAny(msg -> stash())
                .build();
    }

    private Receive active() {
        return receiveBuilder()
                .match(GetMsg.class, this::tryOnGetMessage)
                .match(PutMsg.class, this::tryOnPutMessage)
                .match(UpdateStoreNodeStatusMsg.class, this::onUpdateStoreNodeStatusMessage)
                .match(PartitionRequestMsg.class, this::onPartitionRequestMsg)
                .match(PartitionRequestReplyMsg.class, this::onPartitionRequestReplyMsg)
                .match(CheckDataConsistencyMsg.class, this::onCheckDataConsistencyMsg)
                .match(DataConsistencyOkMsg.class, this::onDataConsistencyOkMsg)
                .match(ClientAssignMsg.class, this::onClientAssignMessage)
                .match(ClusterEvent.UnreachableMember.class, this::onUnreachableMember)
                .match(ClusterEvent.ReachableMember.class, this::onReachableMember)
                .match(ClusterEvent.MemberEvent.class, msg -> {Logger.std.dlog("Node" + nodeNumber + " received message " +msg);})
                .match(Terminated.class, this::onTerminatedClient)
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
        Logger.std.dlog("Node" +nodeNumber+" received update msg, with id: " + msg.getUpdateID() +
                ", curr updateID:" +currentUpdateID);
        //discard the message if the update is older than the last one received by this node
        if(msg.getUpdateID() < currentUpdateID) {
            return;
        }
        currentUpdateID = msg.getUpdateID();

        isInRequestingDataState = true;

        getContext().become(inactive());

        Set<Integer> partitionsToExcludeWhenRequiringUpdate = new HashSet<>();

        //re-write the set of nodes in the system
        storeNodesSet = new HashSet<>();


        for (int i = 0; i < msg.getNodesOfPartition().size(); i++) {
            //if this node is assigned to a partition to which wasn't assigned before, add that partition
            if(msg.getNodesOfPartition().get(i).contains(self()) && !assignedPartitions.contains(i)) {
                Logger.std.ilog("Node"+nodeNumber+" adding partition " + i);
                assignNewPartition(i);
                partitionsToExcludeWhenRequiringUpdate.add(i);
            }
            //else if this node is no longer assigned to a partition, remove it
            else if (!msg.getNodesOfPartition().get(i).contains(self()) && assignedPartitions.contains(i)) {
                Logger.std.ilog("Node" +nodeNumber+ " is marking partition " + i + " to be removed after update " +currentUpdateID);
                partitionsToRemoveOnUpdateComplete.add(i);
                partitionsToExcludeWhenRequiringUpdate.add(i);
            }

            //else if this is the leader and remained the leader in the update, exclude that partition when requiring
            //update
            else if(msg.getNodesOfPartition().get(i).get(0).compareTo(self()) == 0 &&
                nodesOfPartition.get(i).get(0).compareTo(self()) == 0)
                partitionsToExcludeWhenRequiringUpdate.add(i);

            //for each partition, assign to the new set of nodes all the nodes
            storeNodesSet.addAll(msg.getNodesOfPartition().get(i));
        }



        //for each partition not assigned in this update, and for which this node is not leader,
        //ask for a check of consistency with data
        for (Integer partition :
                assignedPartitions) {
            if (!partitionsToExcludeWhenRequiringUpdate.contains(partition)) {
                partitionsRequired.add(partition);
                Logger.std.dlog("Node"+nodeNumber+" sending check consistency for partition " + partition +
                        " to " + nodesOfPartition.get(partition).get(0).path().address());
                nodesOfPartition.get(partition).get(0).tell(
                        new CheckDataConsistencyMsg(partition, updateValuePerPartition.get(partition), currentUpdateID), self());
            }
        }

        //update the new nodesOfPartition, after assignment
        nodesOfPartition = msg.getNodesOfPartition();

        //if at the end of the update no partitions are required, report to the store manager that update for
        //this node is ended
        if(partitionsRequired.isEmpty()) {
            Logger.std.dlog("Node " + nodeNumber + " sending on update an update complete msg" +
                    " since is already empty the partitions required");
            isInRequestingDataState = false;
            storeManager.tell(new UpdateStoreNodeCompletedMsg(currentUpdateID), self());
        }


        Logger.std.ilog("Node"+nodeNumber+"'s new partitions on update " +currentUpdateID+":"
                +assignedPartitions +". full partition:\n" + toStringNodesOfPartition());

    }


    private void onActivateNodeMessage(ActivateNodeMsg msg) {

        Logger.std.dlog("Node" +nodeNumber+ " received as nodesOfPartitions: " + msg.getNodesOfPartition()
                + "\nsize: " +msg.getNodesOfPartition().size());
        hashSpacePartition = msg.getNodesOfPartition().size();
        nodesOfPartition = msg.getNodesOfPartition();
        currentUpdateID = msg.getUpdateID();

        //add all the actors in the lists to the set, and all the partitions assigned to this node
        for (int i = 0; i < nodesOfPartition.size(); i++) {
            storeNodesSet.addAll(nodesOfPartition.get(i));
            if(nodesOfPartition.get(i).contains(self()))
                assignedPartitions.add(i);
        }

        for (Integer assignedPartition :
                assignedPartitions) {
            dataPerPartition.put(assignedPartition, new HashMap<>());
            updateValuePerPartition.put(assignedPartition, 0L);
        }

        Logger.std.dlog("Node" +nodeNumber+ " received partitions: " + assignedPartitions);
        Logger.std.dlog("Final hashmap for Node" +nodeNumber+":\n" + toStringNodesOfPartition());


        //if has to request data, request it and become inactive
        if(msg.mustRequestData()) {
            isInRequestingDataState = true;
            getContext().become(inactive());
            partitionsRequired = new HashSet<>(assignedPartitions);
            for (Integer partition: assignedPartitions) {
                if(nodesOfPartition.get(partition).get(0) != null && !nodesOfPartition.get(partition).get(0).equals(self()))
                    nodesOfPartition.get(partition).get(0).tell(new PartitionRequestMsg(partition, currentUpdateID), self());
            }
        }
        //if doesn't have to request data, become active now
        else {
            getContext().become(active());
            unstashAll();
        }

    }


    private void onCheckDataConsistencyMsg(CheckDataConsistencyMsg msg) {
        if(msg.getUpdateID() < currentUpdateID)
            return;

        if(msg.getUpdateValue() != updateValuePerPartition.get(msg.getPartition())) {
            Logger.std.dlog("Node"+nodeNumber+" received a check consistency from " + sender() +
                    ", but failed. This node's update value for partition " +msg.getPartition() +
                    " is " + updateValuePerPartition.get(msg.getPartition()) +", while sender's is " +msg.getUpdateValue());

            sender().tell(new PartitionRequestReplyMsg(msg.getPartition(), dataPerPartition.get(msg.getPartition()),
                    getClientOpIDMapForSinglePartition(msg.getPartition()), msg.getUpdateID()), self());
        } else {
            Logger.std.dlog("Node"+nodeNumber+" received check consistency from " + sender().path().address()+
                    " for partition " +msg.getPartition() +" and is ok");
            sender().tell(new DataConsistencyOkMsg(msg.getPartition(), msg.getUpdateID()), self());
        }
    }


    private void onDataConsistencyOkMsg(DataConsistencyOkMsg msg) {

        //if is outdated, do not consider it
        if(msg.getUpdateID() < currentUpdateID) {
            Logger.std.dlog("Consistency check ok on Node"+nodeNumber+" but was ignored since is " +
                    "lower than this one's");
            return;
        }

        if(isInRequestingDataState) {

            //if the partition was one required, remove it and put the new data in
            partitionsRequired.remove(msg.getPartition());

            Logger.std.dlog("Node"+nodeNumber +", check ok for partition " +msg.getPartition() +
                    ", partitions required left: " + partitionsRequired);

            //if all the partitions required are received, notify the store manager
            if(partitionsRequired.isEmpty()) {
                isInRequestingDataState = false;
                Logger.std.dlog("Node"+nodeNumber+" got all the data needed. Activating the node and " +
                        "notifying the storemanager");
                storeManager.tell(new UpdateStoreNodeCompletedMsg(currentUpdateID), self());
            }
        }
    }


    /**
     * On partition request, send to the node asking for it the data required if this is the leader.
     * If is not, forward this to the leader
     */
    private void onPartitionRequestMsg(PartitionRequestMsg msg) {
        //forward indefinitely the message on update if this is leader but partition requested is still pending
        //as a request
        if( (nodesOfPartition.get(msg.getPartitionRequired()).get(0).compareTo(self()) == 0 &&
                partitionsRequired.contains(msg.getPartitionRequired()))) {
            if(System.currentTimeMillis() % 1000 == 0)
                Logger.std.dlog("Node"+nodeNumber+" autoforwarding partition request for partition" + " " + msg.getPartitionRequired() + " from " + sender().path().address());
            self().forward(msg, getContext());
            return;
        }

        Logger.std.dlog("Node"+nodeNumber+" received PartitionRequestMsg for partition " +msg.getPartitionRequired());
        if(!msg.open()) {
            Logger.std.dlog("Message PartitionRequest was killed due to many forwards");
            return;
        }


        //if this node is the leader with valid data or possibly it was since removed that partition
        //in this update, answer with its data
        if( nodesOfPartition.get(msg.getPartitionRequired()).get(0).compareTo(self()) == 0 ||
                partitionsToRemoveOnUpdateComplete.contains(msg.getPartitionRequired())) {
            Logger.std.dlog("Leader of partition " + msg.getPartitionRequired()+ " received partition request, giving a copy of this data to " + sender());
            sender().tell(new PartitionRequestReplyMsg(msg.getPartitionRequired(),
                    dataPerPartition.get(msg.getPartitionRequired()),
                    getClientOpIDMapForSinglePartition(msg.getPartitionRequired()), currentUpdateID), self());
        }
        //if this is not the leader for any reason, forward the request to the true leader
        else {
            Logger.std.dlog("Partition request on Node"+nodeNumber+" made on wrong node");
            nodesOfPartition.get(msg.getPartitionRequired()).get(0).forward(msg, getContext());
        }
    }



    private void onPartitionRequestReplyMsg(PartitionRequestReplyMsg msg) {

        if(msg.getUpdateID() < currentUpdateID)
            return;

        Logger.std.dlog("Node"+nodeNumber+" received partition request reply msg." +
                " Partition received: " +msg.getPartitionRequired());

        if(isInRequestingDataState) {
            //if the partition was one required, remove it and put the new data in
            if(partitionsRequired.remove(msg.getPartitionRequired())) {
                Logger.std.dlog("Node"+nodeNumber+" adding data for partition " + msg.getPartitionRequired());
                dataPerPartition.put(msg.getPartitionRequired(), msg.getPartitionData());
                msg.getClientToOpIDMap().forEach((clientID, opID) -> {
                    clientOpIDMap.get(clientID).put(msg.getPartitionRequired(), opID);
                });
            }

            //if all the partitions required are received, notify the store manager
            if(partitionsRequired.isEmpty()) {
                isInRequestingDataState = false;
                Logger.std.dlog("Node"+nodeNumber+" got all the data needed. Activating the node and " +
                        "notifying the storemanager");
                storeManager.tell(new UpdateStoreNodeCompletedMsg(currentUpdateID), self());
            }
        } else {
            Logger.std.dlog("Node"+nodeNumber+" received a partition request reply not asked for!" +
                    " (partition:" +msg.getPartitionRequired()+", sender:" + sender());
        }
    }


    private void onUpdateAllCompleteMsg(UpdateAllCompleteMsg msg) {
        if(msg.getUpdateID() == currentUpdateID) {
            //if another update is incoming, do nothing and wait for it
            if(!msg.isAnotherUpdateIncoming()) {
                for (Integer partitionToRemove :
                        partitionsToRemoveOnUpdateComplete) {
                    removePartitionAssignment(partitionToRemove);
                }
                partitionsToRemoveOnUpdateComplete.clear();

                getContext().become(active());
                unstashAll();
            }
        } else {
            Logger.std.dlog("Node"+nodeNumber+" received update-all-complete message with " +
                    "id " + msg.getUpdateID() +" but current was " +currentUpdateID);
        }
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

        if(getMsg.reduceAliveSteps()) {
            Logger.std.ilog("Node" +nodeNumber+ " received a get message to be killed. It has been discarded");
            sender().tell(new ReplyErrorMsg("Get message was forwarded too many times and has been killed"), self());
            return;
        }


        Logger.std.dlog("Node" +nodeNumber+ " received get message with key " + getMsg.getKey() +
                " from " +sender().path().name());
        //check client id or if request comes from another node in the system
        if(assignedClientIDs.contains(getMsg.getClientID()) || storeNodesSet.contains(getMsg.sender())) {
            int partition = partitionOf(getMsg.getKey());
            //if datum is assigned to this replica
            if(dataPerPartition.containsKey(partition)) {
                ReplyGetMsg reply = replyGetFromData(getMsg, partition);
                if(reply != null) {
                    sender().tell(reply, self());
                }
                //if datum was not found here, ask to the leader. Even if this is the leader, the message could be late, so forward to
                //itself anyway. The message will die eventually or be answered
                else {
                    //set self as sender
                    getMsg.setSender(self());
                    //ask and pipe the answer
                    CompletableFuture<Object> future = ask(nodesOfPartition.get(partition).get(0), getMsg, timeout).toCompletableFuture();
                    pipe(future, getContext().dispatcher()).to(sender());
                }
            } else {
                // if datum is not assigned to this replica, ask to another replica to which has it assigned
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
        //sender has no authorization to talk with this node
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
                    insertData(putMsg, partition, false);
                    if(nodesOfKey.size() >= 2) {
                        putMsg.setSender(self());
                        CompletableFuture<Object> future = ask(nodesOfKey.get(1), putMsg, timeout.multipliedBy(nodesOfKey.size()-1)).toCompletableFuture();
                        pipe(future, getContext().dispatcher()).to(sender());
                    }
                }

                //if is not leader but the put comes from the leader, then update data, and forward if there are
                //replicas after this one, or reply to the leader if is the last replica
                else if(nodesOfKey.get(0).equals(putMsg.sender())) {
                    Logger.std.dlog("Node" +nodeNumber+ " is not leader, put request from leader");
                    //update data
                    insertData(putMsg, partition, true);

                    //if is last node, reply to the leader
                    if(nodesOfKey.get(nodesOfKey.size()-1).equals(self())) {
                        sender().tell(new ReplyPutMsg(putMsg.getKey(), putMsg.getVal(), putMsg.getClientOpID(), true), self());
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

    /**
     * Client assign can happen when a client shows up (is not single assignment) or when a client request
     * a new node. In this last case answer with a different message
     * @param msg the message
     */
    private void onClientAssignMessage(ClientAssignMsg msg) {
        Logger.std.dlog("Node" + nodeNumber+" received client " +msg.getClientRef() +
                (msg.isSingleAssignment() ? " as its first connection" : " as requesting a new access node"));

        //if is single assignment, means that is a generic new assignment while client is already connected
        if(msg.isSingleAssignment()) {
            sender().tell(new RequestNewActorReplyMsg(self()), self());
        }
        //if not single assignment, then is first assignment for that client. setup stuff
        else {
            assignedClientIDs.add(msg.getClientID());
            clientToIDMap.put(msg.getClientRef(), msg.getClientID());
            getContext().watch(msg.getClientRef());
            clientOpIDMap.put(msg.getClientID(), new HashMap<>());
            sender().tell(new GreetingReplyMsg(self(), msg.getNodesAssigned(), hashSpacePartition,  msg.getClientID()), self());
        }


        notifyStoreManagerIfClientLoadChangedEnough();
    }

    private void onTerminatedClient(Terminated t) {
        //remove the actorRef and the id, then notify the store manager if the load has changed significantly
        assignedClientIDs.remove(clientToIDMap.remove(t.actor()));

        notifyStoreManagerIfClientLoadChangedEnough();
    }


    private void onUnknownMessage(Object unknownMsg) {
        //simply log the unknown message
        Logger.std.log(Logger.LogLevel.VERBOSE, "Unknown message received by Node" + nodeNumber+ ": " + unknownMsg);
    }




    public static Props props() {
        return Props.create(StoreNode.class);
    }


    ///Other private methods

    private void notifyStoreManagerIfClientLoadChangedEnough() {
        //if the amount of client shifted too much from last notification, notify the store manager
        if(assignedClientIDs.size() == lastClientNotificationAmount + clientThresholdNotification ||
                assignedClientIDs.size() == lastClientNotificationAmount - clientThresholdNotification) {
            lastClientNotificationAmount = assignedClientIDs.size();
            storeManager.tell(new NodeLoadOfClientsMsg(assignedClientIDs.size()), self());
        }
    }

    private int partitionOf(String key) {
        return key.hashCode() % hashSpacePartition;
    }

    private boolean hasData(String key, int partition) {
        if(!dataPerPartition.containsKey(partition) || !dataPerPartition.get(partition).containsKey(key))
            return false;
        return true;
    }

    /**
     * Get a ReplyGetMsg containing value for a key if exists, null otherwise
     * @param msg the get message
     * @param partition the partition related to the key
     * @return a reply get msg if can retrieve it, null otherwise
     */
    private ReplyGetMsg replyGetFromData(GetMsg msg, int partition) {
        //setup if something is empty for a specific client
        if(!clientOpIDMap.containsKey(msg.getClientID()))
            clientOpIDMap.put(msg.getClientID(), new HashMap<>());
        if(!clientOpIDMap.get(msg.getClientID()).containsKey(partition))
            clientOpIDMap.get(msg.getClientID()).put(partition, 0L);

        //if op id is consistent with what's saved, then get it
        if(clientOpIDMap.get(msg.getClientID()).get(partition) == msg.getClientOpID()) {
            return new ReplyGetMsg(msg.getKey(), dataPerPartition.get(partition).get(msg.getKey()).getValue(),
                    msg.getClientOpID());
        }
        //if op id is precedent what's is in mem, ask to the history, if present
        else if (clientOpIDMap.get(msg.getClientID()).get(partition) < msg.getClientOpID()) {
            return historyKeeper.getDatumFromHistory(msg);
        }
        //if op id is of future, then ask to the leader
        else {
            return null;
        }
    }



    /**
     * Insert a datum in the node's data
     * @param putMsg the message with the key and value
     * @param partition the partition of the key
     */
    private void insertData(PutMsg putMsg, int partition, boolean isFromLeader) {

        //setup for possibly-empty stuff
        if(!dataPerPartition.containsKey(partition))
            dataPerPartition.put(partition, new HashMap<>());
        if(!dataPerPartition.get(partition).containsKey(putMsg.getKey()))
            dataPerPartition.get(partition).put(putMsg.getKey(), new ValueData(""));
        //setup if something is empty for a specific client
        if(!clientOpIDMap.containsKey(putMsg.getClientID()))
            clientOpIDMap.put(putMsg.getClientID(), new HashMap<>());
        if(!clientOpIDMap.get(putMsg.getClientID()).containsKey(partition))
            clientOpIDMap.get(putMsg.getClientID()).put(partition, 0L);


        if(isFromLeader) {
            dataPerPartition.get(partition).put(putMsg.getKey(), new ValueData(putMsg.getVal(), putMsg.getNewness()));
            incrementPartitionUpdateValue(partition);

            if(putMsg.getClientOpID() > clientOpIDMap.get(putMsg.getClientID()).get(partition)) {
                //update the new opid
                clientOpIDMap.get(putMsg.getClientID()).put(partition, putMsg.getClientOpID());
            }
            //if is older, put it in history
            else {
                historyKeeper.insertPutMessage(putMsg);
            }

        }
        //else this is the leader
        else {
            //if message is newer, write it
            if(putMsg.getClientOpID() > clientOpIDMap.get(putMsg.getClientID()).get(partition)) {
                //update the new opid
                clientOpIDMap.get(putMsg.getClientID()).put(partition, putMsg.getClientOpID());

                long newness = dataPerPartition.get(partition).get(putMsg.getKey()).updateOnce(putMsg.getVal());
                incrementPartitionUpdateValue(partition);
                putMsg.setNewness(newness);
            }
        }
    }


    private void incrementPartitionUpdateValue(int partition) {
        //increment by 1 the update value of that partition
        updateValuePerPartition.put(partition, updateValuePerPartition.get(partition)+1);
    }


    private Map<Long, Long> getClientOpIDMapForSinglePartition(int partition) {
        Map<Long, Long> res = new HashMap<>();
        clientOpIDMap.forEach((clientID, partToIDMap) -> {
            res.put(clientID, partToIDMap.get(partition));
        });
        return res;
    }



    /**
     * assign new partition and ask to the leader data for it.
     * enter in requesting state.
     */
    private void assignNewPartition(int partition) {
        dataPerPartition.put(partition, new HashMap<>());
        assignedPartitions.add(partition);
        //request the data
        isInRequestingDataState = true;
        getContext().become(inactive());
        partitionsRequired.add(partition);
        nodesOfPartition.get(partition).get(0).tell(new PartitionRequestMsg(partition, currentUpdateID), self());

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
}
