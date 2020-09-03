package it.polimi.middleware.server.actors;

import akka.actor.*;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import it.polimi.middleware.messages.GreetingMsg;
import it.polimi.middleware.messages.GreetingReplyMsg;
import it.polimi.middleware.messages.RequestNewActorMsg;
import it.polimi.middleware.messages.RequestNewActorReplyMsg;
import it.polimi.middleware.server.exceptions.NotEnoughNodesException;
import it.polimi.middleware.server.management.PartitionManager;
import it.polimi.middleware.server.messages.*;
import it.polimi.middleware.util.Logger;

import static akka.pattern.Patterns.ask;
import static akka.pattern.Patterns.pipe;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * The node underlying the MasterNode. Manages everything conceiving the store: underlying nodes with their workers,
 * establish node's data hierarchy, to update values in order to be consistent, and forward messages to the correct node.
 */
public class StoreManager extends AbstractActorWithStash {
    private final Cluster cluster = Cluster.get(getContext().system());

    //progressive number assigned to new nodes to identify themselves
    private int nodeNumber = 0;

    private long updateID = 0;

    private final boolean autoStart;

    //In how many partitions the key space is divided
    @SuppressWarnings({"FieldMayBeFinal"})
    private int hashSpacePartition;
    //for each partition, minimum amount of node replicas of the same data there must be
    private final int minimumDataReplicas;

    //If this number of replicas are available (= reachable nodes), take action and warn the system that
    //too few reachable replicas remain, thus down some node or add some new node
    private final int minimumReachableDataReplicas;

    private final PartitionManager partitionManager;

    @SuppressWarnings({"FieldMayBeFinal"})
    private int assignedNodesPerClient;

    private final Duration timeout;

    private final List<ActorRef> storeNodes;

    /**
     * A list containing all nodes currently updating for an update given by this store manager
     */
    private final Set<ActorRef> updatingNodes;

    /**
     * List of nodes waiting to be added to the system.
     */
    private final Set<ActorRef> nodesInJoinQueue;

    private boolean isUpdateOngoing = false;


    private final Map<ActorRef, Integer> nodeClientAmountMap;

    public StoreManager() {
        //load from config file how much to divide hash space and how many replicas of data must be active
        Config conf = ConfigFactory.load("conf/store.conf");
        hashSpacePartition = conf.getInt("store.hashSpacePartition");
        minimumDataReplicas = conf.getInt("store.dataReplicas");
        assignedNodesPerClient = conf.getInt("store.assignedNodesPerClient");
        timeout = Duration.ofSeconds(conf.getInt("store.connection.stdTimeout"));
        minimumReachableDataReplicas = conf.getInt("store.minReachableReplicasBeforeTakeAction");
        autoStart = conf.getBoolean("store.autoStartStore");

        //prepare the list of lists with the specified capacities:
        //external list is size of the partition of space, each sublist is size of the number of replicas
        storeNodes = new ArrayList<>();
        updatingNodes = new HashSet<>();
        nodesInJoinQueue = new HashSet<>();

        nodeClientAmountMap = new HashMap<>();
        partitionManager = new PartitionManager(hashSpacePartition, minimumDataReplicas);
    }

    // Subscribe to cluster
    @Override
    public void preStart() {
        cluster.subscribe(self(), ClusterEvent.initialStateAsEvents(), ClusterEvent.MemberUp.class,
                ClusterEvent.UnreachableMember.class, ClusterEvent.MemberEvent.class, ClusterEvent.ReachableMember.class);
    }

    // Re-subscribe when restart
    @Override
    public void postStop() {
        cluster.unsubscribe(self());
    }


    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(GreetingMsg.class, this::onGreetingMessage)
                .match(RequestNewActorMsg.class, this::onRequestNewActorMsg)
                .match(NodeLoadOfClientsMsg.class, this::onNodeLoadOfClientsMsg)
                .match(ClusterEvent.MemberUp.class, this::onMemberUp)
                .match(ClusterEvent.MemberDowned.class, this::onMemberDowned)
                .match(ClusterEvent.UnreachableMember.class, this::onUnreachableMember)
                .match(ClusterEvent.ReachableMember.class, this::onReachableMember)
                .match(UpdateStoreNodeCompletedMsg.class, this::onUpdateStoreNodeCompletedMsg)
                .match(RequestActivateMsg.class, this::onRequestActivateMsg)
                .match(StartSystemMsg.class, this::onStartSystemMessage)
                .build();
    }


    /**
     * On member up, if the role is storenode, assign to him this node's reference
     * @param memberUp
     */
    private void onMemberUp(ClusterEvent.MemberUp memberUp) {
        Logger.std.dlog("Member is up: " + memberUp.member());
        if(memberUp.member().hasRole("storenode")) {
            Logger.std.dlog("Node"+nodeNumber+" will be " +memberUp.member().address());
            getContext().actorSelection(memberUp.member().address() + "/user/storenode").tell(new GrantAccessToStoreMsg(self(), nodeNumber++, true), self());
        }
        //down the new member if is another storenode newer than this. It never happens
        else if(memberUp.member().hasRole("storemanager") && cluster.selfMember().isOlderThan(memberUp.member())) {
            cluster.down(memberUp.member().address());
        }
    }

    private void onMemberDowned(ClusterEvent.MemberDowned md) {
        Logger.std.dlog("Member is downed: " + md.member());
        //remove all the storenode associated to that member
        if(md.member().hasRole("storenode")) {
            ActorRef nodeToRemove = ActorRef.noSender();
            Map<Integer, ActorRef> oldLeaders = partitionManager.getPartitionToLeaderMap();
            for (ActorRef node :
                    storeNodes) {
                if(node.path().address().equals(md.member().address())) {
                    try {
                        nodeToRemove = node;
                        partitionManager.removeNode(node);
                    } catch (NotEnoughNodesException nene) {
                        Logger.std.ilog("Not enough nodes left in the system!");
                        /*/
                        ActorRef newNode = spawnLocalNode();
                        isUpdateOngoing = true;
                        updatingNodes.add(newNode);
                        newNode.tell(new ActivateNodeMsg(partitionManager.getNodesOfPartitionList(), true, updateID), self());
                        //*/
                    }


                }
            }

            //remove the node, notify all other nodes of new assignments
            if (storeNodes.remove(nodeToRemove)) {
                nodeClientAmountMap.remove(nodeToRemove);
                updateID++;
                isUpdateOngoing = true;
                updatingNodes.clear();
                updatingNodes.addAll(storeNodes);

                Logger.std.dlog("Removed from store manager node " +nodeToRemove +
                        "\nstoreNodes("+storeNodes.size()+"):" +storeNodes + "\nupdatingNodes(" + updatingNodes.size()+"):"+updatingNodes +
                        "\nUpdate ID: " +updateID);
                for (ActorRef node :
                        storeNodes) {
                    node.tell(new UpdateStoreNodeStatusMsg(partitionManager.getNodesOfPartitionList(), oldLeaders,
                            updateID), self());
                }
            } else {
                Logger.std.dlog("Tried to remove a node which was not in the store manager. memberAddress: " +md.member().address());
            }
        }
    }

    private void onUnreachableMember(ClusterEvent.UnreachableMember um) {
        Logger.std.ilog("Detected unreachable member: " +um.member().address());

        cluster.down(um.member().address());
        if(true)
            return;

        Map<Integer, ActorRef> oldLeaders = partitionManager.getPartitionToLeaderMap();

        boolean markedUnreachable = false;
        if(um.member().hasRole("storenode")) {
            for (ActorRef node :
                    storeNodes) {
                if (node.path().address().equals(um.member().address())) {
                    Logger.std.dlog("Found the unreachable member in the storeNodes");

                    partitionManager.markUnreachable(node);
                    updatingNodes.remove(node);
                    markedUnreachable = true;

                    //if too few reachable partition replicas remain, remove unreachable nodes until there are enough reachable
                    //replicas again
                    if(partitionManager.getMinReachableReplicasAmount() <= minimumReachableDataReplicas) {
                        partitionManager.removeUnreachableNodeUntilSafeNumberOfReplicas(minimumReachableDataReplicas);
                        int missingNodes = partitionManager.getMissingNodesForMinimumReplicas();
                        for (int i = 0; i < missingNodes; i++) {
                            ActorRef newNode = spawnLocalNode();
                            newNode.tell(new GrantAccessToStoreMsg(self(), nodeNumber-1, false), self());
                            newNode.tell(new ActivateNodeMsg(partitionManager.getNodesOfPartitionList(), true, updateID), self());
                        }
                    }

                }
            }
        }

        //if some node were marked as unreachable something changed, tell to all other nodes
        if(markedUnreachable)
            updateAllNodes(oldLeaders);
    }



    private void onReachableMember(ClusterEvent.ReachableMember rm) {
        if(rm.member().hasRole("storenode")) {
            for (ActorRef node :
                    storeNodes) {
                if(node.path().address().equals(rm.member().address())) {
                    partitionManager.markReachable(node);
                }
            }
        }
    }


    private void onRequestActivateMsg(RequestActivateMsg msg) {
        Logger.std.dlog("Request activate received from " + msg.getStoreNodeRef().path());
        //if an update is ongoing and someone wants to join, wait for update completion first.
        // stash the message
        if(isUpdateOngoing) {
            Logger.std.dlog("Update " +updateID +" is ongoing. Request activation was stashed");
            nodesInJoinQueue.add(msg.getStoreNodeRef());
            stash();
            return;
        }
        //remove if present the node to the join queue
        nodesInJoinQueue.remove(msg.getStoreNodeRef());

        Map<Integer, ActorRef> oldLeaders = partitionManager.getPartitionToLeaderMap();

        //add the node logically to the system
        partitionManager.addNode(msg.getStoreNodeRef());
        storeNodes.add(msg.getStoreNodeRef());

        //if system is running, adding it to the partition manager means updating every node possibly.
        //Notify every node of the new assignments and activate the one making this request
        if(partitionManager.isRunning()) {
            updateID++;
            isUpdateOngoing = true;
            updatingNodes.addAll(storeNodes);
            for (ActorRef node :
                    storeNodes) {
                if (node.equals(msg.getStoreNodeRef()))
                    node.tell(new ActivateNodeMsg(partitionManager.getNodesOfPartitionList(), true, updateID), self());
                else {
                    Logger.std.log(Logger.LogLevel.VERBOSE, "Sending update status to " + node);
                    node.tell(new UpdateStoreNodeStatusMsg(partitionManager.getNodesOfPartitionList(),
                            oldLeaders, updateID), self());
                }
            }
        }
        //if is not running but autostart is on check if the system can start
        else if(partitionManager.canStart() && autoStart) {
            try {
                partitionManager.start();
                sendSetupMessagesToStoreNodes();
                Logger.std.ilog("System auto-started successfully. Partitions:\n"
                        + partitionManager.toStringPartitionsOfNode() +"\n"
                        + partitionManager.toStringNodesOfPartition());
                getContext().parent().tell(new StartSystemReplyMsg(true,
                                "System auto-started successfully"), self());
            } catch (NotEnoughNodesException nene) {
                Logger.std.dlog("Store tried to start after a request activate but failed." +
                        " This should never happen since there's a check");
            }
        }
    }


    private void onUpdateStoreNodeCompletedMsg(UpdateStoreNodeCompletedMsg msg) {
        if(isUpdateOngoing) {
            if(msg.getUpdateID() == updateID) {
                updatingNodes.remove(sender());
                Logger.std.dlog("Update received from " +sender().path().address() +", remaining: " +
                        updatingNodes.size());
                //if no more nodes are left to update, notify to all that update is complete.
                //if other nodes are in queue to be added, notify the nodes that another update is incoming
                if(updatingNodes.isEmpty()) {
                    Logger.std.dlog("Update " + updateID + " is complete for all nodes. Another "
                            +nodesInJoinQueue.size() + " updates are pending");
                    isUpdateOngoing = false;
                    if(nodesInJoinQueue.isEmpty()) {
                        for (ActorRef node :
                                storeNodes) {
                            node.tell(new UpdateAllCompleteMsg(updateID, false), self());
                        }
                    }
                    //if some nodes are in queue to join, notify all nodes that update is complete, another is incoming,
                    //and unstash an activate message
                    else {
                        for (ActorRef node :
                                storeNodes) {
                            node.tell(new UpdateAllCompleteMsg(updateID, true), self());
                        }
                        unstash();
                    }

                }
            } //if update id is different, discard this message

        } else {
            Logger.std.dlog("Received update-complete message from " + sender().path().address() +
                    ", but no updates are ongoing. This shouldn't happen");
        }
    }


    private void onStartSystemMessage(StartSystemMsg msg) {
        //stop if already running
        if(partitionManager.isRunning()) {
            sender().tell(new StartSystemReplyMsg(false, "System is already running!"), self());
            return;
        }

        if(msg.forceNodeCreation())
            localFillStoreRequirements();

        tryStartSystem();
    }



    private void tryStartSystem() {
        try {
            partitionManager.start();
            sendSetupMessagesToStoreNodes();
            sender().tell(new StartSystemReplyMsg(true), self());
            Logger.std.ilog("Partitions:\n" + partitionManager.toStringPartitionsOfNode());
        } //if the partition manager cannot start because there are too few nodes, notify the master node
        catch (NotEnoughNodesException e) {
            sender().tell(new StartSystemReplyMsg(false, e.toString()), self());
        }
    }

    /**
     * Spawn locally all remaining nodes to have a functional store on with specifics specified on the store.conf file
     */
    private void localFillStoreRequirements() {
        for (int j = storeNodes.size(); j < minimumDataReplicas; j++) {
            spawnLocalNode();
        }
    }


    /**
     * Send the setup messages to all store nodes, containing info on their role and neighbor, to make them available.
     * This assumes that the storeNodesLists List is fully initialized and no node is yet operative.
     */
    private void sendSetupMessagesToStoreNodes() {
        Logger.std.dlog("Sending setup messages to nodes. nodesOfPartitionList is:\n" + partitionManager.getNodesOfPartitionList());
        for (ActorRef node : storeNodes) {
            node.tell(new ActivateNodeMsg(partitionManager.getNodesOfPartitionList(), false, updateID), self());
        }
        Logger.std.dlog("Setup messages sent");
    }


    /**
     * On greet message, generate a ID for the client, contact X (config parameter) nodes to be their access points
     * @param msg .
     */
    private void onGreetingMessage(GreetingMsg msg) {
        try {
            //if the system is not running yet, reply with a fail greet
            if(!partitionManager.isRunning())
                sender().tell(new GreetingReplyMsg("Server is not available now. Retry later "), self());


            //assign an acceptable number of nodes to that clients. Which is the one in the config file, or all the nodes
            //if are less
            int assignedNodes = Math.min(assignedNodesPerClient, storeNodes.size());
            //generate random but unique user id (can be negative or positive)
            long uid = UUID.randomUUID().getMostSignificantBits();

            //sort by load of nodes in terms of client assigned, and assign to the least busy ones the new client
            storeNodes.sort(Comparator.comparingInt(this::clientLoadOfNode));

            for (int i = 0; i < assignedNodes; i++) {
                storeNodes.get(i).forward(new ClientAssignMsg(uid, assignedNodes), getContext());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private void onRequestNewActorMsg(RequestNewActorMsg msg) {
        //sort by load of nodes in terms of client assigned, and assign to the least busy ones the new client
        storeNodes.sort(Comparator.comparingInt(this::clientLoadOfNode));

        for (ActorRef node : storeNodes) {
            //if is not a node the client already have, give it, increment load and return
            if (!msg.getActorsAlreadyAssigned().contains(node)) {
                Logger.std.dlog("Asking to " + node.path().address() +" to manage client access, as a new" +
                        " access point for them");
                node.forward(new ClientAssignMsg(msg.getClientID()), getContext());
                return;
            }
        }
    }


    /**
     * When a node tells its load changed, update the map
     * @param msg
     */
    private void onNodeLoadOfClientsMsg(NodeLoadOfClientsMsg msg) {
        Logger.std.dlog(sender().path().address()+" notified that its client" +
                " load now is " +msg.getClientLoad());
        nodeClientAmountMap.put(sender(), msg.getClientLoad());
    }



    private ActorRef spawnLocalNode() {
        //load conf file
        Config conf = ConfigFactory.load("conf/cluster.conf")
                .withValue("akka.remote.netty.tcp.port", ConfigValueFactory.fromAnyRef(0)) //
                .withValue("akka.cluster.roles", ConfigValueFactory.fromIterable(Collections.singletonList("storeNode")));

        ActorSystem as = ActorSystem.create("ServerClusterSystem", conf);
        //create the new storeNode
        ActorRef newNode = as.actorOf(StoreNode.props(), "Node"+ (nodeNumber) + "L");
        storeNodes.add(newNode);
        partitionManager.addNode(newNode);
        newNode.tell(new GrantAccessToStoreMsg(self(), nodeNumber++, false), self());
        return newNode;
    }

    /**
     * Update all the active nodes in the system with the routing table of partitions
     */
    private void updateAllNodes(Map<Integer, ActorRef> oldLeaders) {
        updateID++;
        for (ActorRef node :
                partitionManager.getActiveNodes()) {
            node.tell(new UpdateStoreNodeStatusMsg(partitionManager.getNodesOfPartitionList(),
                    oldLeaders, updateID), self());
        }
    }


    /**
     * The load of a node, in terms of estimated assigned clients
     * @param node the node
     * @return how many clients are assigned to that node
     */
    private int clientLoadOfNode(ActorRef node) {
        nodeClientAmountMap.putIfAbsent(node, 0);
        return nodeClientAmountMap.get(node);
    }


    public static Props props() {
        return Props.create(StoreManager.class);
    }
}
