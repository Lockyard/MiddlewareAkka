package it.polimi.middleware.server.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import it.polimi.middleware.messages.GreetingMsg;
import it.polimi.middleware.messages.GreetingReplyMsg;
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
public class StoreManager extends AbstractActor {
    private final Cluster cluster = Cluster.get(getContext().system());

    //progressive number assigned to new nodes to identify themselves
    private int nodeNumber = 0;

    //In how many partitions the key space is divided
    @SuppressWarnings({"FieldMayBeFinal"})
    private int hashSpacePartition;
    //for each partition, minimum amount of node replicas of the same data there must be
    private final int minimumDataReplicas;

    private PartitionManager partitionManager;

    @SuppressWarnings({"FieldMayBeFinal"})
    private int assignedNodesPerClient;

    private final Duration timeout;

    private final Random r;

    private final List<ActorRef> storeNodes;

    private Map<ActorRef, Integer> nodeClientAmountMap;

    public StoreManager() {
        //load from config file how much to divide hash space and how many replicas of data must be active
        Config conf = ConfigFactory.load("conf/store.conf");
        hashSpacePartition = conf.getInt("store.hashSpacePartition");
        minimumDataReplicas = conf.getInt("store.dataReplicas");
        assignedNodesPerClient = conf.getInt("store.assignedNodesPerClient");
        timeout = Duration.ofSeconds(conf.getInt("store.connection.stdTimeout"));

        //prepare the list of lists with the specified capacities:
        //external list is size of the partition of space, each sublist is size of the number of replicas
        storeNodes = new ArrayList<>();

        r = new Random(System.currentTimeMillis());
        nodeClientAmountMap = new HashMap<>();
        partitionManager = new PartitionManager(hashSpacePartition, minimumDataReplicas);
    }

    // Subscribe to cluster
    @Override
    public void preStart() {
        cluster.subscribe(self(), ClusterEvent.initialStateAsEvents(), ClusterEvent.MemberEvent.class);
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
                .match(ClusterEvent.MemberUp.class, this::onMemberUp)
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
            Logger.std.dlog("Sending grant access to this node");
            getContext().actorSelection(memberUp.member().address() + "/user/storenode").tell(new GrantAccessToStoreMsg(self(), nodeNumber++), self());
        }
    }

    private void onRequestActivateMsg(RequestActivateMsg msg) {
        Logger.std.dlog("Request activate received from " + msg.getStoreNodeRef().path().name());
        partitionManager.addNode(msg.getStoreNodeRef());
        storeNodes.add(msg.getStoreNodeRef());

        //if system is running, adding it to the partition manager means updating every node possibly.
        //Notify every node of the new assignments and activate the one making this request
        if(partitionManager.isRunning()) {
            for (ActorRef node :
                    storeNodes) {
                if (node.equals(msg.getStoreNodeRef()))
                    node.tell(new ActivateNodeMsg(partitionManager.getNodesOfPartitionList(), true), self());
                else
                    node.tell(new UpdateStoreNodeStatusMsg(partitionManager.getNodesOfPartitionList()), self());
            }
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
            //load conf file
            Config conf = ConfigFactory.load("conf/cluster.conf")
                    .withValue("akka.remote.netty.tcp.port", ConfigValueFactory.fromAnyRef(0)) //
                    .withValue("akka.cluster.roles", ConfigValueFactory.fromIterable(Collections.singletonList("storeNode")));

            ActorSystem as = ActorSystem.create("ServerClusterSystem", conf);
            //create the new storeNode
            ActorRef newNode = as.actorOf(StoreNode.props(timeout.getSeconds()), "localStoreNode_"+j);
            //TODO set the storeManager to the nodes
            storeNodes.add(newNode);
            partitionManager.addNode(newNode);
        }
    }


    /**
     * Send the setup messages to all store nodes, containing info on their role and neighbor, to make them available.
     * This assumes that the storeNodesLists List is fully initialized and no node is yet operative.
     */
    private void sendSetupMessagesToStoreNodes() {
        Logger.std.dlog("Sending setup messages to nodes. nodesOfPartitionList is:\n" + partitionManager.getNodesOfPartitionList());
        for (ActorRef node : storeNodes) {
            node.tell(new ActivateNodeMsg(partitionManager.getNodesOfPartitionList(), false), self());
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
                CompletableFuture<Object> future = ask(storeNodes.get(i), new ClientAssignMsg(uid, assignedNodes), timeout).toCompletableFuture();
                incrementClientLoadTo(storeNodes.get(i));
                pipe(future, getContext().dispatcher()).to(sender());
            }
        } catch (Exception e) {
            e.printStackTrace();
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



    private void incrementClientLoadTo(ActorRef node) {
        nodeClientAmountMap.put(node, nodeClientAmountMap.get(node)+1);
    }


    public static Props props() {
        return Props.create(StoreManager.class);
    }
}
