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
import it.polimi.middleware.messages.GetMsg;
import it.polimi.middleware.messages.PutMsg;
import it.polimi.middleware.server.messages.UpdateStoreNodeStatusMsg;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * The node underlying the MasterNode. Manages everything conceiving the store: underlying nodes with their workers,
 * establish node's data hierarchy, to update values in order to be consistent, and forward messages to the correct node.
 */
public class StoreManager extends AbstractActor {
    private final Cluster cluster = Cluster.get(getContext().system());

    //In how many partitions the key space is divided
    private final int hashSpacePartition;
    //for each partition, minimum amount of node replicas of the same data there must be
    private final int minimumDataReplicas;

    private final Random r;

    /**
     * List of lists of storeNodes. Each list in the list contains all nodes belonging to the same partition.
     * Size of the external list will be the number of divisions of the hash space, while
     * internal sublists will have each all the nodes containing replicas of the same data
     */
    private final List<List<ActorRef>> storeNodesLists;

    public StoreManager() {
        //load from config file how much to divide hash space and how many replicas of data must be active
        Config conf = ConfigFactory.load("conf/store.conf");
        hashSpacePartition = conf.getInt("store.hashSpacePartition");
        minimumDataReplicas = conf.getInt("store.dataReplicas");

        //prepare the list of lists with the specified capacities:
        //external list is size of the partition of space, each sublist is size of the number of replicas
        storeNodesLists = new ArrayList<>(hashSpacePartition);
        for (int i = 0; i < hashSpacePartition; i++) {
            storeNodesLists.add(new ArrayList<>(minimumDataReplicas));
        }

        r = new Random(System.currentTimeMillis());

        localFillStoreRequirements();
    }

    // Subscribe to cluster
    @Override
    public void preStart() {
        cluster.subscribe(self(), ClusterEvent.initialStateAsEvents(), ClusterEvent.MemberUp.class);
    }

    // Re-subscribe when restart
    @Override
    public void postStop() {
        cluster.unsubscribe(self());
    }


    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(GetMsg.class, this::onGetMessage)
                .match(PutMsg.class, this::onPutMessage)
                .build();
    }

    //Aa BB

    /**
     * Spawn locally all remaining nodes to have a functional store on with specifics specified on the store.conf file
     */
    private void localFillStoreRequirements() {
        for (int i = 0; i < hashSpacePartition; i++) {
            //for each partition, fill its list up to #dataReplicas members
            for (int j = storeNodesLists.get(i).size(); j < minimumDataReplicas; j++) {
                //load conf file
                Config conf = ConfigFactory.load("conf/cluster.conf")
                        .withValue("akka.remote.netty.tcp.port", ConfigValueFactory.fromAnyRef(0)) //
                        .withValue("akka.cluster.roles", ConfigValueFactory.fromIterable(Collections.singletonList("storeNode")));

                ActorSystem as = ActorSystem.create("ServerClusterSystem", conf);
                //create the new storeNode. Name: storeNode_Px_Cy, where Px indicates Partition x,
                //Ny indicates Node number y (of that partition). The replica is leader only if j == 0, which is the first created
                storeNodesLists.get(i).add(as.actorOf(StoreNode.props(hashSpacePartition, i, j, j==0, self()), "storeNode_P"+i+"_N"+j));
            }
        }

        sendSetupMessagesToStoreNodes();
    }

    /**
     * Send the setup messages to all store nodes, containing info on their role and neighbor, to make them available.
     * This assumes that the storeNodesLists List is fully initialized and no node is already operative.
     */
    private void sendSetupMessagesToStoreNodes() {

        //for each partition (= for each set of replicas)
        for (int i = 0; i < storeNodesLists.size(); i++) {

            //update the first node, the leader one
            if(storeNodesLists.get(i).size() > 0) {
                //if is the only one in the list, is leader and last and has no neighbors.
                //also, in this case skip the rest of the loop, since there are no other nodes
                if(storeNodesLists.get(i).size() == 1) {
                    storeNodesLists.get(i).get(0).tell(new UpdateStoreNodeStatusMsg(ActorRef.noSender(), ActorRef.noSender(),
                            true, true, false, ActorRef.noSender()), self());
                    continue;
                }
                //otherwise is a leader with at least 1 replica under it.
                else
                    storeNodesLists.get(i).get(0).tell(new UpdateStoreNodeStatusMsg(ActorRef.noSender(), storeNodesLists.get(i).get(1),
                            true, false, false, ActorRef.noSender()), self());
            }

            //update all the others, except the last
            for (int j = 1; j < storeNodesLists.get(i).size()-1; j++) {
                storeNodesLists.get(i).get(j).tell(new UpdateStoreNodeStatusMsg(storeNodesLists.get(i).get(j-1),
                        storeNodesLists.get(i).get(j+1), false, false, false, ActorRef.noSender()), self());
            }
            //update the last node
            storeNodesLists.get(i).get(storeNodesLists.get(i).size()-1).tell(new UpdateStoreNodeStatusMsg(
                    storeNodesLists.get(i).get(storeNodesLists.size()-2), ActorRef.noSender(), false, true, false, ActorRef.noSender()),
                    self()
            );
        }
    }

    /**
     * On get message forward it to the correct partition, to one among the available replicas,
     * keeping the workload balanced
     * @param msg the get message incoming
     */
    public void onGetMessage(GetMsg msg) {
        int assignedPartition = msg.getKey().hashCode() % hashSpacePartition;
        //take a random node in the assigned partition of the key string in the get message
        storeNodesLists.get(assignedPartition).get(r.nextInt(storeNodesLists.get(assignedPartition).size()))
                .forward(msg, getContext());
    }

    /**
     * On put message, forward it to the leader of the partition
     * @param msg the put message incoming
     */
    public void onPutMessage(PutMsg msg) {
        int assignedPartition = msg.getKey().hashCode() % hashSpacePartition;
        storeNodesLists.get(assignedPartition).get(0).forward(msg, context());
    }


    public static Props props() {
        return Props.create(StoreManager.class);
    }
}
