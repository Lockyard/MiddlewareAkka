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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The node underlying the MasterNode. Manages everything conceiving the store: underlying nodes with their workers,
 * establish node's data hierarchy, to update values in order to be consistent, and forward messages to the correct node.
 */
public class StoreManager extends AbstractActor {
    private final Cluster cluster = Cluster.get(getContext().system());

    private int hashSpacePartition;
    private int minimumDataCopies;

    /**
     * List of lists of storeNodes. Each list in the list contains all nodes belonging to the same partition.
     * Size of the external list will be the number of divisions of the hash space, while
     * internal sublists will have each all the nodes containing copies of the same data
     */
    private List<List<ActorRef>> storeNodesLists;

    public StoreManager() {
        //load from config file how much to divide hash space and how many copies of data must be active
        Config conf = ConfigFactory.load("conf/store.conf");
        hashSpacePartition = conf.getInt("store.hashSpacePartition");
        minimumDataCopies = conf.getInt("store.dataCopies");

        //prepare the list of lists with the specified capacities:
        //external list is size of the partition of space, each sublist is size of the number of copies
        storeNodesLists = new ArrayList<>(hashSpacePartition);
        for (int i = 0; i < minimumDataCopies; i++) {
            storeNodesLists.add(new ArrayList<>(minimumDataCopies));
        }
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
                //.match(GetMsg.class, this::tmpResponse)
                .build();
    }

    //Aa BB

    /**
     * Spawn locally all remaining nodes to have a functional store on with specifics specified on the store.conf file
     */
    private void localFillStoreRequirements() {
        for (int i = 0; i < hashSpacePartition; i++) {
            //for each partition, fill its list up to #dataCopies members
            for (int j = storeNodesLists.get(i).size(); j < minimumDataCopies; j++) {
                //load conf file
                Config conf = ConfigFactory.load("conf/cluster.conf")
                        .withValue("akka.remote.netty.tcp.port", ConfigValueFactory.fromAnyRef(0)) //
                        .withValue("akka.cluster.roles", ConfigValueFactory.fromIterable(Collections.singletonList("storeNode")));

                ActorSystem as = ActorSystem.create("ServerClusterSystem", conf);
                //create the new storeNode. Name: storeNode_Px_Cy, where Px indicates Partition x, Ny indicates Node number y
                storeNodesLists.get(i).add(as.actorOf(StoreNode.props(hashSpacePartition, i, j), "storeNode_P"+i+"_N"+j));
            }
        }
    }


    public static Props props() {
        return Props.create(StoreManager.class);
    }
}
