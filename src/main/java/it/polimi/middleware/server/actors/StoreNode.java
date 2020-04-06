package it.polimi.middleware.server.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.util.ArrayList;
import java.util.List;

public class StoreNode extends AbstractActor {

    private final int hashSpacePartition, partitionNumber, nodeNumber;
    //list of dataKeepers for this node
    private final List<ActorRef> dataKeepers;

    /**
     * New StoreNode with the specified hashPartition
     * @param hashSpacePartition indicates in how many parts the hashSpace was divided
     * @param partitionNumber indicates which part of the divided hashSpace is assigned to this node
     */
    public StoreNode(int hashSpacePartition, int partitionNumber, int nodeNumber) {
        this.hashSpacePartition = hashSpacePartition;
        this.partitionNumber = partitionNumber;
        this.nodeNumber = nodeNumber;

        Config conf = ConfigFactory.load("conf/store.conf");
        int dataKeepersPerNode = conf.getInt("store.node.dataKeepersPerNode");
        dataKeepers = new ArrayList<>(dataKeepersPerNode);

        //create its data keepers
        for (int i = 0; i < dataKeepersPerNode; i++) {
            //each data keeper sees a subspace of the partition assigned to this StoreNode. Thus, their hashSpacePartition is multiplied by
            //the number of data keepers.
            dataKeepers.add(getContext().actorOf(DataKeeper.props(hashSpacePartition*dataKeepersPerNode, i*hashSpacePartition+partitionNumber),
                    //name: dataKeeper_Px_Ny_Kz/a = Partition x, Node y, Keeper z/a (a = total data keepers of Node y, which has assigned partition x)
                    "dataKeeper_P"+partitionNumber+"_N"+nodeNumber+"_K"+i+"/"+dataKeepersPerNode)); //name of the data keeper node to identify it clearly
        }

    }


    @Override
    public Receive createReceive() {
        return null;
    }

    public static Props props(int hashSpacePartition, int partitionNumber, int nodeNumber) {
        return Props.create(StoreNode.class, hashSpacePartition, partitionNumber, nodeNumber);
    }
}
