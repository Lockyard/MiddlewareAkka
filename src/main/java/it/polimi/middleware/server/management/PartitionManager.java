package it.polimi.middleware.server.management;

import akka.actor.ActorRef;
import it.polimi.middleware.server.exceptions.NotEnoughNodesException;
import it.polimi.middleware.util.Logger;

import java.util.*;

public class PartitionManager {

    /** number of partitions **/
    private int partitions;
    /** number of minimum replicas, R parameter **/
    private int nReplicas;

    private boolean running;

    /**
     * This list contains, for a given index, which is a partition index, the nodes which have that datum, in order
     * of leadership on that datum
     * example:
    0   N0, N3, N12
    1   N0, N3, N9
    2   N0, N3, N9
    3   N1, N2, N5
    ...
     * N0 is leader for keys with value 0, 1 and 2 in module
     */
    private List<List<ActorRef>> nodesOfPartition;

    /**
     * This is the inverse of nodes of module: given a node, get a list of the modules on which that node was working
     */
    private HashMap<ActorRef, List<Integer>> partitionsOfNode;

    private List<ActorRef> nodes;


    /**
     *
     * @param partitions number of partitions to divide the keyspace. must be PoT, or will be rounded
     * @param nReplicas must be > 0
     */
    public PartitionManager(int partitions, int nReplicas){
        //round partitions to a PoT with int math
        this.partitions = (partitions/2)*2;
        //set at least 1 minReplicas
        this.nReplicas = Math.max(nReplicas,1);

        nodesOfPartition = new ArrayList<>(partitions);
        for (int i = 0; i < partitions; i++) {
            nodesOfPartition.add(i, new ArrayList<ActorRef>());
        }
        partitionsOfNode = new HashMap<>(partitions * nReplicas);
        nodes = new ArrayList<>();
        running = false;
    }







    /**
     * Start the partition manager. This will make it assign the nodes it has registered the partitions.
     * After this, it will suppose to be running, meaning that when adding and removing a node it will change
     * everytime other assignments to partitions
     */
    public void start() throws NotEnoughNodesException {
        if(nodes.size() < nReplicas)
            throw new NotEnoughNodesException(nReplicas, nodes.size());

        int baseNodesPerR = nodes.size()/ nReplicas;
        int leftOutNodes = nodes.size() - (baseNodesPerR * nReplicas);

        int partitionsPerNode = partitions/baseNodesPerR;
        int leftOutPartitions = partitions - baseNodesPerR * partitionsPerNode;
        int partitionIndex = 0;

        //set basic nodes, baseNodesPerR for each replica
        for (int i = 0; i < baseNodesPerR; i++) {
            int assignedPartitions = leftOutPartitions == 0 ? partitionsPerNode : partitionsPerNode + 1;

            for (int j = 0; j < nReplicas; j++) {
                ActorRef nodeToSetup = nodes.get(i+j*baseNodesPerR);

                //here we are assigning to a single node a range of indexes of partitions
                for (int k = partitionIndex; k < partitionIndex + assignedPartitions; k++) {
                    blindAssignPartitionToNode(k, nodeToSetup);
                }
            }
            //update partition Index
            partitionIndex += partitionsPerNode;
            if(leftOutPartitions > 0) {
                leftOutPartitions--;
                partitionIndex++;
            }
        }

        running = true;

        Logger.std.dlog("left out nodes = " + leftOutNodes);
        //now introduce the new nodes like they were added a running time
        for (int i = nodes.size() - leftOutNodes; i < nodes.size(); i++) {
            giveLoadToNewNode(nodes.get(i));
        }

    }


    public void addNode(ActorRef newNode) {
        //if is actually a new node, add it
        if(!nodes.contains(newNode)) {
            partitionsOfNode.put(newNode, new ArrayList<>());
            nodes.add(newNode);

            //if the partition manager is running, try to get partitions from various nodes to distribute the load
            if(running) {
                giveLoadToNewNode(newNode);
            }

        }
    }

    private void giveLoadToNewNode(ActorRef newNode) {
        //the load of the node that will be added, in terms of n of partitions assigned
        int expectedLoadOfNewNode = (partitions * nReplicas) / nodes.size();

        //a list of nodes, sorted by load descending, excluding the newNode
        List<ActorRef> nodesByLoad = new ArrayList<>(nodes);
        nodesByLoad.remove(newNode);
        nodesByLoad.sort(Comparator.comparingInt(this::loadOfNode).reversed());

        int currMaxLoad;
        boolean hasTakenSomeLoadThisCycle = true;

        //put on the new node all the load it can get without loading itself more than expected
        while (hasTakenSomeLoadThisCycle && loadOfNode(newNode) < expectedLoadOfNewNode) {
            //resort nodes, recalculate maxLoad
            nodesByLoad.sort(Comparator.comparingInt(this::loadOfNode).reversed());
            currMaxLoad = currentMaxLoad();
            hasTakenSomeLoadThisCycle = false;
            //take 1 from every node with max load, stop if reaching the load of the new node
            for (int i = 0; i < nodesByLoad.size() && loadOfNode(newNode) < expectedLoadOfNewNode; i++) {
                //if we reach nodes with less than max load then stop the cycle
                if(loadOfNode(nodesByLoad.get(i)) != currMaxLoad && hasTakenSomeLoadThisCycle)
                    break;
                //move load from curr node to new node, in the expected amount or less if reaching the expectedLoad
                int gainedLoad = moveLoadFromNodeToNode(nodesByLoad.get(i), newNode, 1);
                if(gainedLoad > 0)
                    hasTakenSomeLoadThisCycle = true;

            }
        }
    }



    /**
     *
     * It must be used over all the nodes present in the manager consistently wrt to a node added or failing
     * @param giverNode the node which gives the load to the other
     * @param takerNode the node which takes the load from the other
     * @param amount how much load to take/give. Must be positive smaller/equal than the partitions assigned to the giver node
     * @return the amount of load moved. Can go from 0 if no partition can be moved from one node to the other, up to the given amount
     */
    private int moveLoadFromNodeToNode(ActorRef giverNode, ActorRef takerNode, int amount) {
        if(amount <= 0)
            return 0;
        int originalAmount = amount;
        List<Integer> partitionsOfGiver = partitionsOfNode.get(giverNode);
        List<Integer> partitionsOfTaker = partitionsOfNode.get(takerNode);
        Logger.std.dlog(giverNode.path().name() + " giving " +amount + " partitions to " +takerNode.path().name());
        for (int i = 0; i < partitionsOfGiver.size() && amount > 0; i++) {
            //if the taker doesn't have yet that partition assigned, take it and reduce the amount to take
            if(!partitionsOfTaker.contains(partitionsOfGiver.get(i))) {
                int selectedPartition = partitionsOfGiver.get(i);
                //move the partition selected from giver to taker
                movePartitionFromNodeToNode(giverNode, takerNode, selectedPartition);
                amount--;
            }
        }

        if(amount!= 0) {
            Logger.std.log(Logger.LogLevel.WARNING, "Some amount of load remained while! -> " + amount);
        }
        return originalAmount-amount;
    }


    private void movePartitionFromNodeToNode(ActorRef giverNode, ActorRef takerNode, int selectedPartition) {
        nodesOfPartition.get(selectedPartition).remove(giverNode);
        nodesOfPartition.get(selectedPartition).add(takerNode);
        partitionsOfNode.get(giverNode).remove(Integer.valueOf(selectedPartition));
        partitionsOfNode.get(takerNode).add(selectedPartition);
    }


    private void blindAssignPartitionToNode(int partition, ActorRef node) {
        nodesOfPartition.get(partition).add(node);
        partitionsOfNode.get(node).add(partition);
    }

    private int loadOfNode(ActorRef node) {
        return partitionsOfNode.get(node).size();
    }

    public int currentMaxLoad() {
        //calculate new max load
        int currMaxLoad = 0;
        for (ActorRef node : nodes) {
            currMaxLoad = Math.max(currMaxLoad, loadOfNode(node));
        }
        return currMaxLoad;
    }

    public int currentMinLoadExcluding(ActorRef excludedNode) {
        //calculate new max load
        int currMinLoad = Integer.MAX_VALUE;
        for (ActorRef node : nodes) {
            if(node.compareTo(excludedNode) == 0)
                continue;
            currMinLoad = Math.min(currMinLoad, loadOfNode(node));
        }
        return currMinLoad;
    }


    public String toString() {
        return toStringNodesOfPartition();
    }

    public String toStringPartitionsOfNode() {
        StringBuilder sb = new StringBuilder();
        sb.append("PartitionManager Nodes -> Partitions: -------------------------\n");
        for (Map.Entry<ActorRef, List<Integer>> entry : partitionsOfNode.entrySet()) {
            sb.append("" + entry.getKey().path().name() + ", - P:" + entry.getValue() + " (" + loadOfNode(entry.getKey()) + ")\n");
        }
        sb.append("-------------------------------------------------------------");
        return sb.toString();
    }

    public String toStringNodesOfPartition() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < nodesOfPartition.size(); i++) {
            sb.append("P:" + i + " - " + nodesOfPartition.get(i)+ "\n");
        }
        return sb.toString();
    }

    public String toStringListOfNodes() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < nodes.size(); i++) {
            sb.append(nodes.get(i).path().name() + " (" + loadOfNode(nodes.get(i)) + "), ");
            if(i%4 == 3)
                sb.append("\n");
        }
        return sb.toString();
    }


    public void sortNodesByLoad() {
        nodes.sort(Comparator.comparingInt(this::loadOfNode).reversed());
    }

}
