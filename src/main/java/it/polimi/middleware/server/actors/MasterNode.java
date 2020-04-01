package it.polimi.middleware.server.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import it.polimi.middleware.messages.ServiceMessage;

public class MasterNode extends AbstractActor {
    private ActorRef storeManager;

    private final Cluster cluster = Cluster.get(getContext().system());

    // Subscribe to cluster
    @Override
    public void preStart() throws Exception {
        super.preStart();
        storeManager = getContext().actorOf(StoreManager.props());
        cluster.subscribe(self(), ClusterEvent.initialStateAsEvents(), ClusterEvent.MemberUp.class);
    }

    // Although we assume the master node to never fail, unsuscribe it from cluster on stop
    @Override
    public void postStop() {
        cluster.unsubscribe(self());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                //forward service messages to the storeManager
                .match(ServiceMessage.class, msg -> storeManager.forward(msg, getContext()))
                .build();
    }

    public static Props props() {
        return Props.create(MasterNode.class);
    }
}
