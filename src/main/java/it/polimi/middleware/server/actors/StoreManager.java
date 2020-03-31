package it.polimi.middleware.server.actors;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import it.polimi.middleware.messages.GetMsg;
import it.polimi.middleware.messages.ReplyGetMsg;

public class StoreManager extends AbstractActor {
    private final Cluster cluster = Cluster.get(getContext().system());

    // Subscribe to cluster
    @Override
    public void preStart() {
        cluster.subscribe(self());
    }

    // Re-subscribe when restart
    @Override
    public void postStop() {
        cluster.unsubscribe(self());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(GetMsg.class, this::tmpResponse)
                .build();
    }

    private void tmpResponse(GetMsg msg) {
        sender().tell(new ReplyGetMsg("Received get msg with id: " + msg.getKey()), self());
    }

    public static Props props() {
        return Props.create(StoreManager.class);
    }
}
