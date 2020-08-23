package it.polimi.middleware.server.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import it.polimi.middleware.messages.GreetingMsg;
import it.polimi.middleware.messages.RequestNewActorMsg;
import it.polimi.middleware.messages.ServiceMessage;
import it.polimi.middleware.server.messages.StartSystemMsg;
import it.polimi.middleware.server.messages.StartSystemReplyMsg;
import it.polimi.middleware.util.Logger;

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

    // Although we assume the master node to never fail, unsubscribe it from cluster on stop
    @Override
    public void postStop() {
        cluster.unsubscribe(self());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                //forward service messages to the storeManager
                .match(GreetingMsg.class, msg -> storeManager.forward(msg, getContext()))
                .match(RequestNewActorMsg.class, msg -> storeManager.forward(msg, getContext()))
                .match(StartSystemMsg.class, msg -> storeManager.tell(msg, self()))
                .match(StartSystemReplyMsg.class, this::onStartSystemReplyMsg)
                .build();
    }

    private void onStartSystemReplyMsg(StartSystemReplyMsg msg) {
        if(msg.isSuccessful()) {
            Logger.std.ilog("System started successfully! " +msg.getDescription());
        } else {
            Logger.std.ilog("System couldn't start! >> " + msg.getDescription());
        }
    }




    public static Props props() {
        return Props.create(MasterNode.class);
    }



}
