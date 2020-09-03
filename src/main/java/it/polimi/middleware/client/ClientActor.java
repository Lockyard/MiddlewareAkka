package it.polimi.middleware.client;

import akka.actor.*;
import it.polimi.middleware.messages.*;
import it.polimi.middleware.util.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Actor which is used by the client app to communicate with the server.
 * Tells and receive messages to/from it.
 */
public class ClientActor extends AbstractActorWithStash {

    private ActorSelection server;
    private String serverAddress;
    private long clientID = 0;
    private boolean isConnected = false;
    /**
     * The only nodes this client can use to communicate to the server
     */
    private final List<ActorRef> accessNodes;

    //index used to do round-robin on the accessNodes
    private int rrIndex = 0;

    /**
     * A map where key is the partition and value is the operation id for that partition.
     * Is a progressive number increasing by 1 for each put.
     */
    private final HashMap<Integer, Integer> opIDPerPartition;

    private int numOfPartitions;


    public ClientActor(String serverAddress) {
        this.serverAddress = serverAddress;
        accessNodes = new ArrayList<>();
        opIDPerPartition = new HashMap<>();
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        server = getContext().actorSelection("akka.tcp://" + serverAddress + "/user/MasterNode");
        System.out.println("ClientActor Prestart, server is at path: " + "akka.tcp://" + serverAddress + "/user/MasterNode");
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                //if it receives a get or put message, tell it to the server as the sender
                .match(GetMsg.class, this::onGetMsg)
                .match(PutMsg.class, this::onPutMsg)
                //if it receives a reply, tell it to the client app
                .match(ReplyGetMsg.class, ClientApp::receiveGetReply)
                .match(ReplyPutMsg.class, ClientApp::receivePutReply)
                //greetings and reply
                .match(GreetingMsg.class, msg -> server.tell(msg, self()))
                .match(GreetingReplyMsg.class, this::onGreetingReplyMessage)
                //in case of access nodes terminated and new assigned
                .match(RequestNewActorReplyMsg.class, this::onRequestNewActorReplyMsg)
                .match(Terminated.class, this::onTerminatedAccessNode)
                .build();
    }


    private void onGetMsg(GetMsg msg) {
        if(accessNodes.isEmpty()) {
            Logger.std.dlog("Client doesn't have at the moment any access node. Stashing request");
            stash();
            return;
        }
        Logger.std.dlog("sending get message to " + accessNodes.get(rrIndex).path().address());
        msg.setClientID(clientID);
        msg.setClientOpID(getOpIdForKey(msg.getKey()));
        msg.setSender(self());
        accessNodes.get(rrIndex).tell(msg, self());
        roundRobin();
    }

    private void onPutMsg(PutMsg msg) {
        if(accessNodes.isEmpty()) {
            Logger.std.dlog("Client doesn't have at the moment any access node. Stashing request");
            stash();
            return;
        }

        msg.setClientID(clientID);
        msg.setSender(self());
        msg.setClientOpID(incrementAndGetOpIdForKey(msg.getKey()));
        Logger.std.dlog("sending put message to " + accessNodes.get(rrIndex).path().address() +
                " (opID:" + msg.getClientOpID()+")");
        accessNodes.get(rrIndex).tell(msg, self());
        roundRobin();
    }

    private void roundRobin() {
        rrIndex = (rrIndex +1) % accessNodes.size();
    }

    //if greeted with success, add the actor and watch it
    private void onGreetingReplyMessage(GreetingReplyMsg msg) {
        Logger.std.dlog("GreetingReply received");
        if(msg.isSuccessful()) {
            isConnected = true;
            clientID = msg.getClientID();
            accessNodes.add(sender());
            getContext().watch(sender());

            numOfPartitions = msg.getNumOfPartitions();
            for (int i = 0; i < numOfPartitions; i++) {
                opIDPerPartition.put(i, 0);
            }
            ClientApp.receiveGreetingReplyUpdate(isConnected, "Received store node address (" + sender().path().address()+")" +
                    " , " + accessNodes.size() + "/" + msg.getTotalAssignedActors());
        } else {
            ClientApp.receiveGreetingReplyUpdate(isConnected, "Greeting with server failed [" + msg.getDescription()+"]");
        }
    }

    private void onTerminatedAccessNode(Terminated t) {
        Logger.std.dlog("Client saw its node " +t.actor().path().address() +" terminated. Asking for " +
                "one other node");
        //remove it, if actually removed ask to the server another actor assignment
        if(accessNodes.remove(t.actor())) {
            server.tell(new RequestNewActorMsg(clientID, new ArrayList<>(accessNodes)), self());
        }
    }

    private void onRequestNewActorReplyMsg(RequestNewActorReplyMsg msg) {
        if(accessNodes.contains(sender())) {
            server.tell(new RequestNewActorMsg(clientID, new ArrayList<>(accessNodes)), self());
        } else {
            accessNodes.add(sender());
            Logger.std.ilog("Received new node. Assigned nodes:\n" + accessNodes);
        }
        if(accessNodes.size()>0) {
            unstashAll();
        }
    }


    private int getOpIdForKey(String key) {
        return opIDPerPartition.get(key.hashCode() % numOfPartitions);
    }

    private int incrementAndGetOpIdForKey(String key) {
        int k = key.hashCode() % numOfPartitions;
        opIDPerPartition.put(k, opIDPerPartition.get(k) + 1);
        return opIDPerPartition.get(k);
    }


    public static Props props(String serverAddress) {
        return Props.create(ClientActor.class, serverAddress);
    }
}
