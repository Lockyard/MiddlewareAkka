package it.polimi.middleware.messages;

import akka.actor.ActorRef;

import java.io.Serializable;

/**
 * Reply of the greeting message. If successful, contains 1 actorRef and a number indicating how many
 * actors the client receiving the message should expect to be assigned to them.
 */
public class GreetingReplyMsg implements Serializable {

    /**
     * Useful for Java when serializing class. Better to assign a UID to the class than making it assigned by Java
     */
    private static final long serialVersionUID = 771240900123302L;

    private final boolean success;
    private ActorRef assignedActor;
    private int totalAssignedActors;
    private long clientID;




    private int numOfPartitions;

    private String description;

    /**
     * Unsuccessful reply with a description
     * @param description the description of the failure
     */
    public GreetingReplyMsg(String description) {
        success = false;
        this.description = description;
    }

    /**
     * Create the reply specifying the actor and the total actors assigned. This means that the greeting was successful
     * @param assignedActor the ref of one of the actor assigned
     * @param totalAssignedActors the total actors assigned that the client should expect to reply
     */
    public GreetingReplyMsg(ActorRef assignedActor, int totalAssignedActors, int numOfPartitions, long clientID) {
        this.assignedActor = assignedActor;
        this.totalAssignedActors = totalAssignedActors;
        this.numOfPartitions = numOfPartitions;
        this.clientID = clientID;
        success = true;
    }

    public boolean isSuccessful() {
        return success;
    }

    public ActorRef getAssignedActor() {
        return assignedActor;
    }

    public int getTotalAssignedActors() {
        return totalAssignedActors;
    }

    public String getDescription() {
        return description;
    }

    public long getClientID() {
        return clientID;
    }

    public int getNumOfPartitions() {
        return numOfPartitions;
    }
}
