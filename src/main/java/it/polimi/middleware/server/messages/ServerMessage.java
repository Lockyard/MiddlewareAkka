package it.polimi.middleware.server.messages;

import it.polimi.middleware.interfaces.CborSerializable;

import java.io.Serializable;


/**
 * A generic message passed from and to server actors
 */
public abstract class ServerMessage implements Serializable {

    private static int DEFAULT_ALIVE_STEPS = 64;

    private int aliveSteps = DEFAULT_ALIVE_STEPS;

    /**
     * open the message. This method is supposed to be used before processing the message
     * @return true if the message should be kept alive, false otherwise
     */
    public boolean open() {
        return --aliveSteps > 0;
    }

    public static void setDefaultAliveSteps(int defaultAliveSteps) {
        DEFAULT_ALIVE_STEPS = defaultAliveSteps;
    }
}
