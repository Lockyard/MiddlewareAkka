package it.polimi.middleware.messages;

import java.io.Serializable;

/**
 * Message used by client to introduce themselves to the server the first time
 */
public class GreetingMsg implements Serializable {

    /**
     * Useful for Java when serializing class. Better to assign a UID to the class than making it assigned by Java
     */
    private static final long serialVersionUID = 771240900123301L;

}
