package it.polimi.middleware.messages;

import java.io.Serializable;

/**
 * Generic Message class, used to identify if an incoming actor receives a message which belongs to the service use.
 * Messages which belongs to the key-value store service must extend this class
 */
public abstract class ServiceMessage implements Serializable {
}
