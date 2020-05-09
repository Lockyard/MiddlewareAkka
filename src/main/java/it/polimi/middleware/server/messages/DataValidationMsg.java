package it.polimi.middleware.server.messages;

/**
 * A message which contains a key, a value and a newness value to update and validate eventually a ValueData from
 * one StoreNode replica to another
 */
public class DataValidationMsg extends ServerMessage {
    private final String key, value;
    private final int newness;

    public DataValidationMsg(String key, String value, int newness) {
        this.key = key;
        this.value = value;
        this.newness = newness;
    }


    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public int getNewness() {
        return newness;
    }
}