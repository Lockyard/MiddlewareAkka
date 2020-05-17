package it.polimi.middleware.server.store;

import java.io.Serializable;

/**
 * The "value" element of the key-value store. Holds also information about its validity, since there are multiple copies which must be
 * coherent with client requests
 */
public class ValueData implements Serializable {
    public static final int DEFAULT_NEWNESS = 0;

    private String value;
    /**
     * newness is a value assigned by the leader replica and increased every time the value is updated by the leader replica
     */
    private long newness;

    /**
     * Create a ValueData with specified value and newness 0
     * @param value its contained value
     */
    public ValueData(String value) {
        this.value = value;
        newness = DEFAULT_NEWNESS;
    }

    private ValueData(ValueData otherVD) {
        this.value = otherVD.value;
        this.newness = otherVD.newness;
    }



    /**
     * Set a new value for this ValueData, if the newness of the new ValueData is higher than this one's.
     * @param newData the new ValueData from which to copy new values
     * @return true if the data was overridden with the new data. False if nothing changed.
     */
    public boolean updateIfNewer(ValueData newData) {
        if(newData.getNewness() < getNewness())
            return false;
        this.value = newData.value;
        this.newness = newData.newness;
        return true;
    }

    /**
     * Set a new value for this ValueData, if the newness of the new ValueData is higher than this one's.
     * @param newValue the new value to write
     * @param newness the newness of the new value
     * @return true if the data was overridden with the new data. False if nothing changed.
     */
    public boolean updateIfNewer(String newValue, long newness) {
        if(newness < getNewness())
            return false;
        this.value = newValue;
        this.newness = newness;
        return true;
    }

    /**
     * Update this ValueData with a new one, both as value and newness
     * @param newData the new ValueData
     */
    public void update(ValueData newData) {
        this.value = newData.value;
        this.newness = newData.newness;
    }

    /**
     * Get the value of this valueData
     * @return the String contained
     */
    public String getValue() {
        return value;
    }

    /**
     * Get the newness of this value data. If higher, the datum inside this object is more important/recent
     * than another in one other ValueData
     * @return the newness of this object
     */
    public long getNewness() {
        return newness;
    }

    public void resetNewness() {
        newness = 0;
    }

    public ValueData copy() {
        return new ValueData(this);
    }
}
