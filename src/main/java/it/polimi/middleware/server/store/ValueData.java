package it.polimi.middleware.server.store;

import it.polimi.middleware.server.exceptions.DataInconsistencyException;

import java.io.Serializable;

/**
 * The "value" element of the key-value store. Holds also information about its validity, since there are multiple copies which must be
 * coherent with client requests
 */
public class ValueData implements Serializable {
    private String content;
    private boolean isValid;
    /**
     * newness is a value assigned by the leader replica and increased every time the value is updated by the leader replica
     */
    private int newness;

    //TODO add clientID and newness logic (and CC actor) to implement client consistency
    /**
     * Additional parameters to ensure client-centric consistency
     */
    private long clientID;
    private int clientDataNewness;

    public ValueData(String content, boolean isValid, long clientID, int clientDataNewness) {
        this.content = content;
        this.isValid = isValid;
        this.clientID = clientID;
        this.clientDataNewness = clientDataNewness;
        newness = 0;
    }

    private ValueData(ValueData otherVD) {
        this.content = otherVD.content;
        this.isValid = otherVD.isValid;
        this.newness = otherVD.newness;
        this.clientDataNewness = otherVD.clientDataNewness;
        this.clientID = otherVD.clientID;
    }

    /**
     * Indicates if this datum is valid or not. Useful since ValueData is thought to be used in a context of
     * data replication and asynchronous environment
     * @return true if the data is considered valid. If not, is possibly outdated.
     */
    public boolean isValid() {
        return isValid;
    }

    public void setValid(boolean valid) {
        isValid = valid;
    }

    /**
     * Get the content of this valueData
     * @return the String contained
     */
    public String getContent() {
        return content;
    }


    /**
     * Get the content for a specific client, considering its newness
     * @param clientID the id of the client
     * @param clientDataNewness the newness of the request from the point of view of client
     * @return the content if is consistent from the point of view of the client
     * @throws DataInconsistencyException if the client request is older than the datum inside this ValueData,
     * from its point of view (i.e. if he was the one who wrote the new datum first).
     */
    public String getContentForClient(long clientID, int clientDataNewness) throws DataInconsistencyException {
        //if the newness of incoming request is inferior, then throw the exception since this ValueData
        // doesn't have the correct datum anymore
        if(this.clientID == clientID && clientDataNewness < this.clientDataNewness) {
            throw new DataInconsistencyException();
        } else {
            return getContent();
        }
    }

    /**
     * Set a new content for this ValueData, if the newness of the new ValueData is higher than this one's.
     * @param newData the new ValueData from which to copy new values
     * @return true if the data was overridden with the new data. False if nothing changed.
     */
    public boolean updateValueDataIfNewer(ValueData newData) {
        if(newData.getNewness() < getNewness())
            return false;
        this.content = newData.content;
        this.newness = newData.newness;
        this.clientID = newData.clientID;
        this.clientDataNewness = newData.clientDataNewness;
        isValid = newData.isValid;
        return true;
    }

    /**
     * Update this ValueData specifying the client. This checks if the data received is new enough wrt the data contained
     * here. If the data contained here was written by a different client, the newness is not considered.
     * @param content the new content
     * @param clientID the clientID which is requesting to write the new content
     * @param clientDataNewness data newness wrt the client requesting it
     * @param isValid true if the data inserted is to be considered valid
     * @return true if the data was updated, false otherwise
     */
    public boolean updateValueDataForClient(String content, long clientID, int clientDataNewness, boolean isValid) {
        //if data here is of the same client but it's older, do not write anything
        if(clientID == this.clientID && clientDataNewness < getNewness()) {
            return false;

        //otherwise write the new content and update client, newness and validity information
        } else {
            this.content = content;
            newness++;
            this.clientID = clientID;
            this.clientDataNewness = clientDataNewness;
            this.isValid = isValid;
            return true;
        }
    }


    /**
     * Set new content for this ValueData, increase its newness and get a copy of it.
     * @param content the new content String of this data
     * @return a copy of this valueData
     */
    public ValueData setNewContent(String content) {
        this.content = content;
        newness++;
        return this.copy();
    }

    /**
     * Get the newness of this value data. If higher, the datum inside this object is more important/recent
     * than another in one other ValueData
     * @return the newness of this object
     */
    public int getNewness() {
        return newness;
    }


    public void resetNewness() {
        newness = 0;
    }

    public ValueData copy() {
        return new ValueData(this);
    }
}
