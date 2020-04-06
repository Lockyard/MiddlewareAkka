package it.polimi.middleware.server.store;

import java.sql.Timestamp;

/**
 * The "value" element of the key-value store. Holds also information about its validity, since there are multiple copies which must be
 * coherent with client requests
 */
public class ValueData {
    private String content;
    private boolean isValid;

    public ValueData(String content, boolean isValid) {
        this.content = content;
        this.isValid = isValid;
    }

    public boolean isValid() {
        return isValid;
    }

    public void setValid(boolean valid) {
        isValid = valid;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}
