package it.polimi.middleware.server.management;

import it.polimi.middleware.messages.GetMsg;
import it.polimi.middleware.messages.PutMsg;
import it.polimi.middleware.messages.ReplyGetMsg;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;

/**
 * This keeper holds put messages up to a certain size in chronological order of insertion,
 * and can retrieve an answer to a get if is in the history
 */
public class HistoryKeeper {

    private PutMsg[] messages;

    private int index;

    public HistoryKeeper(int capacity) {
        messages = new PutMsg[capacity];
        index = 0;
        Arrays.fill(messages, new PutMsg("0", "0"));
    }

    public void insertPutMessage(PutMsg msg) {
        messages[index] = msg;
        index = (index+1) % messages.length;
    }

    /**
     * Search for a put with same client ID and operation ID and retrieve that result
     * @return null if not found, a {@link ReplyGetMsg if found}
     */
    public ReplyGetMsg getDatumFromHistory(GetMsg msg) {
        for (int i = index; i >= 0 ; i--) {
            //if found a match
            if(msg.getClientID() == messages[i].getClientID() && msg.getClientOpID() == messages[i].getClientOpID()) {
                return new ReplyGetMsg(msg.getKey(), messages[i].getVal(), msg.getClientOpID());
            }
        }
        for (int i = messages.length; i > index; i--) {
            //if found a match
            if(msg.getClientID() == messages[i].getClientID() && msg.getClientOpID() == messages[i].getClientOpID()) {
                return new ReplyGetMsg(msg.getKey(), messages[i].getVal(), msg.getClientOpID());
            }
        }

        return null;
    }


}
