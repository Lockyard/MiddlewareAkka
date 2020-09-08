package it.polimi.middleware.client;

import it.polimi.middleware.messages.GetMsg;
import it.polimi.middleware.messages.PutMsg;
import it.polimi.middleware.messages.ServiceMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class CommandParser {

    /**
     * parse a line of command and get a list of service messages to send to the server
     * @param line
     * @return
     */
    public static List<ServiceMessage> parseLine(String line) {

        if(line.startsWith("h")) {
            System.out.println("COMMANDS:\n" +
                    "put k v\t-> put in key k the value v\n" +
                    "get k\t-> get the value in key k\n" +
                    "getr k1 k2\t-> get all the values between k1 and k2 (both included). k1 and k2 must be numbers\n" +
                    "putr k1 k2 v\t-> put value v in all the keys from k1 to k2 (both included). k1 and k2 must be numbers" +
                    "Separate more command with a line to perform multiple commands, e.g:\n" +
                    "put fookey barvalue,get fookey");//
            //return an empty list
            return new ArrayList<>();
        }
        try {
            StringTokenizer commandTokenizer = new StringTokenizer(line, ",", false);
            ArrayList<ServiceMessage> messages = new ArrayList<>(commandTokenizer.countTokens());
            while (commandTokenizer.hasMoreElements()) {
                messages.addAll(parseSingleCommand(commandTokenizer.nextToken()));
            }
            return messages;

        } catch (Exception e) {
            System.out.println("Unkown command. Type \"help\" of \"h\" to see usage");
            return new ArrayList<>();
        }

    }

    /**
     * Parse a single command
     * @return list of messages obtained by parsing the command
     */
    private static List<ServiceMessage> parseSingleCommand(String line) {
        try {
            ArrayList<ServiceMessage> messages = new ArrayList<>();
            StringTokenizer argumentTokenizer = new StringTokenizer(line, " ");
            //1st arg
            if(argumentTokenizer.hasMoreTokens()) {
                String arg1 = argumentTokenizer.nextToken();
                arg1 = arg1.toLowerCase();
                //no error check on anything, since is all in a try case with generic error message print in case
                switch (arg1) {
                    case "get":
                        messages.add(new GetMsg(argumentTokenizer.nextToken()));
                        break;
                    case "put":
                        messages.add(new PutMsg(argumentTokenizer.nextToken(), argumentTokenizer.nextToken()));
                        break;
                    case "getr":
                    case "getrange":
                        int baseI = Integer.parseInt(argumentTokenizer.nextToken());
                        int maxI = Integer.parseInt(argumentTokenizer.nextToken());
                        for (int i = baseI; i <= maxI; i++) {
                            messages.add(new GetMsg(Integer.toString(i)));
                        }
                        break;
                    case "putr":
                    case "putrange":
                        baseI = Integer.parseInt(argumentTokenizer.nextToken());
                        maxI = Integer.parseInt(argumentTokenizer.nextToken());
                        String value = argumentTokenizer.nextToken();
                        for (int i = baseI; i <= maxI; i++) {
                            messages.add(new PutMsg(Integer.toString(i), value));
                        }
                        break;

                    default:
                        unrecognizedCommand(line);
                }
            }

            return messages;
        } catch (Exception e) {
            System.out.println("Error while parsing command: \"" + line + "\". Messages of this command were not sent." +
                    " Type help to get how to format commands.");
            return new ArrayList<>();
        }

    }

    private static void unrecognizedCommand(String command) {
        System.out.println("\""+command+"\" was not recognized as a command and was skipped");
    }
}
