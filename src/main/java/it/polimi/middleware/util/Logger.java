package it.polimi.middleware.util;

public class Logger {

    public static final Logger std = new Logger();

    public enum LogLevel {
        VERBOSE(0), DEBUG(1), WARNING(2), ERROR(3), NONE(4);
        private int level;
        private LogLevel(int level) {
            this.level = level;
        }

        public int getLevel() {return level;}
    };

    private LogLevel logLevel = LogLevel.DEBUG;

    public void setLogLevel(LogLevel logLevel) {
        this.logLevel = logLevel;
    }

    /**
     * Log something with the specified loglevel
     * @param logLevelValue the value of log level. Visible in Logger.LogLevel class
     * @param logMessage the message
     */
    public void log(int logLevelValue, String logMessage) {
        if(logLevelValue >= this.logLevel.getLevel())
            System.out.println(logMessage);
    }

    /**
     * Log something with the specified loglevel
     */
    public void log(LogLevel logLevel, String logMessage) {
        if(logLevel.getLevel() >= this.logLevel.getLevel())
            System.out.println(logMessage);
    }


}
