package it.polimi.middleware.util;

public class Logger {

    public static final Logger std = new Logger();

    public enum LogLevel {
        VERBOSE(0), DEBUG(1), INFO(2), WARNING(3), ERROR(4), NONE(5);
        private int level;
        private LogLevel(int level) {
            this.level = level;
        }

        public int getLevel() {return level;}
    };

    private LogLevel logLevel = LogLevel.VERBOSE;

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

    /**
     * Log in debug level something
     */
    public void dlog(String debugLogMessage) {
        if(this.logLevel.getLevel() <= LogLevel.DEBUG.level)
            System.out.println(debugLogMessage);
    }

    /**
     * log in info level something with "[INFO] " preponed
     */
    public void ilog(String infoLogMessage) {
        if(this.logLevel.getLevel() <= LogLevel.INFO.level)
            System.out.println("[INFO] " + infoLogMessage);
    }


}
