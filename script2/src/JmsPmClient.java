//
// Implements a simple client which connects to 5620 SAM using JMS and once 
// connected invokes a script to do a set of registerLogToFile SAM-O API calls.
//
// The JMS connection is maintained to ensure the registerLogToFile calls do 
// not timeout.
//
// If any issue arises, such as being unable to connect to SAM or too many missed 
// heartbeats, this program will exit. It is designed with the intent that it be 
// monitored by a script that will restart it if it is not running.
//

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Hashtable;
import java.io.InputStream;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

public class JmsPmClient extends Thread implements ExceptionListener, MessageListener
{
  //
  // Some compile-time 'settings'.
  //

  // Number of milliseconds before we give up on receiving a JMS heartbeat once we 
  // are connected and registered.
  private static final int HEARTBEAT_TIMEOUT = 60000;

  // Number of milliseconds before we give up on the registerLogToFile call completing.
  private static final int REGISTRATION_TIMEOUT = 240000;

  // Number of milliseconds before we give up on this JMS client connected to SAM.
  private static final int CONNECTION_TIMEOUT = 240000;

  //
  // Variables.
  //

  // Singleton JMS client.
  private static JmsPmClient jmsClient = null;

  // Process used to execute the registerLogToFile script - make it availble to CheckTimer.
  public static Process regProcess = null;
  
  // Support for 5.0 external JMS context.
  private static final String JMS_CONTEXT = "external/5620SamJMSServer";

  // The connection factory.
  protected static final String CONNECTION_FACTORY = "SAMConnectionFactory";

  // The port seperator character for initial context construction.
  protected static final char PORT_SEP_CHAR = ':';

  // The URL seperator character for initial context construction.
  protected static final char URL_SEP_CHAR = '/';

  // The seperator character for initial context construction.
  private static final char MULTI_SEP_CHAR = ',';

  // This is the topic string name.
  protected String strName;

  // This is the URL string for a topic.
  protected String strUrl;

  // This is the port for the topic.
  protected String port;

  // This is the high availability URL string for a topic.
  protected String strHaUrl;

  // This is the high availability port for the topic.
  protected String haPort;

  // This is the client id for the topic.
  protected String clientId;

  // This is the accepted client id for the topic. This attribute
  // contains the client id that was successfully registered with the server.
  protected String acceptedClientId;

  // This is the user name for the topic.
  protected String user;

  // This is the password of the user for the topic.
  protected String password;

  // This is the message selector for the topic.
  protected String filter;
  
  // This is the password of the user for the topic.
  protected String pLFile;

  // This identifies if the consumer is listening to the topic.
  protected boolean isListening;

  // This identifies if the consumer is connected.
  protected boolean isConnected = false;

  // This identifies if the registerLogToFile has completed.
  protected boolean isRegistered = false;

  // This identifies if the consumer has been stopped.
  protected boolean isStopped = false;

  // The JNDI context of the connection.
  private Context jndiContext = null;

  // The topic connection factory.
  private TopicConnectionFactory topicConnectionFactory = null;

  // The topic connection.
  private TopicConnection topicConnection = null;

  // The topic session.
  private TopicSession topicSession = null;

  // The topic.
  private Topic topic = null;

  // The topic subscriber.
  private TopicSubscriber topicSubscriber = null;

  // The topic subscriber.
  private Boolean isPersistent =false; 

  // Identifies if high availability is enabled.
  private boolean isHaEnabled = false;

  // Counter for total number of messages.
  private static int counter = 1;

  // The timestamp of the last heartbeat.
  private long lastHeartbeatMs = 0;

  // Used to call CheckTask regularly.
  Timer checkTimer = null;

  // Singleton logger.
  public static Logger aLogger = null;

  // The format of the timestamp in the log messages.
  static private SimpleDateFormat aLogFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss.SSS-zzz");

  static class Logger {
    FileOutputStream aFileOut = null;

    public void init() {
      try {
        if (aFileOut == null) {
          aFileOut = new FileOutputStream("JmsPmClient.log.txt", true);
        }
     } catch (java.io.FileNotFoundException e) {
       // Not much we can do, we're reporting errors in the log file.
     }
    }

    public void log(String pMsg) {
      String aLog;
      Date aNow;

      aNow = new Date();

      aLog = aLogFormat.format(aNow) + " " + pMsg;

      try {
        if (aFileOut != null) {
          aFileOut.write(aLog.getBytes());
        }
      } catch (java.io.IOException e) {
        // Not much we can do, we're reporting errors in the log file.
      }
    }
  }

  class CheckTask extends TimerTask {
    public void run() {
      if (isRegistered) {
        // LOG
        System.out.println("- tick " + ((System.currentTimeMillis() - lastHeartbeatMs) / 1000) + "s");

        // Give up, too long without a heartbeat.
        if ((System.currentTimeMillis() - lastHeartbeatMs) > HEARTBEAT_TIMEOUT) {
          System.out.println("Missed heartbeat(s), exiting");

          if (aLogger != null) {
            aLogger.log("ERROR Missed heartbeat(s), exiting\n");
          }

          if (regProcess != null) {
            regProcess.destroy();
          }

          System.exit(1);
        }
      } else if (isConnected) {
        System.out.println("- tick (waiting for registration)");

        // Give up, too long without a connection.
        if ((System.currentTimeMillis() - lastHeartbeatMs) > REGISTRATION_TIMEOUT) {
          System.out.println("- unable to register, exiting");

          if (aLogger != null) {
            aLogger.log("ERROR Unable to register, exiting\n");
          }

          if (regProcess != null) {
            regProcess.destroy();
          }

          System.exit(1);
        }
      } else {
        // LOG
        System.out.println("- tick (waiting for connection)");

        // Give up, too long without a connection.
        if ((System.currentTimeMillis() - lastHeartbeatMs) > CONNECTION_TIMEOUT) {
          System.out.println("Unable to connect, exiting");

          if (aLogger != null) {
            aLogger.log("ERROR Unable to connect to SAM, exiting\n");
          }

          if (regProcess != null) {
            regProcess.destroy();
          }

          System.exit(1);
        }
      }
    }
  }

  //
  // Contruct the JmsPmClient.
  //
  public JmsPmClient(String aInTopic, String aInUrl, String aInHaUrl, String aInId, String aInUser, 
    String aInPassword, String aInFilter, String pLFile, Boolean aIsPersistent)
  {
    String[] lUrlPort = aInUrl.split(":");
    strName = aInTopic;
    strUrl = lUrlPort[0];
    port = lUrlPort[1];
    clientId = aInId;
    this.pLFile = pLFile;
    user = aInUser;
    password = aInPassword;
    filter = aInFilter;
    isPersistent = aIsPersistent;

    if (aInHaUrl != null) {
      String[] lHaUrlPort = aInHaUrl.split(":");
      isHaEnabled = true;
      haPort = lHaUrlPort[1];
      strHaUrl = lHaUrlPort[0];
    }

    // Start our verification task.
    if (checkTimer == null) {
      checkTimer = new Timer();
      checkTimer.scheduleAtFixedRate(new CheckTask(), 10000, 10000);
    }
  }

  //
  // Process an incoming JMS message.
  //
  public void onMessage(Message aInMessage)
  {
    int start;
    int end;

    try {
      // LOG
      //System.out.println("Event " + counter);

      if (aInMessage instanceof TextMessage) {
        String aMessage;

        // LOG
        //System.out.println("TextMessage " + counter + ": "+ ((TextMessage) aInMessage).getText());

        aMessage = ((TextMessage) aInMessage).getText();
        System.out.println("message is "  + aMessage);

        if (aMessage.indexOf("<MTOSI_NTType>NT_HEARTBEAT</MTOSI_NTType>") >= 0) {
          //
          // Process the heartbeast message.
          //

          // We must be connected if we received a JMS heartbeat.
          isConnected = true;

          if ((isConnected) && (isRegistered)) {
            // Heartbeats only count if we're connected and registered.
            lastHeartbeatMs = System.currentTimeMillis();
          }
        } else if (aMessage.indexOf("<ALA_eventName>LogFileAvailable</ALA_eventName>") >= 0) {
          //
          // Process the accounting stats file available message.
          //

          System.out.println("LogFileAvaialable");
          // Find the start and end of the filename.
          start = aMessage.indexOf("<fileName>");
          if (start > 10) {
            start += 10;
            end = aMessage.indexOf("</fileName>");
          } else {
            end = 0;
          }

          // LOG
          if (end > 0) {
            System.out.println("- " + aMessage.substring(start, end));
          } else {
            System.out.println("- ERROR unknown file available");
          }
        }

        counter++;
      } else if (aInMessage instanceof ObjectMessage) {
        // In SAM 5.0, TextMessages are encapsulated in Object Messages.  The following
        // code allows the TextMessage to be unwrapped and processed as normal.
        // This will allow backwards compatability with previous versions of SAM.
        // NOTE: JMS header properties are contained in the Object Message, not the
        //       encapsulated TextMessage.

        Object lObject = ((ObjectMessage) aInMessage).getObject();

        System.out.println("compatibility");
        // LOG
        //System.out.println("ObjectMessage " + counter);

        if (lObject != null && lObject instanceof Message) {
          onMessage((Message) lObject);
        }
      } else {
        // LOG
        System.out.println("Invalid Message Type.");
      }
    } catch (Throwable e) {
      // LOG
      System.out.println("- EXCEPTION on message " + counter + " " + e.toString());

      // Something went unexpectedly wrong.
      isConnected = false;
    }
  }

//  public void initializeConnection1() throws Exception
//  {
//    try {
//      Hashtable<String,String> env = new Hashtable<String,String>();
//
//      env.put(Context.INITIAL_CONTEXT_FACTORY, "org.jnp.interfaces.NamingContextFactory");
//      //env.put(Context.INITIAL_CONTEXT_FACTORY, "org.jboss.naming.remote.client.InitialContextFactory");
//      env.put(Context.URL_PKG_PREFIXES, "org.jboss.naming:org.jnp.interfaces");
//      env.put("jnp.disableDiscovery", "true");
//      env.put("jnp.timeout", "60000");
//      //env.put(Context.INITIAL_CONTEXT_FACTORY, "org.jboss.naming.remote.client.InitialContextFactory");
//
//      env.put(Context.PROVIDER_URL, "remote:" + strUrl + PORT_SEP_CHAR + port +
//        MULTI_SEP_CHAR + strHaUrl + PORT_SEP_CHAR + haPort);
//
//
//      System.out.println("- connecting to SAM (primary " + strUrl + ", standby " + strHaUrl + ")");
//
//      if (aLogger != null) {
//        aLogger.log("INFO Connecting to SAM (primary " + strUrl + ", standby " + strHaUrl + ")\n");
//      }
//
//      jndiContext = new InitialContext(env);
//      System.out.println("jndi context is set ");
//
//      // For SAM 5.0 support of the external JMS server.
//      topicConnectionFactory = getExternalFactory(jndiContext);
//      topicConnection = topicConnectionFactory.createTopicConnection(user, password);
//
//      // LOG
//      System.out.println("Connection created for user: " + user);
//
//      // Set the client id.
//      topicConnection.setClientID(clientId);
//
//      // LOG
//      System.out.println("Using client id: " + clientId);
//
//      // Create the topic session.
//      topicSession = topicConnection.createTopicSession(false, TopicSession.AUTO_ACKNOWLEDGE);
//
//      // LOG
//      System.out.println("Topic session created.");
//
//      // Find the topic.
//      // For SAM 5.0 support of the external JMS server.
//      Context lInitialContext = (Context) jndiContext.lookup(JMS_CONTEXT);
//      topic = (Topic) lInitialContext.lookup(strName);
//
//      // LOG
//      System.out.println("Finished initializing topic...");
//
//      // Create topic subscriber.
//      topicSubscriber = topicSession.createSubscriber(topic, filter, false);
//
//      // LOG
//      System.out.println("Topic subscriber created with filter: " + filter);
//
//      acceptedClientId = topicConnection.getClientID();
//
//      // LOG
//      System.out.println("Client id: " + topicConnection.getClientID());
//
//      setMessageListener(this);
//      setExceptionListener(this);
//      startListening();
//
//      isConnected = true;
//
//      // We implicitly have our first heartbeat.
//      lastHeartbeatMs = System.currentTimeMillis();
//
//      // LOG
//      System.out.println("- connection up, invoking registerLogToFile script");
//
//      //
//      // Once we reach this point the JMS connection is up, so call the script to 
//      // send the registerLogToFile SAM-O calls.
//      //
//
//      Runtime lRuntime = Runtime.getRuntime();
//     // regProcess = lRuntime.exec("/home/tpim/scripts/pmExport/registerLogToFile.pl");
//      regProcess = lRuntime.exec(pLFile);
//      System.out.println("reg output is" + regProcess);
//
//      InputStream lIn = regProcess.getInputStream();
//      BufferedInputStream lInBuff = new BufferedInputStream(lIn);
//      InputStreamReader lInReader = new InputStreamReader(lInBuff);
//      BufferedReader lInBuffReader = new BufferedReader(lInReader);
//
//      String lLine;
//      try {
//        while ((lLine = lInBuffReader.readLine()) != null) {
//          System.out.println("SCRIPT> " + lLine);
//        }
//
//        // The script completed.
//        isRegistered = true;
//      } finally {
//        lInBuffReader.close();
//        lInReader.close();
//        lInBuff.close();
//        lIn.close();
//      }
//    } catch (Exception jmse) {
//      System.out.println("- ERROR unable to connect" );
//      jmse.printStackTrace();
//
//      if (topicSession != null) {
//        topicSession.close();
//      }
//      if (topicConnection != null) {
//        topicConnection.close();
//      }
//
//      // LOG
//      System.out.println("- EXCEPTION during connection " + jmse.getMessage());
//
//      isConnected = false;
//
//      throw new JMSException(jmse.getMessage());
//    }
//  }
  
  
    /**
     * This method is called to initialize the connection to the
     * server.
     *
     * @throws Exception The exception thrown if a conneciton error occurs.
     */
    public void initializeConnection() throws Exception
    {
        try
        {
            Hashtable env = new Hashtable();
            env.put(Context.SECURITY_PRINCIPAL, user );
            env.put(Context.SECURITY_CREDENTIALS, password);
            
//            env.put(Context.INITIAL_CONTEXT_FACTORY, "org.jnp.interfaces.NamingContextFactory");
//            //env.put(Context.INITIAL_CONTEXT_FACTORY, "org.jboss.naming.remote.client.InitialContextFactory");
//            env.put(Context.URL_PKG_PREFIXES, "org.jboss.naming:org.jnp.interfaces");
//            env.put("jnp.disableDiscovery", "true");
//            env.put("jnp.timeout", "60000");
//            //env.put(Context.INITIAL_CONTEXT_FACTORY, "org.jboss.naming.remote.client.InitialContextFactory");

//            env.put(Context.PROVIDER_URL, "remote:" + strUrl + PORT_SEP_CHAR + port +
//              MULTI_SEP_CHAR + strHaUrl + PORT_SEP_CHAR + haPort);


            System.out.println("- connecting to SAM (primary " + strUrl + ", standby " + strHaUrl + ")");

            jndiContext = new InitialContext(env);

            System.out.println("Initializing Topic (" + strName + ")...");
            try
            {

                topicConnectionFactory = (TopicConnectionFactory)
                        jndiContext.lookup(CONNECTION_FACTORY);
            }
            catch (Exception e)
            {

                topicConnectionFactory = getExternalFactory(jndiContext);
            }

            // To use persistent JMS, the user must have durable subscription
            // permission (i.e. durable subscription role).
            if (user != null)
            {
                topicConnection = topicConnectionFactory.createTopicConnection(user, password);
                System.out.println("Connection created for user: " + user);
            }
            else
            {
                topicConnection = topicConnectionFactory.createTopicConnection();
                System.out.println("Connection created.");
            }

            // Check for persistant JMS, if so, set the unique client id.
            // IMPORTANT: Client Id must be unique! In case of connection failure,
            // it identifies which messages this client missed.
            if ((isPersistent) && (null == clientId))
            {
                System.out.println("Client ID cannot be null for a durable subscription.");
                throw new JMSException("Client ID cannot be null for a durable subscription.");
            }
            if ((null != clientId) && (!"".equals(clientId)))
            {
                topicConnection.setClientID(clientId);
                System.out.println("Using client id: " + clientId);
            }

            // create the topic session.
            topicSession = topicConnection.createTopicSession(false,
                    TopicSession.AUTO_ACKNOWLEDGE);
            System.out.println("Topic session created.");

            // find the topic.
            try
            {
                topic = (Topic) jndiContext.lookup(strName);
            }
            catch (NamingException ne)
            {
                // For SAM 5.0 support of the external JMS server.
                Context lInitialContext = (Context) jndiContext.lookup(JMS_CONTEXT);
                topic = (Topic) lInitialContext.lookup(strName);
            }
            System.out.println("Finished initializing topic...");

            // create topic subscriber based on persistance.
            if (isPersistent)
            {
                // This is where the subscriber is created with durable subscription
                // for persistant JMS.  The client must specify a name that uniquely
                // identifies each durable subscription it creates.

                if (null != filter)
                {
                    topicSubscriber =
                            topicSession.createDurableSubscriber(topic, clientId, filter, false);
                    System.out.println("Durable topic subscriber created with filter: " + filter);
                }
                else
                {
                    topicSubscriber =
                            topicSession.createDurableSubscriber(topic, clientId);
                    System.out.println("Durable topic subscriber created.");
                }
            }
            else
            {
                if (null != filter)
                {
                    topicSubscriber = topicSession.createSubscriber(topic, filter, false);
                    System.out.println("Topic subscriber created with filter: " + filter);
                }
                else
                {
                    topicSubscriber = topicSession.createSubscriber(topic);
                    System.out.println("Topic subscriber created.");
                }
            }
            acceptedClientId = topicConnection.getClientID();
            System.out.println("Client id: " + topicConnection.getClientID());
            setMessageListener(this);
            setExceptionListener(this);
            startListening();
            isConnected = true;
            System.out.println("Connected and listening...");

      // We implicitly have our first heartbeat.
      lastHeartbeatMs = System.currentTimeMillis();

      // LOG
      System.out.println("- connection up, invoking registerLogToFile script");

      //
      // Once we reach this point the JMS connection is up, so call the script to 
      // send the registerLogToFile SAM-O calls.
      //

      Runtime lRuntime = Runtime.getRuntime();
      //regProcess = lRuntime.exec("/home/tpim/scripts/pmExport/registerLogToFile.pl");
      regProcess = lRuntime.exec(pLFile);
      System.out.println("reg output is" + regProcess);

      InputStream lIn = regProcess.getInputStream();
      BufferedInputStream lInBuff = new BufferedInputStream(lIn);
      InputStreamReader lInReader = new InputStreamReader(lInBuff);
      BufferedReader lInBuffReader = new BufferedReader(lInReader);

      String lLine;
      try {
        while ((lLine = lInBuffReader.readLine()) != null) {
          System.out.println("SCRIPT> " + lLine);
        }

        // The script completed.
        isRegistered = true;
      } finally {
        lInBuffReader.close();
        lInReader.close();
        lInBuff.close();
        lIn.close();
      }
        }
        catch (Throwable jmse)
        {
            if (topicSession != null)
            {
                topicSession.close();
            }
            if (topicConnection != null)
            {
                topicConnection.close();
            }
            System.out.println("Exception: " + jmse.getMessage());
            isConnected = false;
            throw new JMSException(jmse.getMessage());
        }
    }


  private TopicConnectionFactory getExternalFactory(Context aInContext) throws NamingException
  {
    try {
      Context lInitialContext = (Context) aInContext.lookup(JMS_CONTEXT);

      return (TopicConnectionFactory) lInitialContext.lookup(CONNECTION_FACTORY);
    } catch (NamingException e) {
      // LOG
      System.out.println("- EXCEPTION looking up JDNI API " + e.toString());

      throw e;
    }
  }

  public void startListening() throws JMSException
  {
    try {
    topicConnection.start();
    }catch (Exception E) {
    // LOG
    System.out.println("- EXCEPTION while starting listening");
    }

    isListening = true;
  }

  public void stopListening() throws JMSException
  {
    if (null != topicConnection) {
      topicConnection.stop();
    }

    // LOG
    //System.out.println("- EXCEPTION while stopping listening");

    isListening = false;
  }

  public void setExceptionListener(ExceptionListener aInListener) throws JMSException
  {
    if (null != topicConnection) {
      topicConnection.setExceptionListener(aInListener);
    }
  }

  public void setMessageListener(MessageListener aInListener)
  {
    try {
      topicSubscriber.setMessageListener(aInListener);
    } catch (Exception e) {
      // LOG
      System.out.println("- EXCEPTION setting message listener " + e.getMessage());

      // Something went unexpectedly wrong.
      isConnected = false;
    }
  }

  public synchronized void closeConnection()
  {
    close();
    isStopped = true;
  }

  private synchronized void close()
  {
    try {
      isConnected = false;

      stopListening();

      try {
        topicSubscriber.close();
      } catch (Exception e) {
        System.out.println("- EXCEPTION on subscriber close " + e.getMessage());
      }

      try {
        topicSession.close();
      } catch (Exception e) {
        System.out.println("- EXCEPTION on session close " + e.getMessage());
      }

      try {
        topicConnection.close();
      } catch (Exception e) {
        System.out.println("- EXCEPTION on topic close " + e.getMessage());
      }

      // LOG
      System.out.println("Topic subscriber connection closed.");
    } catch (Exception e) {
      // LOG
      System.out.println("- EXCEPTION on closing " + e.getMessage());
    }
  }

  public void onException(JMSException aInException)
  {
    // LOG
    System.out.println("- EXCEPTION while connecting " + aInException.getMessage());

    // Something went unexpectedly wrong.
    isConnected = false;

    try {
      setExceptionListener(null);
      topicConnection.close();
    } catch (Exception e) {
      // Ignore this exception, the TCP connection may already be closed.
    }

    if (isHaEnabled) {
      int lAttempts = 0;

      while (!isConnected && !isStopped) {
        lAttempts++;

        try {
          //initializeConnection();
          return;
        } catch (Exception e) {
          // LOG
          System.out.println("- EXCEPTION during connection attempt " + lAttempts + e.getMessage());
        }

        try {
          Thread.sleep(5000);
        } catch (Exception e) {
          // This exception should not happen unless the process
          // is killed at this point, in which case it is ignored.
        }
      }
    } else {
      System.out.println("Exiting...");
      System.exit(3);
    }
  }

  public static JmsPmClient parseCommandLine(String[] aInArgs) throws Exception
  {
    if (aInArgs.length != 12)
    {
      System.out.println("Usage: JmsPmClient -p <primary> -s <standby> -user <user> -pass <password> -c <client id>");

      System.out.println("  -p <primary app server> The primary SAM application server IP:port");
      System.out.println("  -s <standby app server> The standby SAM application server IP:port");
      System.out.println("  -user <user>            The SAM user");
      System.out.println("  -pass <password>        The SAM user password");
      System.out.println("  -c <client id>          The unique client id for this connection");
      System.out.println("  -f <Perl File Location>          The Perl executable file");
      System.out.println("Example: JmsPmClient -p 192.168.1.100:1099 -s 192.168.1.101:1099 -user samUser -pass secret -c jmsUser@1 -f /home/tpim/scripts/pmExport/registerLogToFile.pl");

      System.exit(1);
    }

    int lIndex = 0;
    String lPriApp = null;
    String lSecApp = null;
    String lUser = null;
    String lPassword = null;
    String lFilter = "";
    String lClientId = "";
    String pfFile = "";

    while (lIndex < aInArgs.length) {
      if (aInArgs[lIndex].equals("-p")) {
        lPriApp = aInArgs[++lIndex];
        lIndex++;
      } else if (aInArgs[lIndex].equals("-s")) {
        lSecApp = aInArgs[++lIndex];
        lIndex++;
      } else if (aInArgs[lIndex].equals("-user")) {
        lUser = aInArgs[++lIndex];
        lIndex++;
      } else if (aInArgs[lIndex].equals("-pass")) {
        lPassword = aInArgs[++lIndex];
        lIndex++;
      } else if (aInArgs[lIndex].equals("-c")) {
        lClientId = aInArgs[++lIndex];
        lIndex++;
      } else if (aInArgs[lIndex].equals("-f")) {
        pfFile = aInArgs[++lIndex];
        lIndex++;
      }else {
        lIndex++;
      }
    }

    // A suitable filter is created based on the client id.
    lFilter = "ALA_clientId in ('" + lClientId + "','')";

    // Create a new JmsPmClient object based on the parameters passed in.
    return new JmsPmClient("5620-SAM-topic-xml-file", lPriApp, lSecApp, lClientId, lUser, lPassword, lFilter, pfFile, true);
  }

  public static void main(String[] aInArgs)
  {
    System.out.println("JMS client started");

    try {
      // Get the logger ready.
      aLogger = new Logger();
      aLogger.init();

      // Create our JmsPmClient object based on the command line arguments.
      jmsClient = parseCommandLine(aInArgs);

      // Connect to the server.
      jmsClient.initializeConnection();

      // Wait forever while messages are processed.
      while (true);

      // We will never reach here.
      //jmsClient.closeConnection();
    } catch (Exception e) {
      // LOG
      System.out.println("- Unable to connect to SAM, exiting");
      System.out.println("- Exception " + e.getMessage());

      if (aLogger != null) {
        aLogger.log("ERROR Exception: " + e.getMessage() + "\n");
        aLogger.log("ERROR Unable to connect to SAM, exiting\n");
      }

      // We give up at this point and let the cron restart us.
      System.exit(1);
    }
  }
}

