package com.pingidentity.sync.dst;

import com.google.gson.Gson;
import com.unboundid.directory.sdk.common.internal.Reconfigurable;
import com.unboundid.directory.sdk.sync.api.SyncDestination;
import com.unboundid.directory.sdk.sync.config.SyncDestinationConfig;
import com.unboundid.directory.sdk.sync.types.EndpointException;
import com.unboundid.directory.sdk.sync.types.PostStepResult;
import com.unboundid.directory.sdk.sync.types.SyncOperation;
import com.unboundid.directory.sdk.sync.types.SyncServerContext;
import com.unboundid.ldap.sdk.Entry;
import com.unboundid.ldap.sdk.Modification;
import com.unboundid.ldap.sdk.ResultCode;
import com.unboundid.util.args.ArgumentParser;
import com.unboundid.util.args.FileArgument;
import com.unboundid.util.args.StringArgument;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

/**
 *
 * This extension provides a fairly generic way to publish changes to Kafka
 *
 */
public class Kafka extends SyncDestination implements Reconfigurable<SyncDestinationConfig>
{
    private static final String ARG_NAME_PROPERTIES = "kafka-properties-file";
    private static final String ARG_NAME_TOPIC = "kafka-topic";
    
    private SyncServerContext serverContext;
    private SyncDestinationConfig config;
    private File kafkaPropertiesFile;
    private KafkaProducer<String,String> kafkaProducer;
    private Gson gson;
    private String kafkaTopic;
    
    /**
     *  Provide a string describing this extension succinctly
     * @return
     */
    @Override
    public String getExtensionName()
    {
        return "Sync Kafka Destination";
    }
    
    /**
     * Provide detailed description about this extension
     * @return
     */
    @Override
    public String[] getExtensionDescription()
    {
        return new String[]{"This destination publishes changes from any source to Apache Kafka"};
    }
    
    /**
     *  Performs all processing to ensure arguments will result in a valid configuration
     * @param syncDestinationConfig
     * @param argumentParser
     * @param list
     * @return
     */
    @Override
    public boolean isConfigurationAcceptable(SyncDestinationConfig syncDestinationConfig, ArgumentParser argumentParser, List<String> list)
    {
        return true;
    }
    
    /**
     * Performs all processing to apply configuration to the extension instance
     *
     * @param syncDestinationConfig
     * @param argumentParser
     * @param administrativeActions
     * @param messages
     * @return
     */
    @Override
    public ResultCode applyConfiguration(SyncDestinationConfig syncDestinationConfig, ArgumentParser argumentParser,
                                         List<String> administrativeActions, List<String> messages)
    {
        kafkaPropertiesFile = argumentParser.getFileArgument(ARG_NAME_PROPERTIES).getValue();
        Properties kafkaProperties = new Properties();
        try
        {
            kafkaProperties.load(new FileInputStream(kafkaPropertiesFile));
            kafkaProducer = new KafkaProducer<>(kafkaProperties);
        } catch (IOException e)
        {
            e.printStackTrace();
        }
        
        kafkaTopic = argumentParser.getStringArgument(ARG_NAME_TOPIC).getValue();
        return ResultCode.SUCCESS;
    }
    
    /**
     * Performs all processing required to define configuration arguments for the extension
     *
     * @param parser
     * @throws com.unboundid.util.args.ArgumentException
     */
    
    public void defineConfigArguments(com.unboundid.util.args.ArgumentParser parser)
            throws com.unboundid.util.args.ArgumentException {
        parser.addArgument(new FileArgument(null, ARG_NAME_PROPERTIES, false, 1,
                "{properties.file}", "Kafka configuration properties file"));
    
        parser.addArgument(new StringArgument(null, ARG_NAME_TOPIC, false, 1,
                "{topicName}", "Kafka topic to subscribe to. Specify one or more topics.(default:ping-data-sync-topic)", "ping-data-sync-topic"));
    
    }
    
    /**
     * Performs all processing required to initialize the extension
     *
     * @param serverContext the server context
     * @param config the extension instance configuration
     * @param parser the argument parser
     * @throws EndpointException
     */
    public void initializeSyncDestination(SyncServerContext serverContext,
                                          SyncDestinationConfig config,
                                          com.unboundid.util.args.ArgumentParser parser)
            throws EndpointException
    {
        this.serverContext = serverContext;
        this.config = config;
        gson = new Gson();
    
        List<String> actions = new ArrayList<>();
        List<String> messages = new ArrayList<>();
        ResultCode result = applyConfiguration(config, parser, actions, messages);
        if (! ResultCode.SUCCESS.equals(result) )
        {
            throw new EndpointException(PostStepResult.ABORT_OPERATION,"Error initializing end point");
        }
    }
    
    /**
     * Performs all processing to gracefully shut down
     *
     */
    public void finalizeSyncDestination()
    {
        kafkaProducer.close();
    }
    
    /**
     * Performs all processing to build a URL that represents the extension instance
     * @return a string with the URL for the extension instance
     */
    @Override
    public String getCurrentEndpointURL()
    {
        return "kafka://"+kafkaTopic;
    }
    
    /**
     * Performs all processing required to publish the change corresponding to a new entry
     *
     * @param entry the entry that was created
     * @param syncOperation the sync operation and its context
     * @throws EndpointException an exception representing any error that occurred during processing
     */
    @Override
    public void createEntry(Entry entry, SyncOperation syncOperation) throws EndpointException
    {
        kafkaProducer.send(buildProducerRecord("ADD",entry,syncOperation));
    }
    
    /**
     * Performs all processing required to publish the change corresponding to the modification made to the entry
     *
     * @param entry the entry that was modified
     * @param list a list of modifications that were applied to the entry
     * @param syncOperation the sync operation and its context
     * @throws EndpointException an exception representing any error that occurred during processing
     */
    @Override
    public void modifyEntry(Entry entry, List<Modification> list, SyncOperation syncOperation) throws EndpointException
    {
        kafkaProducer.send(buildProducerRecord("MOD",entry,syncOperation));
    }
    
    /**
     * Performs all processing required to publis the change corresponding to the deleted entry
     *
     * @param entry the entry that was deleted
     * @param syncOperation the sync operation and its context
     * @throws EndpointException an exception representing any error that occurred during processing
     */
    @Override
    public void deleteEntry(Entry entry, SyncOperation syncOperation) throws EndpointException
    {
        kafkaProducer.send(buildProducerRecord("REM",entry,syncOperation));
    }
    
    /**
     * Performs all processing required to build a kafka message
     *
     * @param type the operation type that occurred on the entry at the source
     * @param entry the entry on which the change(s) occurred
     * @param syncOperation the sync operation and it context
     * @return the kafka record
     */
    private ProducerRecord<String,String> buildProducerRecord(String type, Entry entry, SyncOperation syncOperation)
    {
        Map<String,String> payload = new HashMap<>();
        payload.put("type",type);
        payload.put("dn",entry.getDN());
        StringBuilder attributes = new StringBuilder();
        if ( type.equalsIgnoreCase("MOD"))
        {
            for (String a : syncOperation.getModifiedSourceAttributes())
            {
                attributes.append(attributes.length() > 0 ? "," : "" + a);
            }
            payload.put("attrs", attributes.toString());
        }
    
        return new ProducerRecord<>(kafkaTopic,entry.getDN(),gson.toJson(payload));
    }
}
