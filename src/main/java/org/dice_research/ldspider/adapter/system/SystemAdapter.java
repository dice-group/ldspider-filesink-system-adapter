package org.dice_research.ldspider.adapter.system;

import static org.hobbit.core.Constants.CONTAINER_TYPE_SYSTEM;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.jena.rdf.model.Literal;
import org.dice_research.ldspider.vocab.LDSpiderSystem;
import org.hobbit.core.Commands;
import org.hobbit.core.components.AbstractSystemAdapter;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.hobbit.utils.rdf.RdfHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SystemAdapter extends AbstractSystemAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(SystemAdapter.class);

    private final static String LDSPIDER_IMAGE = "dicegroup/ldspider-filesink:latest";
    private final static boolean USE_IMAGE = false;
    protected boolean terminating = false;

    protected String ldSpiderInstance;
    protected Thread ldSpiderThread;

    @Override
    public void init() throws Exception {
        super.init();

    }

    @Override
    public void receiveGeneratedData(byte[] data) {
        // handle the incoming data as described in the benchmark description
        ByteBuffer buffer = ByteBuffer.wrap(data);
        String sparqlUrl = RabbitMQUtils.readString(buffer);
        String sparqlUser = RabbitMQUtils.readString(buffer);
        String sparqlPwd = RabbitMQUtils.readString(buffer);
        String[] seedURIs = RabbitMQUtils.readString(buffer).split("\n");

        Map<String, String> parameters = new HashMap<>();

        parameters.put("s", String.join(",", seedURIs));
        parameters.put("o", "tempFile");

        LOGGER.info("Sparql Endpoint: " + sparqlUrl);
        parameters.put("oe", sparqlUrl);
        LOGGER.info("Sparql User: " + sparqlUser);
        parameters.put("user_sparql", sparqlUser);
        LOGGER.info("Sparql Passwd: " + sparqlPwd);
        parameters.put("passwd_sparql", sparqlPwd);

        LOGGER.info("Seed URIs: {}.", Arrays.toString(seedURIs));

        Literal workerCountLiteral = RdfHelper.getLiteral(systemParamModel, null, LDSpiderSystem.numberOfThreads);
        if (workerCountLiteral == null) {
            throw new IllegalStateException("Couldn't find necessary parameter value for \""
                    + LDSpiderSystem.numberOfThreads + "\". Aborting.");
        }
        int numberOfThreads = workerCountLiteral.getInt();
        parameters.put("t", Integer.toString(numberOfThreads));

        String strategy = RdfHelper.getStringValue(systemParamModel, null, LDSpiderSystem.strategy);
        if (strategy == null) {
            throw new IllegalStateException("Couldn't find necessary parameter value for \""
                    + LDSpiderSystem.numberOfThreads + "\". Aborting.");
        }

        Literal politenessLiteral = RdfHelper.getLiteral(systemParamModel, null, LDSpiderSystem.politeness);
        if (politenessLiteral != null) {
            long politeness = workerCountLiteral.getLong();
            LOGGER.info("Using static politeness strategy with {}ms delay.", politeness);
            parameters.put("polite", Long.toString(politeness));
        }

        switch (strategy) {
        case "b": {
            // Breadth first strategy - we want to crawl as far as possible
            LOGGER.info("Using breadth-first Strategy");
            parameters.put("b", "100");
            break;
        }
        case "c": {
            // Load balancing strategy - we want to crawl as many URIs as possible
            LOGGER.info("Using load balanced Strategy");
            parameters.put("c", Integer.toString(Integer.MAX_VALUE));
            break;
        }
        case "dbfq": {
            // Disk-based breadth first strategy
            LOGGER.info("Using disk-based breadth-first Strategy");
            parameters.put("dbfq", "1");
            parameters.put("b", "100");
            break;
        }
        default: {
            throw new IllegalStateException(
                    "Got an unknown strategy \"" + LDSpiderSystem.numberOfThreads + "\". Aborting.");
        }
        }
        LOGGER.info("Starting LDSpider - FileSink");
        if (USE_IMAGE) {
            ldSpiderInstance = createContainer(LDSPIDER_IMAGE, CONTAINER_TYPE_SYSTEM,
                    parameters.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).toArray(String[]::new));
            LOGGER.info("Image Started");
        } else {
            ldSpiderThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        com.ontologycentral.ldspider.Main.run(parameters);
                        terminate(null);
                    } catch (Exception e) {
                        terminate(e);
                    }
                }
            });
            ldSpiderThread.start();
            ldSpiderInstance = null;
            LOGGER.info("Thread Started");
        }
    }

    @Override
    public void receiveGeneratedTask(String taskId, byte[] data) {
        throw new IllegalStateException("Should not receive any tasks.");
    }

    @Override
    protected synchronized void terminate(Exception cause) {
        LOGGER.debug("Terminating");
        terminating = true;
        super.terminate(cause);
    }

    @Override
    public void receiveCommand(byte command, byte[] data) {
        if (command == Commands.DOCKER_CONTAINER_TERMINATED) {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            String containerName = RabbitMQUtils.readString(buffer);
            int exitCode = buffer.get();
            containerStopped(containerName, exitCode);
        }
        super.receiveCommand(command, data);
    }

    public void containerStopped(String containerName, int exitCode) {
        // Check whether it is one of your containers and react accordingly
        if ((ldSpiderInstance != null) && (ldSpiderInstance.equals(containerName)) && !terminating) {
            Exception e = null;
            if (exitCode != 0) {
                // The ldspider had an error. Its time to panic
                LOGGER.error("ldspider terminated with exit code {}.", exitCode);
                e = new IllegalStateException("ldspider terminated with exit code " + exitCode + ".");
            }
            ldSpiderInstance = null;
            terminate(e);
        }

    }

    @Override
    public void close() throws IOException {
        LOGGER.info("Finishing LDSpider File Sink adapter");
        super.close();
    }

}
