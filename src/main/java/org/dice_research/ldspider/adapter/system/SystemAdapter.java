package org.dice_research.ldspider.adapter.system;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Semaphore;

import org.dice_research.squirrel.data.uri.serialize.Serializer;
import org.dice_research.squirrel.data.uri.serialize.java.GzipJavaUriSerializer;
import org.hobbit.core.components.AbstractSystemAdapter;
import org.hobbit.core.components.ContainerStateObserver;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SystemAdapter extends AbstractSystemAdapter implements ContainerStateObserver {
	
    private static final Logger LOGGER = LoggerFactory.getLogger(SystemAdapter.class);

	
	private final static String LDSPIDER_IMAGE = "ldspider:latest";
    protected int numberOfWorkers = 1;
    private Serializer serializer;
    protected Set<String> workerInstances = new HashSet<>();
    protected Semaphore processTerminated = new Semaphore(0);
    protected boolean terminating = false;
    
    
    protected String ldSpiderInstance;

	
	@Override
	public void init() throws Exception {
		super.init();
		
		ldSpiderInstance = createContainer(LDSPIDER_IMAGE, null, this);

		
		serializer = new GzipJavaUriSerializer();
	}
	
	@Override
	public void receiveGeneratedData(byte[] data) {
		// handle the incoming data as described in the benchmark description
        ByteBuffer buffer = ByteBuffer.wrap(data);
        String sparqlUrl = RabbitMQUtils.readString(buffer);
        String sparqlUser = RabbitMQUtils.readString(buffer);
        String sparqlPwd = RabbitMQUtils.readString(buffer);
		
		String[] WORKER_ENV = { "b=1000",
                "oe=" + sparqlUrl,
                "user_sparql=" + sparqlUser, "passwd_sparql=" + sparqlPwd,
                "s=/seed_file" };		

        String worker;
        
        worker = createContainer(LDSPIDER_IMAGE, WORKER_ENV, this);

            if (worker == null) {
                LOGGER.error("Error while trying to start LDSpider. Exiting.");
                System.exit(1);
            } else {
                LOGGER.info("LDSpider started.");
                workerInstances.add(worker);
            }    
		
	}
	
	@Override
	public void receiveGeneratedTask(String taskId, byte[] data) {
		 // handle the incoming task and create a result
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("receiveGeneratedTask({})->{}", taskId, new String(data));
        } else {
            LOGGER.debug("Received seed URI(s).");
        }

        
        String seed = RabbitMQUtils.readString(data);		
        
        
        //TODO implement a solution for inserting seeds in ldspider
	}
	
	@Override
	protected synchronized void terminate(Exception cause) {
		// TODO Auto-generated method stub
		super.terminate(cause);
	}
	
	
	@Override
	public void containerStopped(String containerName, int exitCode) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		super.close();
	}

}
