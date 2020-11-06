package org.dice_research.ldspider.vocab;

import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;

public class LDSpiderSystem {

    /**
     * The namespace of the vocabulary as a string
     */
    public static final String uri = "http://project-hobbit.eu/ldcbench-system/";

    /**
     * returns the URI for this schema
     * 
     * @return the URI for this schema
     */
    public static String getURI() {
        return uri;
    }

    protected static final Resource resource(String local) {
        return ResourceFactory.createResource(uri + local);
    }

    protected static final Property property(String local) {
        return ResourceFactory.createProperty(uri, local);
    }
    
    public static final Property numberOfThreads = property("numberOfThreads");
    public static final Property strategy = property("strategy");
    public static final Property politeness = property("politeness");

}
