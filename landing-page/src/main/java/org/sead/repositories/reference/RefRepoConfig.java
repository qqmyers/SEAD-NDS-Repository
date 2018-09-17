package org.sead.repositories.reference;

import org.glassfish.jersey.server.ResourceConfig;

public class RefRepoConfig extends ResourceConfig {

    public RefRepoConfig() {
        // Define the package which contains the service classes.
        packages("org.sead.repositories.reference");
    }
}
