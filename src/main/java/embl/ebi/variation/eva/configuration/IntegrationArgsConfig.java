package embl.ebi.variation.eva.configuration;

import org.opencb.datastore.core.ObjectMap;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Created by tom on 17/08/16.
 */
@Configuration
public class IntegrationArgsConfig {

    @Bean(initMethod = "loadArgs")
    public IntegrationArgs evaIntegrationArgs(){
        return new IntegrationArgs();
    }

    @Bean(name = "integrationOptions")
    public ObjectMap integrationOptions(){
        return evaIntegrationArgs().getIntegrationOptions();
    }

}
