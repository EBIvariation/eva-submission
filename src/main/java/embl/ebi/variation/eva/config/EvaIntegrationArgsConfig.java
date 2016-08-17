package embl.ebi.variation.eva.config;

import org.opencb.datastore.core.ObjectMap;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Created by tom on 17/08/16.
 */
@Configuration
public class EvaIntegrationArgsConfig {

    @Bean(initMethod = "loadArgs")
    public EVAIntegrationArgs evaIntegrationArgs(){
        return new EVAIntegrationArgs();
    }

    @Bean
    public ObjectMap evaIntegrationOptions(){
        return evaIntegrationArgs().getEnaFtpOptions();
    }

}
