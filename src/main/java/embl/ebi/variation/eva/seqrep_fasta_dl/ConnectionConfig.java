package embl.ebi.variation.eva.seqrep_fasta_dl;

import embl.ebi.variation.eva.config.EvaIntegrationArgsConfig;
import org.opencb.datastore.core.ObjectMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.ftp.session.DefaultFtpSessionFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * Created by tom on 17/08/16.
 */
@Configuration
@EnableIntegration
@Import({EvaIntegrationArgsConfig.class})
public class ConnectionConfig {

    @Autowired
    private ObjectMap enaFtpOptions;

    @Bean
    public DefaultFtpSessionFactory enaFtpSessionFactory(){
        DefaultFtpSessionFactory sessionFactory = new DefaultFtpSessionFactory();
        sessionFactory.setHost(enaFtpOptions.getString("enaFtpHost"));
        sessionFactory.setPort(enaFtpOptions.getInt("enaFtpPort"));
        sessionFactory.setUsername(enaFtpOptions.getString("enaFtpUserId"));
        sessionFactory.setPassword(enaFtpOptions.getString("enaFtpPassword"));
        return sessionFactory;
    }

    @Bean
    public ThreadPoolTaskExecutor taskExecutor(){
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(5);
        executor.setMaxPoolSize(10);
        return executor;
    }

}
