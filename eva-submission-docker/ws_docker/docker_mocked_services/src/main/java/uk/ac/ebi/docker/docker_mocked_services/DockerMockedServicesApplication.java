package uk.ac.ebi.docker.docker_mocked_services;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;

@SpringBootApplication
public class DockerMockedServicesApplication extends SpringBootServletInitializer {

    public static void main(String[] args) {
        SpringApplication.run(DockerMockedServicesApplication.class, args);
    }

    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
        return application.sources(DockerMockedServicesApplication.class);
    }

}
