package uk.ac.ebi.eva.configuration;

import javax.validation.constraints.*;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "ena-ftp", ignoreUnknownFields = false)
public class EnaFtpProperties {

    @NotNull
    @Size(min = 1)
    private String host;

    @Min(1)
    @Max(65535)
    private int port;

    private String username;

    private String password;

    @NotNull
    @Size(min = 1)
    private String sequenceReportRoot;

    public EnaFtpProperties() {
    }

    public EnaFtpProperties(String host, int port, String username, String password, String sequenceReportRoot) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.sequenceReportRoot = sequenceReportRoot;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getSequenceReportRoot() {
        return sequenceReportRoot;
    }

    public void setSequenceReportRoot(String sequenceReportRoot) {
        this.sequenceReportRoot = sequenceReportRoot;
    }

}
