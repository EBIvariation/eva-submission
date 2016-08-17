package embl.ebi.variation.eva;

import embl.ebi.variation.eva.sequence_report_download.ENASequenceReportDownload;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.GenericMessage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Properties;

@SpringBootApplication
@IntegrationComponentScan
public class EvaIntegrationApplication {


	public static void main(String[] args) {

        ConfigurableApplicationContext ctx = SpringApplication.run(EvaIntegrationApplication.class, args);

		Properties properties = loadProperties();

        String assemblyAccession = "GCA_000001405.10";
        String localAssemblyDirectoryRoot = "/home/tom/Job_Working_Directory/Java/eva-integration/src/main/resources/test_dl/ftpInbound";
        File sequenceReportFile = Paths.get(localAssemblyDirectoryRoot, assemblyAccession + "_sequence_report.txt").toFile();

//        setupEnvironment(ctx, assemblyAccession);

        ENASequenceReportDownload.ENAFtpLs enaFtpLs = ctx.getBean(ENASequenceReportDownload.ENAFtpLs.class);
        enaFtpLs.lsEnaFtp("pub/databases/ena/assembly/");
	}


	private static Properties loadProperties(){
		Properties prop = new Properties();
		try {
			prop.load(ENASequenceReportDownload.class.getClassLoader().getResourceAsStream("application.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return prop;
	}


    private static void setupEnvironment(ConfigurableApplicationContext ctx, String accession) {
        StandardEnvironment env = new StandardEnvironment();
        Properties props = new Properties();

        props.setProperty("assembly.accession", accession);

        PropertiesPropertySource pps = new PropertiesPropertySource("ftpprops", props);
        env.getPropertySources().addFirst(pps);
        ctx.setEnvironment(env);
        ctx.refresh();
    }
}
