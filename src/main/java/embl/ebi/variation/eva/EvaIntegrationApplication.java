package embl.ebi.variation.eva;

import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.core.env.StandardEnvironment;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Properties;

public class EvaIntegrationApplication {

	public static void main(String[] args) {

		ConfigurableApplicationContext ctx = new ClassPathXmlApplicationContext(
				new String[] {"download-seq-report-config.xml", "connection-config.xml"},
				false);

		Properties properties = loadProperties();

        String assemblyAccession = "GCA_000001405.10";
        String localAssemblyDirectoryRoot = "/home/tom/Job_Working_Directory/Java/eva-integration/src/main/resources/test_dl/ftpInbound";
        File f = Paths.get(localAssemblyDirectoryRoot, assemblyAccession + "_sequence_report.txt").toFile();

        setupEnvironment(ctx, assemblyAccession);

        if(!f.exists()) {
            ENASequenceReportDL.downloadSequenceReport(ctx, assemblyAccession,
                    properties.getProperty("remoteSequenceReportDirectory"));
        }else{
            System.out.println("FILE EXISTS");
        }
	}


	private static Properties loadProperties(){
		Properties prop = new Properties();
		try {
			prop.load(ENASequenceReportDL.class.getClassLoader().getResourceAsStream("application.properties"));
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
