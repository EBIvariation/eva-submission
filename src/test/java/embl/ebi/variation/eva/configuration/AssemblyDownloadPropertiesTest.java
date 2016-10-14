package embl.ebi.variation.eva.configuration;

import static org.junit.Assert.*;

import java.util.Set;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;

import org.junit.Before;
import org.junit.Test;

public class AssemblyDownloadPropertiesTest {

    private Validator validator;

    @Before
    public void setUp() {
        ValidatorFactory vf = Validation.buildDefaultValidatorFactory();
        this.validator = vf.getValidator();
    }
    
    @Test
    public void createAssemblyDownloadProperties() {
        Set<ConstraintViolation<AssemblyDownloadProperties>> violations;
        violations = this.validator.validate(new AssemblyDownloadProperties("GCA_12345", "/"));
        assertTrue(violations.isEmpty());
        
        violations = this.validator.validate(new AssemblyDownloadProperties("GCA_12345.1", "/"));
        assertTrue(violations.isEmpty());
    }

    @Test
    public void failNullAssemblyAccession() {
        Set<ConstraintViolation<AssemblyDownloadProperties>> violations = this.validator.validate(new AssemblyDownloadProperties(null, "/"));
        assertFalse(violations.isEmpty());
    }

    @Test
    public void failNotMachingAssemblyAccession() {
        Set<ConstraintViolation<AssemblyDownloadProperties>> violations;
        violations = this.validator.validate(new AssemblyDownloadProperties("GCA_12345.", "/"));
        assertFalse(violations.isEmpty());
        violations = this.validator.validate(new AssemblyDownloadProperties("GCA12345", "/"));
        assertFalse(violations.isEmpty());
        violations = this.validator.validate(new AssemblyDownloadProperties("GCA12345.1", "/"));
        assertFalse(violations.isEmpty());
    }

    @Test
    public void failNullDownloadRootPath() {
        Set<ConstraintViolation<AssemblyDownloadProperties>> violations = this.validator.validate(new AssemblyDownloadProperties("GCA_12345.1", null));
        assertFalse(violations.isEmpty());
    }

    @Test
    public void failEmptyDownloadRootPath() {
        Set<ConstraintViolation<AssemblyDownloadProperties>> violations = this.validator.validate(new AssemblyDownloadProperties("GCA_12345.1", ""));
        assertFalse(violations.isEmpty());
    }
    
}
