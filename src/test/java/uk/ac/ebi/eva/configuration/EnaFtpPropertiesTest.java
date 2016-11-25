package uk.ac.ebi.eva.configuration;

import static org.junit.Assert.*;

import java.util.Set;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;

import org.junit.Before;
import org.junit.Test;

public class EnaFtpPropertiesTest {

    private Validator validator;

    @Before
    public void setUp() {
        ValidatorFactory vf = Validation.buildDefaultValidatorFactory();
        this.validator = vf.getValidator();
    }
    
    @Test
    public void createEnaFtpProperties() {
        Set<ConstraintViolation<EnaFtpProperties>> violations;
        violations = this.validator.validate(new EnaFtpProperties("localhost", 1, null, null, "/"));
        assertTrue(violations.isEmpty());
        
        violations = this.validator.validate(new EnaFtpProperties("localhost", 8080, null, null, "/"));
        assertTrue(violations.isEmpty());
        
        violations = this.validator.validate(new EnaFtpProperties("localhost", 65535, null, null, "/"));
        assertTrue(violations.isEmpty());
    }

    @Test
    public void failNullHost() {
        Set<ConstraintViolation<EnaFtpProperties>> violations = this.validator.validate(new EnaFtpProperties(null, 8080, null, null, "/"));
        assertFalse(violations.isEmpty());
    }

    @Test
    public void failEmptyHost() {
        Set<ConstraintViolation<EnaFtpProperties>> violations = this.validator.validate(new EnaFtpProperties("", 8080, null, null, "/"));
        assertFalse(violations.isEmpty());
    }

    @Test
    public void failPortNumberNegative() {
        Set<ConstraintViolation<EnaFtpProperties>> violations = this.validator.validate(new EnaFtpProperties("localhost", -1000, null, null, "/"));
        assertFalse(violations.isEmpty());
    }

    @Test
    public void failPortNumberZero() {
        Set<ConstraintViolation<EnaFtpProperties>> violations = this.validator.validate(new EnaFtpProperties("localhost", 0, null, null, "/"));
        assertFalse(violations.isEmpty());
    }

    @Test
    public void failPortNumberTooLarge() {
        Set<ConstraintViolation<EnaFtpProperties>> violations = this.validator.validate(new EnaFtpProperties("localhost", 65536, null, null, "/"));
        assertFalse(violations.isEmpty());
    }

    @Test
    public void failNullSequenceReportRoot() {
        Set<ConstraintViolation<EnaFtpProperties>> violations = this.validator.validate(new EnaFtpProperties("localhost", 8080, null, null, null));
        assertFalse(violations.isEmpty());
    }

    @Test
    public void failEmptySequenceReportRoot() {
        Set<ConstraintViolation<EnaFtpProperties>> violations = this.validator.validate(new EnaFtpProperties("localhost", 8080, null, null, ""));
        assertFalse(violations.isEmpty());
    }
    
}
