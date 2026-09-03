package org.openphc.cce.collector.fhir;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.ResourceType;
import org.openphc.cce.collector.api.exception.PatientIdNotFoundException;
import org.openphc.cce.collector.service.PayloadValidationResult;
import org.openphc.cce.common.fhir.ResourceTypeDetector;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Performs FHIR R4 structural validation on the {@code data} payload.
 *
 * <p>Structural validation checks that the payload can be parsed by HAPI FHIR
 * into a recognized FHIR resource. This is <em>not</em> full FHIR profile
 * validation — the Collector only verifies that the resource is structurally
 * sound.</p>
 *
 * <h3>Validation checks</h3>
 * <ol>
 *   <li>{@code data.resourceType} present and non-empty — required</li>
 *   <li>HAPI FHIR can parse the data into {@link IBaseResource} — required</li>
 *   <li>Patient ID extracted from parsed resource matches CloudEvents {@code subject} — rejects on mismatch or extraction failure</li>
 * </ol>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FhirResourceValidator {

    private final FhirResourceParser parser;
    private final ResourceTypeDetector resourceTypeDetector;
    private final PatientIdExtractor patientIdExtractor;

    /**
     * Validate the FHIR payload.
     *
     * @param data    the FHIR resource as a Jackson {@link JsonNode}
     * @param subject the CloudEvents {@code subject} (patient UPID)
     * @return validation result with errors and the parsed resource
     *         (if parsing succeeded)
     */
    public PayloadValidationResult validate(JsonNode data, String subject) {
        List<String> errors = new ArrayList<>();

        // Check: resourceType present and a recognized FHIR R4 resource type
        ResourceType resourceType = resourceTypeDetector.detect(data);
        if (resourceType == null) {
            errors.add("data.resourceType is required and must be a recognized FHIR R4 resource type");
            return buildResult(errors, null);
        }

        // Check: HAPI FHIR can parse the resource
        IBaseResource parsedResource;
        try {
            parsedResource = parser.parse(data);
        } catch (IllegalArgumentException e) {
            errors.add("Failed to parse FHIR resource: " + e.getMessage());
            return buildResult(errors, null);
        }

        // Check: subject reference — rejects on mismatch or extraction failure
        checkSubjectReference(parsedResource, subject, errors);

        return buildResult(errors, parsedResource);
    }

    private void checkSubjectReference(IBaseResource parsedResource, String subject,
                                       List<String> errors) {
        if (subject == null) {
            return;
        }

        try {
            String extractedPatientId = patientIdExtractor.extract(parsedResource);
            if (!Objects.equals(extractedPatientId, subject)) {
                errors.add("Extracted patient ID '" + extractedPatientId
                        + "' does not match CloudEvents subject '" + subject + "'");
                log.warn("Subject mismatch: extracted patient ID='{}' "
                        + "does not match CloudEvents subject='{}'", extractedPatientId, subject);
            }
        } catch (PatientIdNotFoundException e) {
            // No patient reference found — reject the event
            errors.add("Patient ID extraction failed for subject '" + subject + "': " + e.getMessage());
            log.warn("Patient ID extraction failed for subject '{}': {}", subject, e.getMessage());
        }
    }

    private PayloadValidationResult buildResult(List<String> errors, IBaseResource resource) {
        return PayloadValidationResult.builder()
                .valid(errors.isEmpty())
                .errors(List.copyOf(errors))
                .parsedResource(resource)
                .build();
    }
}
