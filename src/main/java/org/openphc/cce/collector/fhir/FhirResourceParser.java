package org.openphc.cce.collector.fhir;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.DataFormatException;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.springframework.stereotype.Component;

/**
 * Parses FHIR R4 resources from {@link JsonNode} payloads using HAPI FHIR.
 *
 * <p>The inbound {@code data} field arrives as a Jackson {@link JsonNode}. This
 * component serializes the node to JSON text and hands it to HAPI FHIR's JSON
 * parser for structural parsing.</p>
 *
 * <p>Reading the {@code resourceType} discriminator is not here but in
 * {@code cce-common-util}'s {@link org.openphc.cce.common.fhir.ResourceTypeDetector}: the Matcher
 * Service has to reach the same verdict on the same payload, and the two services had drifted onto
 * different lookups.</p>
 */
@Component
@RequiredArgsConstructor
public class FhirResourceParser {

    private final FhirContext fhirContext;

    /**
     * Parse the data node into a HAPI FHIR {@link IBaseResource}.
     *
     * @param data the FHIR resource as a Jackson {@link JsonNode}
     * @return the parsed FHIR resource
     * @throws IllegalArgumentException if HAPI FHIR cannot parse the resource
     */
    public IBaseResource parse(JsonNode data) {
        try {
            String json = data.toString();
            return fhirContext.newJsonParser().parseResource(json);
        } catch (DataFormatException e) {
            throw new IllegalArgumentException("Failed to parse FHIR resource: " + e.getMessage(), e);
        }
    }
}
