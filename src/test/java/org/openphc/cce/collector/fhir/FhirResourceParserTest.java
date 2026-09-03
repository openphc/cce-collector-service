package org.openphc.cce.collector.fhir;

import ca.uhn.fhir.context.FhirContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Condition;
import org.hl7.fhir.r4.model.Encounter;
import org.hl7.fhir.r4.model.Observation;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link FhirResourceParser}.
 * Uses a real {@link FhirContext} (R4) — no Spring context.
 */
class FhirResourceParserTest {

    private static FhirResourceParser parser;
    private static final ObjectMapper mapper = new ObjectMapper();

    @BeforeAll
    static void setup() {
        FhirContext ctx = FhirContext.forR4();
        parser = new FhirResourceParser(ctx);
    }

    // ════════════════════════════════════════════════════════════════
    // parse
    // ════════════════════════════════════════════════════════════════

    @Nested
    @DisplayName("parse")
    class Parse {

        @Test
        @DisplayName("parses valid Encounter")
        void parsesEncounter() {
            JsonNode data = validEncounter();
            IBaseResource resource = parser.parse(data);

            assertThat(resource).isInstanceOf(Encounter.class);
        }

        @Test
        @DisplayName("parses valid Observation")
        void parsesObservation() {
            JsonNode data = validObservation();
            IBaseResource resource = parser.parse(data);

            assertThat(resource).isInstanceOf(Observation.class);
        }

        @Test
        @DisplayName("parses valid Condition")
        void parsesCondition() {
            JsonNode data = validCondition();
            IBaseResource resource = parser.parse(data);

            assertThat(resource).isInstanceOf(Condition.class);
        }

        @Test
        @DisplayName("throws on unknown resourceType")
        void throwsOnUnknownResourceType() {
            JsonNode data = mapper.valueToTree(Map.of(
                    "resourceType", "UnknownFooBarResource",
                    "id", "example"));

            assertThatThrownBy(() -> parser.parse(data))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Failed to parse FHIR resource");
        }
    }

    // ─── Test data helpers ─────────────────────────────────────────

    static JsonNode validEncounter() {
        Map<String, Object> data = new HashMap<>();
        data.put("resourceType", "Encounter");
        data.put("id", "enc-001");
        data.put("status", "finished");
        data.put("class", Map.of(
                "system", "http://terminology.hl7.org/CodeSystem/v3-ActCode",
                "code", "AMB"));
        data.put("subject", Map.of("reference", "Patient/UPID-12345"));
        return mapper.valueToTree(data);
    }

    static JsonNode validObservation() {
        return mapper.valueToTree(Map.of(
                "resourceType", "Observation",
                "id", "obs-001",
                "status", "final",
                "code", Map.of("coding", List.of(Map.of(
                        "system", "http://loinc.org",
                        "code", "29463-7",
                        "display", "Body Weight"))),
                "subject", Map.of("reference", "Patient/UPID-12345")));
    }

    static JsonNode validCondition() {
        return mapper.valueToTree(Map.of(
                "resourceType", "Condition",
                "id", "cond-001",
                "clinicalStatus", Map.of("coding", List.of(Map.of(
                        "system", "http://terminology.hl7.org/CodeSystem/condition-clinical",
                        "code", "active"))),
                "subject", Map.of("reference", "Patient/UPID-12345")));
    }

    static JsonNode validPatient() {
        return mapper.valueToTree(Map.of(
                "resourceType", "Patient",
                "id", "UPID-12345",
                "identifier", List.of(Map.of(
                        "system", "http://openphc.org/identifier/upid",
                        "value", "UPID-12345")),
                "name", List.of(Map.of(
                        "family", "KAYITESI",
                        "given", List.of("Marie-Claire")))));
    }

    static JsonNode validRelatedPerson() {
        return mapper.valueToTree(Map.of(
                "resourceType", "RelatedPerson",
                "id", "rp-001",
                "patient", Map.of("reference", "Patient/UPID-12345")));
    }
}
