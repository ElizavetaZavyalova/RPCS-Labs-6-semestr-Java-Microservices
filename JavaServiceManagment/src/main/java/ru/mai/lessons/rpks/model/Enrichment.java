package ru.mai.lessons.rpks.model;

import jakarta.persistence.*;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Entity
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "enrichment_rules")
public class Enrichment {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long id;
    @Min(value = 1)
    private long enrichmentId;
    @Min(value = 1)
    private long ruleId;
    @NotBlank(message = "FIELD_NAME_IS_REQUIRED")
    @NotNull
    private String fieldName;
    @NotBlank(message = "FIELD_NAME_ENRICHMENT_IS_REQUIRED")
    @NotNull
    private String fieldNameEnrichment;
    @NotBlank(message = "FIELD_VALUE_IS_REQUIRED")
    @NotNull
    private String fieldValue;
    @NotBlank(message = "FIELD_VALUE_DEFAULT_IS_REQUIRED")
    @NotNull
    private String fieldValueDefault;
}
