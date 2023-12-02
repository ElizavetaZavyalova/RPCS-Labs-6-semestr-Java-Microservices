package ru.mai.lessons.rpks.model;

import jakarta.persistence.*;
import jakarta.validation.constraints.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Entity
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "deduplication_rules")
public class Deduplication{
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long id;
    @Min(value = 1)
    private long deduplicationId;
    @Min(value = 1)
    private long ruleId;
    @NotBlank(message = "FILED_NAME_IS_REQUIRED")
    @NotNull
    private String fieldName;
    @Min(value = 0)
    private long timeToLiveSec;
    @AssertTrue
    private boolean isActive;
}
