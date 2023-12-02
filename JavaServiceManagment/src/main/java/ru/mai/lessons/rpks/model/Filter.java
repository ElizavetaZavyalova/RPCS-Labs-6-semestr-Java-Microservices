package ru.mai.lessons.rpks.model;

import jakarta.persistence.*;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Entity
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "filter_rules")
public class Filter {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long id;
    @Min(value = 1)
    private long filterId;
    @Min(value = 1)
    private long ruleId;
    @NotBlank(message = "FILTER_NAME_IS_REQUIRED")
    @NotNull
    private String fieldName;
    @NotBlank(message = "FILTER_FUNCTION_NAME_ID_IS_REQUIRED")
    @Pattern(regexp = "^equals|not_equals|contains|not_contains$")
    private String filterFunctionName;
    @NotBlank(message = "FILTER_VALUE_IS_REQUIRED")
    @NotNull
    private String filterValue;
}
