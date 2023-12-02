package ru.mai.lessons.rpks.controller;

import io.swagger.v3.oas.annotations.Operation;
import jakarta.transaction.Transactional;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import ru.mai.lessons.rpks.model.Enrichment;
import ru.mai.lessons.rpks.repository.EnrichmentRepository;
import java.util.List;

@RestController
@RequestMapping("enrichment")
public class EnrichmentController {
    @Autowired
    private EnrichmentRepository enrichmentRepository;

    @GetMapping("/findAll")
    @ResponseStatus(value = HttpStatus.OK)
    @Operation(summary = "Получить информацию о всех правилах обогащения в БД")
    public Iterable<Enrichment> getAllEnrichments() {
        return enrichmentRepository.findAll();
    }

    @GetMapping("/findAll/{id}")
    @ResponseStatus(value = HttpStatus.OK)
    @Operation(summary = "Получить информацию о всех правилах обогащения в БД по enrichment id")
    public Iterable<Enrichment> getAllEnrichmentsByEnrichmentId(@PathVariable long id) {
        return enrichmentRepository.findByEnrichmentId(id);
    }

    @GetMapping("/find/{enrichmentId}/{ruleId}")
    @ResponseStatus(value = HttpStatus.OK)
    @Operation(summary = "Получить информацию о правиле обогащения по enrichment id и rule id")
    public Enrichment getEnrichmentById(@PathVariable long enrichmentId, @PathVariable long ruleId) {
        List<Enrichment> enrichmentList=enrichmentRepository.findByEnrichmentIdAndRuleId(enrichmentId,ruleId);
        return enrichmentList.isEmpty()?(null):enrichmentList.get(0);
    }

    @DeleteMapping("/delete")
    @ResponseStatus(value = HttpStatus.OK)
    @Operation(summary = "Удалить информацию о всех правилах обогащения")
    public void deleteEnrichment() {
        enrichmentRepository.deleteAll();
    }

    @Transactional
    @DeleteMapping("/delete/{enrichmentId}/{ruleId}")
    @ResponseStatus(value = HttpStatus.OK)
    @Operation(summary = "Удалить информацию по конкретному правилу обогащения с enrichment id и rule id")
    public void deleteEnrichmentById(@PathVariable long enrichmentId, @PathVariable long ruleId) {
         enrichmentRepository.deleteAllByEnrichmentIdAndAndRuleId(enrichmentId,ruleId);
    }

    @PostMapping("/save")
    @ResponseStatus(value = HttpStatus.CREATED)
    @Operation(summary = "Создать правило обогащения")
    public void save(@RequestBody @Valid Enrichment enrichment) {
        enrichmentRepository.save(enrichment);
    }

}
