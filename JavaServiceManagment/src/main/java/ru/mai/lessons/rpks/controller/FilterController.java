package ru.mai.lessons.rpks.controller;

import io.swagger.v3.oas.annotations.Operation;
import jakarta.transaction.Transactional;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import ru.mai.lessons.rpks.model.Filter;
import ru.mai.lessons.rpks.repository.FilterRepository;
import java.util.List;

@RestController
@RequestMapping("filter")
public class FilterController {

    @Autowired
    private FilterRepository filterRepository;

    @GetMapping("/findAll")
    @Operation(summary = "Получить информацию о всех фильтрах в БД")
    @ResponseStatus(value = HttpStatus.OK)
    public Iterable<Filter> getAllFilters() {
        return filterRepository.findAll();
    }

    @GetMapping("/findAll/{id}")
    @ResponseStatus(value = HttpStatus.OK)
    @Operation(summary = "Получить информацию о всех фильтрах в БД по filter id")
    public Iterable<Filter> getAllFiltersByFilterId(@PathVariable long id) {
        return filterRepository.findByFilterId(id);
    }

    @GetMapping("/find/{filterId}/{ruleId}")
    @ResponseStatus(value = HttpStatus.OK)
    @Operation(summary = "Получить информацию о фильтре по filter id и rule id")
    public Filter getFilterByFilterIdAndRuleId(@PathVariable long filterId, @PathVariable long ruleId) {
        List<Filter> filters=filterRepository.findByFilterIdAndRuleId(filterId,ruleId);
        return filters.isEmpty()?null:filters.get(0);

    }

    @DeleteMapping("/delete")
    @ResponseStatus(value = HttpStatus.OK)
    @Operation(summary = "Удалить информацию о всех фильтрах")
    public void deleteFilter() {
        filterRepository.deleteAll();
    }
    @Transactional
    @DeleteMapping("/delete/{filterId}/{ruleId}")
    @ResponseStatus(value = HttpStatus.OK)
    @Operation(summary = "Удалить информацию по конкретному фильтру filter id и rule id")
    public void deleteFilterById(@PathVariable long filterId, @PathVariable long ruleId) {
        filterRepository.deleteAllByFilterIdAndAndRuleId(filterId,ruleId);

    }
    @PostMapping("/save")
    @Operation(summary = "Создать фильтр")
    @ResponseStatus(value = HttpStatus.CREATED)
    public void save(@RequestBody @Valid Filter filter) {
        filterRepository.save(filter);
    }

}
