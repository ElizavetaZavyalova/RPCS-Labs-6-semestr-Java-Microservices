package ru.mai.lessons.rpks.repository;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;
import ru.mai.lessons.rpks.model.Enrichment;
import java.util.List;

@Repository
public interface EnrichmentRepository extends CrudRepository<Enrichment, Long> {
    List<Enrichment> findByEnrichmentIdAndRuleId(long enrichmentId, long ruleId);
    Iterable<Enrichment> findByEnrichmentId(long enrichmentId);
    void deleteAllByEnrichmentIdAndAndRuleId(long enrichmentId, long ruleId);
}
