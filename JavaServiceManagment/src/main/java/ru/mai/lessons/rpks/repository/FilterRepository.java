package ru.mai.lessons.rpks.repository;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;
import ru.mai.lessons.rpks.model.Filter;
import java.util.List;

@Repository
public interface FilterRepository extends CrudRepository<Filter, Long> {
    List<Filter> findByFilterIdAndRuleId(long filterId, long ruleId);
    Iterable<Filter> findByFilterId(long filterId);
    void deleteAllByFilterIdAndAndRuleId(long filterId, long ruleId);
}
