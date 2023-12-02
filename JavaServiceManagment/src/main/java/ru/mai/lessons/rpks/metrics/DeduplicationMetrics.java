package ru.mai.lessons.rpks.metrics;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.actuate.info.Info;
import org.springframework.boot.actuate.info.InfoContributor;
import org.springframework.stereotype.Component;
import ru.mai.lessons.rpks.repository.DeduplicationRepository;

@Component
@RequiredArgsConstructor
public class DeduplicationMetrics implements InfoContributor {
    private final DeduplicationRepository deduplicationRepository;
    private  static final String COUNT_DEDUPLICATIONS="countDeduplications";
    @Override
    public void contribute(Info.Builder builder) {
        builder.withDetail(COUNT_DEDUPLICATIONS, deduplicationRepository.count());
    }
}
