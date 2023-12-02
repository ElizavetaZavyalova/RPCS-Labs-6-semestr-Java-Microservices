package ru.mai.lessons.rpks.metrics;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.actuate.info.Info;
import org.springframework.boot.actuate.info.InfoContributor;
import org.springframework.stereotype.Component;
import ru.mai.lessons.rpks.repository.EnrichmentRepository;

@Component
@RequiredArgsConstructor
public class EnrichmentMetrics implements InfoContributor {
    private final EnrichmentRepository enrichmentRepository;
    private  static final String COUNT_ENRICHMENTS="countEnrichments";
    @Override
    public void contribute(Info.Builder builder) {
        builder.withDetail(COUNT_ENRICHMENTS, enrichmentRepository.count());
    }
}
