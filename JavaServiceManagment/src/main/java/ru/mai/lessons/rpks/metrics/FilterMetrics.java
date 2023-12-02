package ru.mai.lessons.rpks.metrics;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.actuate.info.Info;
import org.springframework.boot.actuate.info.InfoContributor;
import org.springframework.stereotype.Component;
import ru.mai.lessons.rpks.repository.FilterRepository;

@Component
@RequiredArgsConstructor
public class FilterMetrics implements InfoContributor {
    private final FilterRepository filterRepository;
    private  static final String COUNT_FILTERS="countFilters";
    @Override
    public void contribute(Info.Builder builder) {
        builder.withDetail(COUNT_FILTERS, filterRepository.count());
    }
}
