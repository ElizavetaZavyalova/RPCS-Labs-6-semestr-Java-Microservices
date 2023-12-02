package ru.mai.lessons.rpks.model;


import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Builder
@ToString
@Getter
@Setter
public class Rule {
    private Long filterId; // id фильтра
    private Long ruleId; // id правила
    private String fieldName; // поле сообщения, по которому выполняем фильтрацию { "name": "Jhonas"}, fieldName = "name", Jhon
    private String filterFunctionName; // название функции фильтрации, equals, contains, not_equals, not_contains
    private String filterValue; // сравнимаемое значение, например, filterValue = Jhon, значит сообщения должны содержать в поле, заданном в fieldName значение Jhon для фильтрации
}
