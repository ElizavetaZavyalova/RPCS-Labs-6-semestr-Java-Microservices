package ru.mai.lessons.rpks;

import com.typesafe.config.Config;

public interface ConfigReader {//Есть
    public Config loadConfig(); // метод читает конфигурацию из файла *.conf
}
