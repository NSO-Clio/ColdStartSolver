# Web-app

# Структура

- ```models_and_dataset``` - папка в которую надо загрузить данные и веса, ссылка и объяснение есть в папке
- ```static``` - папка со статическими данными: js скрипты и стили
- ```templates``` - папка с html структурами
- ```app.py``` - апи
- ```RecModel.py``` - файл с классом нашего алгоритма
- ```db.py``` - класс бд

# Ход решения

- Мы пробовали несколько идей: метод коллаборативной фильтрации, различные рекомендательные модели, но в итоге по сравнениям мы остановились на ансамбле моделей из библиотеки Rectools (ImplicitItemKNNWrapperModel, PopularModel, PureSVDModel).

- Мы также меняем целевой рейтинг для модели по формуле: (время просмотра / время видео) * 5 ( в округлении)

- В результате исследования мы пришли к выводу, что модель лучше работает без текстовых переменных

# Описание архитектуры алгоритма

![photo_2024-09-29_11-21-21 (1)](https://github.com/user-attachments/assets/1622a4d8-d259-4086-be45-38416f8d4983)

- Наш итоговый продукт – сервис, на котором пользователю предлагаются топ-10 видео на основе его предпочтений
- Путь алгоритма:
  1) Обработка и соединение данных о логах просмотров и информации о видео
  2) Использование предобученных рекомендательных моделей
  3) Составление рекомендации
  4) Дополнение данных предпочтениями нового «холодного» пользователя на нашем сайте
  5) Составление новой рекомендации

# Архитектура веб-приложения

![image](https://github.com/user-attachments/assets/02b7b2f6-1297-458d-be60-aa2d26a1f65f)


# Запуск

- установка всех библиотке
```
pip install -r requirements.txt
```

- Подключение данных видео и логов пользователей, это делается в файле app.py в методе нашего алгоритма create_data:

**Так же все эти два файла должны быть в разширении .parquet**

```python
if __name__ == '__main__':
    recModel = Kyuriy()
    recModel.create_data(
        video_stat_path='models_and_dataset/YOUR_VIDEO_STAT.parquet',
        logs_df_path='models_and_dataset/YOUR_LOGS.parquet'
    )
    app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)
```

- после запускаем приложение

```
python app.py
```


# Про алгоритм


В файле RecModel.py есть класс Kyuriy, который является нашим алгоритмом

Пометка для использования только класса в будущем

## Методы

- ```create_data(self, video_stat_path: str, logs_df_path: str) -> None``` - Инициализация базы данных для вывода информации о видео. video_stat_path: Путь к файлу с статистикой видео в формате .parquet , logs_df_path: Путь к файлу с логами в формате .parquet.
- ```recommend(self, user_id: str, get_rec_type: str) -> dict | pd.DataFrame``` - Генерация рекомендаций для пользователя. user_id: Идентификатор пользователя , get_rec_type: Тип возвращаемых данных ('json' или 'DataFrame') -> Рекомендации в формате json или DataFrame
- ```fit_models(self) -> None``` - Обучение моделей на основе текущего набора данных.
- ```__reduce_mem_usage(df: pd.DataFrame) -> pd.DataFrame``` - Приватный метод. Оптимизация использования памяти путем изменения типов данных в DataFrame. param df: Исходный DataFrame ->  Оптимизированный DataFrame
- ```update_dataset(self, interactions_new_user: pd.DataFrame) -> None``` -  Обновление набора данных новыми взаимодействиями пользователей. interactions_new_user: DataFrame с колонками [datetime, user_id, item_id, weight] -> Обновленный объект Dataset

## P.S.

- перед тем как работать, делать рекомендации или дообучать модели которые есть в классе, нужно исполнить метод ```create_data(self, video_stat_path: str, logs_df_path: str) -> None```
