# Библиотека для Домашнего задания 4 ("Агрегация данных по покупкам из различных источников с обработкой ошибок") по курсу Airflow  
## Файлы
- merch/ 
  - calculators.py - генерация новых полей
  - cleaners.py - очистка данных
  - db.py - взаимодействие с БД
  - downloaders.py - скачивание данных
  - loaders.py - загрузка данных
  - notifiers.py - отправка уведомлений
  - operators.py - кастомные операторы
  - processors.py - объединение логики по обработке данных и генерации финального датасета
  - utils.py - дополнительные функции общего назначения