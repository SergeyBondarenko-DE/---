# Проект "Электронный брокер-консультант"
**Цели проекта**
- **Сегментация клиентов по портфелю ценных бумаг**: Разделение клиентов на группы в зависимости от размера их портфеля для лучшего понимания их потребностей и предпочтений в управлении инвестициями.
- **Сегментация клиентов по внепортфельным операциям**: Классификация клиентов в зависимости от их банковских транзакций, что помогает в оптимизации финансовых потоков и предложении персонализированных решений.
- **Таргетированные маркетинговые предложения**: Предоставление клиентам персонализированных предложений, направленных на увеличение прибыли банка и портфеля ценных бумаг клиента.


##### [ER-диаграмма базы данных](https://wiki.astondevs.ru/pages/viewpage.action?pageId=132245580)

**Шаги по реализиции проекта:**
##### 1. Проверка данных перед загрузкой в БД:
- использую скрипт [unique.py](https://git.astondevs.ru/laboratory/hadoop/lab-projects/s.bondarenko_wave15_projecta/-/blob/development/unique.py?ref_type=heads) для проверки уникальности отдельных колонок
- использую скрипт [unique_rows.py](https://git.astondevs.ru/laboratory/hadoop/lab-projects/s.bondarenko_wave15_projecta/-/blob/development/unique_rows.py?ref_type=heads) для проверки уникальности комбинации нескольких колонок. 
*Уникальность колонок*  
![уникальность колонок](images/1.png){ width=800px } 

##### 2. Создание базовых таблиц и загрузка в них данных:
Создаю базовые таблицы и загружаю в них данные. По одному скрипту на каждую таблицу:  
- [script_clients](https://git.astondevs.ru/laboratory/hadoop/lab-projects/s.bondarenko_wave15_projecta/-/blob/development/script_clients.py?ref_type=heads)
- [script_securities](https://git.astondevs.ru/laboratory/hadoop/lab-projects/s.bondarenko_wave15_projecta/-/blob/development/script_securities.py?ref_type=heads)   
- [script_bank_transactions](https://git.astondevs.ru/laboratory/hadoop/lab-projects/s.bondarenko_wave15_projecta/-/blob/development/script_bank_transactions.py?ref_type=heads)
- [script_security_transactions](https://git.astondevs.ru/laboratory/hadoop/lab-projects/s.bondarenko_wave15_projecta/-/blob/development/script_security_transactions.py?ref_type=heads)

##### 3. Генератор фейковых данных:
Запускаю генератор фейковых данных [data_generator.py](https://git.astondevs.ru/laboratory/hadoop/lab-projects/s.bondarenko_wave15_projecta/-/blob/development/data_generator.py?ref_type=heads). Получаю 4 csv-файла с данными.    

##### 4. Создание копий базовых таблиц - буферных и загрузка в них сгенерированных данных:
- [script_clients_buff](https://git.astondevs.ru/laboratory/hadoop/lab-projects/s.bondarenko_wave15_projecta/-/blob/development/script_clients_buff.py?ref_type=heads)
- [script_securities_buff](https://git.astondevs.ru/laboratory/hadoop/lab-projects/s.bondarenko_wave15_projecta/-/blob/development/script_securities_buff.py?ref_type=heads)
- [script_bank_transactions_buff](https://git.astondevs.ru/laboratory/hadoop/lab-projects/s.bondarenko_wave15_projecta/-/blob/development/script_bank_transactions_buff.py?ref_type=heads) 
- [script_security_transactions_buff](https://git.astondevs.ru/laboratory/hadoop/lab-projects/s.bondarenko_wave15_projecta/-/blob/development/script_security_transactions_buff.py?ref_type=heads)    
Дополнительно в буферные таблицы добавлена колонка с полем _**upload_date**_, фиксирующая время загрузки.  
Реализована автоматическая очистка таблиц перед загрузкой новых данных.

##### 5. Обновление базовых таблиц:
В базовые таблицы дабавляю колонку с полем _**upload_date**_, фиксирующую время загрузки и колонку _**status**_ c двумя возможными значениями:  
A (active) или D (deprecated)  
**Пример кода для таблицы securities**
```shell
ALTER TABLE bondarenko.securities
ADD COLUMN upload_date TIMESTAMP DEFAULT NOW();
-- Шаг 1: Добавить колонку без NOT NULL
ALTER TABLE bondarenko.securities
ADD COLUMN status CHAR(1) DEFAULT 'A' CHECK (status IN ('A', 'D'));
-- Шаг 2: Обновить существующие строки, заполнив значение 'A'
UPDATE bondarenko.securities
SET status = 'A'
WHERE status IS NULL;
-- Шаг 3: Добавить ограничение NOT NULL
ALTER TABLE bondarenko.securities
ALTER COLUMN status SET NOT NULL;
```
##### 6. Хранимые процедуры для переноса данных:
Использую хранимые процедуры для выполнения переноса данных из буферных таблиц в базовые.  
`call bondarenko.merge_securities();` - пример команды вызова хранимой процедуры.  
В базовую таблицу добавляются новые записи из буферной со статусом 'А'. 
Если запись из базовой таблицы равна записи из буферной таблицы - старая запись в базовой таблице остается, но получает статус 'D' вместо 'А'.   
Для таблиц bank_transactions и security_transactions проверка на уникальность выполняется по составному ключу: (client_id, transaction_date, transaction_type) и (client_id, security_id, transaction_date) соответственно. 

##### 7. Преобразование и загрузка данных для таблиц 'currencies' и 'currencies_buff'
Аналогичные действия проделал для таблиц [currencies](https://git.astondevs.ru/laboratory/hadoop/lab-projects/s.bondarenko_wave15_projecta/-/blob/development/script_currencies.py) и [currencies_buff](https://git.astondevs.ru/laboratory/hadoop/lab-projects/s.bondarenko_wave15_projecta/-/blob/development/script_currencies_buff.py).  
Предварительно были получены данные о курсах валют с сайта ЦБ РФ, преобразованы в табличный формат (DataFrame) с помощью библиотеки pandas и сохранены в файл формата CSV



#### Генерация данных и запись в HDFS с использованием PySpark
Генерирую синтетические данные с помощью стандартных Python-библиотек (Faker, pandas, random, numpy).
Преобразую данные в DataFrame библиотеки pandas, затем в PySpark DataFrame для обработки в распределённой среде.
Записываю данные в HDFS на удалённом сервере, используя Apache Spark и метод .write.csv.
1. Копирование файла на удалённый сервер
Для начала копирую файл [spark_hdfs.py](https://git.astondevs.ru/laboratory/hadoop/lab-projects/s.bondarenko_wave15_projecta/-/blob/development/spark_hdfs.py) с локального компьютера в файловую систему сервера, в домашнюю директорию пользователя с помощью команды scp:
```
scp D:\praktika\spark_hdfs.py s.bondarenko@172.17.0.23:/home/s.bondarenko
```
2. Подключение к удалённому серверу
```bash
ssh s.bondarenko@172.17.0.23
```
3. Установка PySpark и необходимых библиотек на удалённом сервере
``` python
pip install pyspark pandas numpy faker
```
4. Запуск Spark-скрипта на удалённом сервере через YARN с помощью команды spark-submit:
```
spark-submit --master yarn /home/s.bondarenko/spark_hdfs.py
```

