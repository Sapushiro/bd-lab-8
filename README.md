# OpenFoodFacts KMeans Pipeline

Лабораторная работа по построению распределённого ML-pipeline для кластеризации продуктов из датасета OpenFoodFacts.

В проекте используются:

- Apache Spark / PySpark — обработка данных и обучение модели;
- Scala + Spark — реализация Data Mart;
- Apache Cassandra — источник и хранилище результатов;
- Kubernetes — запуск Spark Driver и Executor;
- Docker — контейнеризация компонентов;
- PersistentVolumeClaim — обмен данными между этапами pipeline;
- KMeans — кластеризация продуктов.

## Архитектура

Полный pipeline:

```text
OpenFoodFacts Parquet
        │
        │ ingest.sh
        ▼
     PySpark
        │
        ▼
Cassandra
openfoodfacts.product_raw
        │
        │ prepare.sh
        ▼
Scala Data Mart
        │
        ▼
PVC: /data/model_input.parquet
        │
        │ train.sh
        ▼
PySpark KMeans
        │
        ├── /data/models/openfoodfacts_kmeans
        ├── /data/artifacts/kmeans_metrics.json
        │
        ▼
PVC: /data/model_output.parquet
        │
        │ publish.sh
        ▼
Scala Data Mart
        │
        ▼
Cassandra
openfoodfacts.product_clusters
```

## Структура проекта

```text
bd-lab-8/
├── data-mart/
│   ├── project/
│   │   └── build.properties
│   ├── src/main/scala/
│   │   ├── CassandraRepository.scala
│   │   ├── Config.scala
│   │   ├── Main.scala
│   │   └── Preprocessor.scala
│   ├── build.sbt
│   └── Dockerfile
│
├── model/
│   ├── src/
│   │   ├── cassandra_ingestion.py
│   │   ├── logger.py
│   │   ├── spark_session.py
│   │   └── train.py
│   ├── config.json
│   ├── Dockerfile
│   ├── requirements.txt
│   └── requirements-docker.txt
│
├── k8s/
│
├── scripts/
    ├── ingest.sh
    ├── prepare.sh
    ├── train.sh
    ├── publish.sh
    └── init_cassandra.cql
```

## Данные

В качестве исходного датасета используется OpenFoodFacts в формате Parquet.

Для модели используются следующие признаки:

| Признак | Описание |
|---|---|
| `energy_kcal` | энергетическая ценность продукта |
| `fat` | содержание жиров |
| `carbohydrates` | содержание углеводов |
| `proteins` | содержание белков |
| `sugars` | содержание сахаров |

Поле `code` используется как идентификатор продукта.

Количество обрабатываемых продуктов задаётся в `model/config.json`:

```json
"max_rows": 100000
```

## Cassandra

Используется keyspace:

```text
openfoodfacts
```

### Исходные данные

Таблица:

```text
openfoodfacts.product_raw
```

Структура:

```sql
CREATE TABLE IF NOT EXISTS openfoodfacts.product_raw (
    code text PRIMARY KEY,
    energy_kcal double,
    fat double,
    carbohydrates double,
    proteins double,
    sugars double
);
```

### Результаты кластеризации

Таблица:

```text
openfoodfacts.product_clusters
```

Структура:

```sql
CREATE TABLE IF NOT EXISTS openfoodfacts.product_clusters (
    code text PRIMARY KEY,
    energy_kcal double,
    fat double,
    carbohydrates double,
    proteins double,
    sugars double,
    prediction int
);
```

`prediction` содержит номер кластера, назначенного моделью KMeans.

## Kubernetes

Все Spark-приложения запускаются в Kubernetes в namespace:

```text
bd-lab-8
```

Проверить состояние ресурсов:

```bash
kubectl get pods -n bd-lab-8
```

Для Spark используется ServiceAccount:

```text
spark
```

Spark запускается в `cluster` deploy mode.

При таком режиме Kubernetes создаёт отдельный Driver Pod, который управляет Spark-приложением, и Executor Pods, выполняющие распределённые вычисления.

## Persistent Volume

Для передачи данных между независимыми Spark-приложениями используется PVC:

```text
spark-data
```

Он монтируется в Driver и Executor контейнеры:

```text
/data
```

Основные данные:

```text
/data/food.parquet
/data/model_input.parquet
/data/model_output.parquet
/data/models/openfoodfacts_kmeans
/data/artifacts/kmeans_metrics.json
```

Проверить содержимое PVC можно через `data-loader`:

```bash
kubectl exec -n bd-lab-8 data-loader -- \
  find /data -maxdepth 2 -type f
```

## Docker Images

В pipeline используются два Docker-образа.

### PySpark / ML

```text
sapushiro/bd-lab-8-spark:latest
```

Используется для:

- ingestion исходных данных;
- обучения KMeans;
- формирования predictions;
- сохранения модели и метрик.

Сборка:

```bash
docker build \
  -f model/Dockerfile \
  -t sapushiro/bd-lab-8-spark:latest \
  model
```

Публикация:

```bash
docker push sapushiro/bd-lab-8-spark:latest
```

### Scala Data Mart

```text
sapushiro/bd-lab-8-data-mart:latest
```

Используется для:

- подготовки данных из Cassandra;
- публикации результатов модели обратно в Cassandra.

Перед сборкой Docker-образа необходимо собрать Scala JAR:

```bash
cd data-mart

sbt clean compile
sbt package

cd ..
```

После этого:

```bash
docker build \
  -f data-mart/Dockerfile \
  -t sapushiro/bd-lab-8-data-mart:latest \
  .
```

И:

```bash
docker push sapushiro/bd-lab-8-data-mart:latest
```

## Запуск pipeline

Для запуска отдельных этапов используются shell-скрипты из директории `scripts`.

### 1. Ingestion

```bash
./scripts/ingest.sh
```

PySpark читает исходный:

```text
/data/food.parquet
```

Из массива `nutriments` извлекаются необходимые признаки.

После преобразования данные записываются в:

```text
Cassandra → openfoodfacts.product_raw
```

### 2. Prepare

```bash
./scripts/prepare.sh
```

Scala Data Mart читает продукты из:

```text
openfoodfacts.product_raw
```

После preprocessing формируется датасет для ML:

```text
/data/model_input.parquet
```

### 3. Train

```bash
./scripts/train.sh
```

PySpark читает:

```text
/data/model_input.parquet
```

Признаки объединяются в feature vector и масштабируются.

Для нескольких значений `k` обучаются модели KMeans.

Качество кластеризации оценивается с помощью Silhouette Score.

Лучшая модель сохраняется в:

```text
/data/models/openfoodfacts_kmeans
```

Метрики:

```text
/data/artifacts/kmeans_metrics.json
```

Предсказания:

```text
/data/model_output.parquet
```

### 4. Publish

```bash
./scripts/publish.sh
```

Scala Data Mart читает:

```text
/data/model_output.parquet
```

и записывает результаты в:

```text
openfoodfacts.product_clusters
```

## Проверка результата

Подключиться к Cassandra:

```bash
kubectl exec -it -n bd-lab-8 <cassandra-pod> -- cqlsh
```

Получить несколько результатов:

```sql
SELECT *
FROM openfoodfacts.product_clusters
LIMIT 10;
```

В результате для каждого продукта должны присутствовать признаки и поле:

```text
prediction
```

с номером назначенного кластера.

## ML Pipeline

Перед обучением выполняются:

1. выбор необходимых признаков;
2. формирование feature vector через `VectorAssembler`;
3. масштабирование через `StandardScaler`;
4. обучение KMeans;
5. вычисление Silhouette Score;
6. выбор лучшего значения `k`;
7. сохранение лучшей модели;
8. сохранение predictions и metrics.

Параметры модели задаются в:

```text
model/config.json
```

Основные параметры:

```json
{
  "min_k": 2,
  "max_k": 8,
  "seed": 42,
  "max_iter": 50,
  "tolerance": 0.0001
}
```

## Data Mart

Data Mart реализован на Scala и Spark.

Он имеет два режима работы.

### `prepare`

```text
Cassandra
    ↓
Scala Data Mart
    ↓
preprocessing
    ↓
model_input.parquet
```

### `publish`

```text
model_output.parquet
    ↓
Scala Data Mart
    ↓
Cassandra
```

Таким образом, ML-компонент не взаимодействует с Cassandra напрямую во время обучения.

Обмен между Data Mart и ML осуществляется через Parquet-файлы на общем Persistent Volume.

## Используемые технологии

| Компонент                   | Технология         |
| --------------------------- | ------------------ |
| Распределённая обработка    | Apache Spark 3.5.1 |
| ML                          | PySpark ML         |
| Алгоритм                    | KMeans             |
| Data Mart                   | Scala 2.12         |
| База данных                 | Apache Cassandra   |
| Оркестрация                 | Kubernetes         |
| Контейнеризация             | Docker             |
| Формат промежуточных данных | Parquet            |
| Постоянное хранилище        | Kubernetes PVC     |
