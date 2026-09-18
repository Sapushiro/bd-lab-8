CREATE KEYSPACE IF NOT EXISTS openfoodfacts
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

CREATE TABLE IF NOT EXISTS openfoodfacts.product_raw (
    code text PRIMARY KEY,
    energy_kcal double,
    fat double,
    carbohydrates double,
    proteins double,
    sugars double
);

CREATE TABLE IF NOT EXISTS openfoodfacts.product_clusters (
    code text PRIMARY KEY,
    energy_kcal double,
    fat double,
    carbohydrates double,
    proteins double,
    sugars double,
    prediction int
);