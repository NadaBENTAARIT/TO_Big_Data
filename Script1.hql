DROP TABLE IF EXISTS data_table;
DROP TABLE IF EXISTS filtered_data;
DROP TABLE IF EXISTS grouped_data;

-- Créer la table et charger les données
CREATE TABLE data_table (
    date STRING,
    time STRING,
    location STRING,
    category STRING,
    amount FLOAT,
    payment STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

LOAD DATA LOCAL INPATH 'datashop.txt' INTO TABLE data_table;

-- Filtrer les données pour l'année 2012
CREATE TABLE filtered_data AS
SELECT *
FROM data_table
WHERE SUBSTRING(date, 0, 4) = '2012';

-- Regrouper les données par emplacement
CREATE TABLE grouped_data AS
SELECT
    location,
    SUM(amount) AS total_amount
FROM filtered_data
GROUP BY location;

-- Stocker le résultat
INSERT OVERWRITE LOCAL DIRECTORY '/home/cloudera/output_hive_1'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT * FROM grouped_data;