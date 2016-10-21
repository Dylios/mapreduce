-- Creation de la database si elle n'existe pas
CREATE DATABASE IF NOT EXISTS tlorillon;

-- On utilise la DB
USE tlorillon;

-- Création de la table CSV
CREATE EXTERNAL TABLE prenoms (name STRING, sexe STRING, origine STRING, version DOUBLE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\;'
STORED AS TEXTFILE LOCATION '/user/tlorillon/hive/prenoms';

-- Remplissage de la table avec le fichier csv
LOAD DATA INPATH '/user/tlorillon/hive/prenoms.csv' INTO TABLE prenoms;

-- Création de la table ORC
CREATE EXTERNAL TABLE prenoms2 (name STRING, sexe ARRAY<STRING>, origine ARRAY<STRING>, version DOUBLE)
STORED AS ORC LOCATION '/user/tlorillon/hive/prenoms2';

-- On copie prenoms dans prenoms2 (csv dasn ORC)
INSERT INTO TABLE prenoms2 SELECT name,IF(sexe="", array(),split(sexe,",\\ ?")) AS sexe,IF(origine="",array(),split(origine,",\\ ?")) AS origine, version FROM prenoms;

-- 1ere: Find the number of first names for each origin.
SELECT origine, COUNT(name) AS nbr_name FROM (SELECT origine_ex AS origine, name FROM prenoms2 LATERAL VIEW explode(origine) tmp AS origine_ex) AS tmp GROUP BY origine;

-- 2ème: Find the number of first names for each number of origins.
SELECT nbr_origine, COUNT(name) AS name_count FROM (SELECT size(origine) AS nbr_origine, name FROM prenoms2) AS tmp GROUP BY nbr_origine;

-- 3ème: Find the proportion for each gender.
SELECT sexe, nbr_sexe / total * 100.0 AS sexe_p FROM (SELECT sexe_ex AS sexe, COUNT(sexe_ex) AS nbr_sexe FROM prenoms2 LATERAL VIEW explode(sexe) tmp AS sexe_ex GROUP BY sexe_ex) AS tmp1 JOIN (SELECT COUNT(*) AS total FROM prenoms) AS tmp2;