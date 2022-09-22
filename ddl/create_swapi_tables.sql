CREATE SCHEMA `swapi`;

CREATE TABLE `swapi`.`people_raw` (
  `name` VARCHAR(255) NULL,
  `height` VARCHAR(255) NULL,
  `new_tablecol` VARCHAR(255) NULL,
  `mass` VARCHAR(255) NULL,
  `hair_color` VARCHAR(255) NULL,
  `skin_color` VARCHAR(255) NULL,
  `eye_color` VARCHAR(255) NULL,
  `birth_year` VARCHAR(255) NULL,
  `gender` VARCHAR(255) NULL,
  `homeworld` VARCHAR(255) NULL,
  `films` VARCHAR(255) NULL,
  `species` VARCHAR(255) NULL,
  `vehicles` VARCHAR(255) NULL,
  `starships` VARCHAR(255) NULL,
  `created` VARCHAR(255) NULL,
  `edited` VARCHAR(255) NULL,
  `url` VARCHAR(255) NULL,
  `ingested_datetime` VARCHAR(255) NULL);


  
  CREATE TABLE `swapi`.`people_report` (
  `name` VARCHAR(255) NULL,
  `birth_year` FLOAT NULL,
  `films` VARCHAR(255) NULL,
  `ingested_datetime` DATETIME NULL,
  `raw_ingested_datetime` VARCHAR(255) NULL);



create view swapi.oldes_charc_by_film as
select name, films, birth_year, BirthYearRank
from (
  select name, films, birth_year,
  DENSE_RANK() OVER(PARTITION BY films ORDER BY birth_year ASC) AS BirthYearRank
  # dense_rank() over ( partition by films, birth_year order by birth_year asc ) as "row"
  from swapi.people_report
  where birth_year > 0 and films <> '' # and films = 'https://swapi.dev/api/films/2/'
  order by films
) a
where BirthYearRank = 1;