create database netflix;
use netflix;
show tables;
select * from netflix_data;


																			-- DATA CLEANING --

-- to check how many rows my data contains.
select count(*) from netflix_data;

-- createing a primary key
alter table netflix_data modify column show_id varchar(10) primary key;

-- check duplicates 
select show_id, count(*)
from netflix_data
group by show_id
having count(*) > 1;

select * from netflix_data
where title in ( select title from netflix_data
group by title
having count(*) > 1)
order by title;


-- Removing/deleting Duplicates
delete from netflix_data where show_id = 's304';
delete from netflix_data where show_id = 's160';
delete from netflix_data where show_id = 's1271';
delete from netflix_data where show_id = 's5026';
select count(*) from netflix_data; 


                                                              -- creating new tables for sorting data
                                                              
-- DIRECTOR TABLE
CREATE TABLE netflix_director AS
WITH RECURSIVE split_cte AS (
  SELECT 
    show_id,
    TRIM(SUBSTRING_INDEX(director, ',', 1)) AS director,
    SUBSTRING(director, LENGTH(SUBSTRING_INDEX(director, ',', 1)) + 2) AS rest
  FROM netflix_data
  WHERE director IS NOT NULL AND director != ''

  UNION ALL

  SELECT 
    show_id,
    TRIM(SUBSTRING_INDEX(rest, ',', 1)),
    SUBSTRING(rest, LENGTH(SUBSTRING_INDEX(rest, ',', 1)) + 2)
  FROM split_cte
  WHERE rest IS NOT NULL AND rest != ''
)
SELECT show_id, director FROM split_cte;


-- CAST TABLE
create table netflix_cast as
WITH RECURSIVE split_cte AS (
  SELECT 
    show_id,
    TRIM(SUBSTRING_INDEX(casts, ',', 1)) AS casts,
    SUBSTRING(casts, LENGTH(SUBSTRING_INDEX(casts, ',', 1)) + 2) AS rest
  FROM netflix_data
  WHERE casts IS NOT NULL AND casts != ''

  UNION ALL

  SELECT 
    show_id,
    TRIM(SUBSTRING_INDEX(rest, ',', 1)),
    SUBSTRING(rest, LENGTH(SUBSTRING_INDEX(rest, ',', 1)) + 2)
  FROM split_cte
  WHERE rest IS NOT NULL AND rest != ''
  )
  SELECT show_id, casts FROM split_cte;
  
  
  -- COUNTRY TABLE
  create table netflix_country as
  WITH RECURSIVE split_cte AS (
  SELECT 
    show_id,
    TRIM(SUBSTRING_INDEX(country, ',', 1)) AS country,
    SUBSTRING(country, LENGTH(SUBSTRING_INDEX(country, ',', 1)) + 2) AS rest
  FROM netflix_data
  WHERE country IS NOT NULL AND country != ''

  UNION ALL

  SELECT 
    show_id,
    TRIM(SUBSTRING_INDEX(rest, ',', 1)),
    SUBSTRING(rest, LENGTH(SUBSTRING_INDEX(rest, ',', 1)) + 2)
  FROM split_cte
  WHERE rest IS NOT NULL AND rest != ''
  )
  SELECT show_id, country FROM split_cte;
  
    
  -- GENRE TABLE
  create table netflix_genre as
  WITH RECURSIVE split_cte AS (
  SELECT 
    show_id,
    TRIM(SUBSTRING_INDEX(listed_in, ',', 1)) AS listed_in,
    SUBSTRING(listed_in, LENGTH(SUBSTRING_INDEX(listed_in, ',', 1)) + 2) AS rest
  FROM netflix_data
  WHERE listed_in IS NOT NULL AND listed_in != ''

  UNION ALL

  SELECT 
    show_id,
    TRIM(SUBSTRING_INDEX(rest, ',', 1)),
    SUBSTRING(rest, LENGTH(SUBSTRING_INDEX(rest, ',', 1)) + 2)
  FROM split_cte
  WHERE rest IS NOT NULL AND rest != ''
  )
  SELECT show_id, listed_in FROM split_cte;
  

-- DATA CONVERSION AS WELL AS DATA TYPE CONVERSION
create table netflix_stg as
with cte as (
select *, row_number() over (partition by title, types order by show_id) as rn
from netflix_data )
select show_id, types, title, str_to_date(date_added, '%M %d, %Y') as date_added, release_year, rating, case when duration is null then rating else duration end as duration, description
from cte;
select * from netflix_stg;

-- MISSING VALUES for country and duration columns
insert into netflix_country
select show_id, m.country
from netflix_data nd
inner join (
select director, country 
from netflix_country nc
inner join netflix_director nf on nc.show_id = nf.show_id
group by director, country
) m on nd.director = m.director
where nd.country is null;

select * from netflix_data where duration is null;
select * from netflix_stg where duration is null;
select * from netflix_stg where date_added is null;


                                                                -- DAtA ANALYSIS -- 

-- 1. For each director count the no. of movies and tv shows created by them in separate columns for directors who have created tv shows and movies both?
select nd.director,
count(distinct case when ns.types = 'movie' then ns.show_id end) as no_of_movies,
count(distinct case when ns.types = 'TV Show' then ns.show_id end) as no_of_TVshow
from netflix_stg ns 
inner join netflix_director nd on ns.show_id = nd.show_id
group by nd.director
having count(distinct ns.types) > 1;

-- 2. Which country has highest number of comedy movies
select nc.country, count(ng.show_id) as comedy
from netflix_country nc
join netflix_genre ng on nc.show_id = ng.show_id
join netflix_stg ns on ns.show_id = ng.show_id
where ng.listed_in = 'comedies' and ns.types = 'movie'
group by nc.country
order by comedy desc
limit 3;

-- 3. For each year (as per date added to netflix), which director has maximum number of movies released
select year(date_added) as date_year, count(ns.show_id) as no_of_movies, nd.director
from netflix_stg ns
inner join netflix_director nd on ns.show_id = nd.show_id
where ns.types = 'movie'
group by date_year, nd.director
order by no_of_movies desc;

-- 4. what is average duration of movies in each genre
/*select distinct ng.listed_in, round(avg(ns.duration), 2)  as avg_duration
from netflix_genre ng
join netflix_stg ns on ns.show_id = ng.show_id
where ns.types = 'movie'
group by ng.listed_in
order by avg_duration desc;*/
select distinct ng.listed_in, avg(cast(replace(ns.duration, 'min', ' ') as unsigned)) as avg_duration
from netflix_genre ng
join netflix_stg ns on ns.show_id = ng.show_id
where ns.types = 'movie'
group by ng.listed_in
order by avg_duration desc;


-- 5. Find the list of directors who have created horror and comedy movies both. 
-- display director names along with the number of comedy and horror movies directed by them.alter.
select nd.director,
count(distinct case when ng.listed_in = 'comedies' then ns.show_id end) as no_of_comedy,
count(distinct case when ng.listed_in = 'Horror Movies' then ns.show_id end) as no_of_horror
from netflix_stg ns
join netflix_genre ng on ng.show_id = ns.show_id
join netflix_director nd on nd.show_id = ns.show_id
where types = 'movie' and ng.listed_in in ('Comedies', 'Horror Movies')
group by nd.director
having count(distinct ng.listed_in) = 2; 


