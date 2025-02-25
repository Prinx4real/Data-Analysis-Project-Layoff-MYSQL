-- SQL Project - Data Cleaning

-- https://www.kaggle.com/datasets/swaptr/layoffs-2022

SELECT *
 FROM world_layoffs.layoffs;

-- now when we are data cleaning we usually follow a few steps
-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary - few ways



-- 1. Remove Duplicates

# First let's check for duplicates


SELECT *
FROM world_layoffs.layoffs;

-- first thing we want to do is create a staging table. This is the one we will work in and clean the data. We want a table with the raw data in case something happens



CREATE TABLE world_layoffs.layoffs_staging
like world_layoffs.layoffs;

SELECT *
FROM world_layoffs.layoffs_staging;

insert world_layoffs.layoffs_staging
select *
from world_layoffs.layoffs;


SELECT *,
row_number() over(partition by company,location,industry, total_laid_off,`date`, stage, country, funds_raised_millions) AS row_num
FROM world_layoffs.layoffs_staging;


with duplicate_cte AS
(
SELECT *,
row_number() over(partition by company,location,industry, total_laid_off,`date`, stage, country, funds_raised_millions) AS row_num
FROM world_layoffs.layoffs_staging
)

select *
from duplicate_cte
where row_num > 1;

select *
from world_layoffs.layoffs_staging
where company = 'casper';

with duplicate_cte AS
(
SELECT *,
row_number() over(partition by company,location,industry, total_laid_off,`date`, stage, country, funds_raised_millions) AS row_num
FROM world_layoffs.layoffs_staging
)

delete
from duplicate_cte
where row_num > 1;


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `rum_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;



select *
from world_layoffs.layoffs_staging2;

insert into world_layoffs.layoffs_staging2
SELECT *,
row_number() over(partition by company,location,industry, total_laid_off,`date`, stage, country, funds_raised_millions) AS row_num
FROM world_layoffs.layoffs_staging;

delete
from world_layoffs.layoffs_staging2
where rum_num > 1;


select *
from world_layoffs.layoffs_staging2
;

-- 2. standardize data : finding issues and errors in your data and fixing it

-- if we look at company it looks like we have some wide empty spaces at the end, let's take a look at these

select company, Trim(company)
from world_layoffs.layoffs_staging2;

update world_layoffs.layoffs_staging2
set company = Trim(company);


-- I also noticed the Crypto has multiple different variations. We need to standardize that - let's say all to Crypto

select distinct industry
from world_layoffs.layoffs_staging2
order by 1;

select *
from world_layoffs.layoffs_staging2
where industry like 'crypto%';



update world_layoffs.layoffs_staging2
set industry = 'crypto'
where industry like 'crypto%' ;

-- now that's taken care of:

select distinct industry
from world_layoffs.layoffs_staging2
order by industry;

-- we also need to look at 

select *
from world_layoffs.layoffs_staging2;

-- everything looks good except apparently we have some "United States" and some "United States." with a period at the end. Let's standardize this.

select distinct country
from world_layoffs.layoffs_staging2
order by 1;

select distinct country, trim(trailing '.' from country)
from world_layoffs.layoffs_staging2
order by 1;

update world_layoffs.layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'united state%';

-- now if we run this again it is fixed
SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY country;


-- fix the date columns:
SELECT `date`
FROM world_layoffs.layoffs_staging2;

select `date`,
str_to_date(`date`, '%m/%d/%Y')
from world_layoffs.layoffs_staging2;

update world_layoffs.layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

alter table world_layoffs.layoffs_staging2
modify column `date` date;

select *
from world_layoffs.layoffs_staging2;


-- if we look at industry it looks like we have some null and empty rows, let's take a look at these
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- let's take a look at these
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'Bally%';

-- we should set the blanks to nulls since those are typically easier to work with
UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- nothing wrong here
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'airbnb';


select *
from world_layoffs.layoffs_staging2 t1
join world_layoffs.layoffs_staging2 t2
        on t1.company = t2.company
 where (t1.industry is null or t1.industry = '')
 and t2.industry is not null;
 
 -- make it clear
 
 select t1.industry, t2.industry
from world_layoffs.layoffs_staging2 t1
join world_layoffs.layoffs_staging2 t2
        on t1.company = t2.company
 where (t1.industry is null or t1.industry = '')
 and t2.industry is not null;
 
 
 -- now we need to populate those nulls if possible

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;


SELECT *
FROM world_layoffs.layoffs_staging2;


-- 3. Look at Null Values

-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. I don't think I want to change that
-- I like having them null because it makes it easier for calculations during the EDA phase

-- so there isn't anything I want to change with the null values


SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- since the data contain null total_laid_off and  percentage_laid_off it should be deleted.

DELETE FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


SELECT *
FROM world_layoffs.layoffs_staging2;

-- Drop column that is useless i.e the rum_num

alter table world_layoffs.layoffs_staging2
drop column rum_num;

-- Data is now completely clean
 
 SELECT *
FROM world_layoffs.layoffs_staging2;
 
 
 












































































