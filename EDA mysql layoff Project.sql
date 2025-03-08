-- EDA

-- Here we are jsut going to explore the data and find trends or patterns or anything interesting like outliers

-- normally when you start the EDA process you have some idea of what you're looking for

-- with this info we are just going to look around and see what we find!


SELECT *
FROM world_layoffs.layoffs_staging2;

select MAX(total_laid_off)
FROM world_layoffs.layoffs_staging2;

-- Calculate the maximum and minimum percentage_laid_off
SELECT MAX(percentage_laid_off),  MIN(percentage_laid_off)
FROM world_layoffs.layoffs_staging2
WHERE  percentage_laid_off IS NOT NULL;

-- Which companies had 1 which is basically 100 percent of they company laid off
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE  percentage_laid_off = 1;
-- these are mostly startups it looks like who all went out of business during this time


-- Calculate based on the company, location, indudtry and date with the most total_laid_off

-- Companies with the biggest single Layoff

SELECT company,sum(total_laid_off)
FROM world_layoffs.layoffs_staging2
group by company
ORDER BY 2 DESC
LIMIT 10;

-- by location
SELECT location, sum(total_laid_off)
FROM world_layoffs.layoffs_staging2
group by location
ORDER BY 2 DESC
LIMIT 10;

-- industry
SELECT industry, sum(total_laid_off)
FROM world_layoffs.layoffs_staging2
group by industry
ORDER BY 2 DESC
LIMIT 10;

-- by country
SELECT country, sum(total_laid_off)
FROM world_layoffs.layoffs_staging2
group by country
ORDER BY 2 DESC
LIMIT 10;


-- by date
select year(`date`), sum(total_laid_off)
from world_layoffs.layoffs_staging2
where year(`date`) is not null
group by year(`date`)
order by 1 desc;

-- by stage
select stage, sum(total_laid_off)
from world_layoffs.layoffs_staging2
group by stage
order by 2 desc
limit 10;


select substring(`date`,1,7) as `month`, sum(total_laid_off)
from world_layoffs.layoffs_staging2
where substring(`date`,1,7)  is not null
group by `month`
order by 1 asc;


with rolling_total as
(
select substring(`date`,1,7) as `month`, sum(total_laid_off) as total_off
from world_layoffs.layoffs_staging2
where substring(`date`,1,7)  is not null
group by `month`
order by 1 asc
)

select `month`, total_off, sum(total_off) over(order by `month`) as rolling_total
from rolling_total;



-- by country
SELECT country, sum(total_laid_off)
FROM world_layoffs.layoffs_staging2
group by country
ORDER BY 2 DESC;

-- which year each company laid off employee the most
SELECT company, year(`date`), sum(total_laid_off)
FROM world_layoffs.layoffs_staging2
group by company, year(`date`)
ORDER BY 3 desc;

with company_year ( company, years, total_laid_off) as
(
SELECT company, year(`date`), sum(total_laid_off)
FROM world_layoffs.layoffs_staging2
group by company, year(`date`)
), company_year_rank as
(select *, dense_rank() over(partition by years order by total_laid_off desc) as ranking
from company_year
where years is not null
)
select *
from company_year_rank
where ranking <= 5;












































