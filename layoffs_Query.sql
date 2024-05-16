# Data Cleaning

SELECT *
FROM layoffs;

-- 1. Ramove Duplicate 
-- 2. Standerdize the data 
-- 3. Null Values OR blank Values
-- 4. Remove any columns

CREATE TABLE layoffs_cleaning
LIKE layoffs;

INSERT layoffs_cleaning
SELECT * 
FROM layoffs;

SELECT *
FROM layoffs_cleaning;

CREATE TABLE layoffs_staging AS SELECT * FROM layoffs;

SELECT * , 
row_number() OVER(partition by
company,location , industry , total_laid_off, percentage_laid_off,'date', stage , country , funds_raised_millions) AS row_num
FROM layoffs_staging;


WITH dublicate_cte AS (
SELECT * , 
row_number() OVER(partition by
company,location , industry , total_laid_off, percentage_laid_off,'date', stage , country , funds_raised_millions) AS row_num
FROM layoffs_staging)
SELECT * 
FROM dublicate_cte
WHERE row_num > 1;

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
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;



INSERT layoffs_staging2
SELECT * , 
row_number() OVER(partition by
company,location , industry , total_laid_off, percentage_laid_off,'date', stage , country , funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT * 
FROM layoffs_staging2
WHERE company='cazoo';

DELETE 
FROM layoffs_staging2
WHERE row_num>1;

DELETE 
FROM layoffs_staging2
WHERE row_num=2;

SET sql_safe_updates=0;

DELETE 
FROM layoffs_staging2
WHERE row_num=2;

SELECT * 
FROM layoffs_staging2;

-- Standerdizing data
UPDATE layoffs_staging2
SET company= TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

select * 
FROM layoffs_staging2
WHERE industry LIKE 'crypto%';

UPDATE layoffs_staging2
SET industry ='crypto'
WHERE industry LIKE 'crypto%';


SELECT DISTINCT industry
FROM layoffs_staging2;

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

SELECT country
FROM layoffs_staging2
WHERE country LIKE 'United states%';

UPDATE layoffs_staging2
SET country ='United States'
WHERE country LIKE 'United states%';

 

SELECT * 
FROM layoffs_staging2;

SELECT `date`,
STR_TO_DATE(`date` , '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date`=STR_TO_DATE(`date` , '%m/%d/%Y');

SELECT * 
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


-- Null 
SELECT * 
FROM layoffs_staging2;


SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

SELECT * 
FROM layoffs_staging2
WHERE industry IS NULL;

UPDATE layoffs_staging2 
SET industry = NUll 
WHERE industry ='';

SELECT * 
FROM layoffs_staging2
WHERE company ='Airbnb';

SELECT t1.industry ,t2.industry 
FROM layoffs_staging2 t1 
JOIN layoffs_staging2 t2 
ON t1.company=t2.company
AND t1.location=t2.location
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2 
ON t1.company=t2.company
SET t1.industry =t2.industry 
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;


SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;


DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

SELECT * 
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;


-- Exploratoty Data Analysis

SELECT * 
FROM layoffs_staging2;

SELECT max(total_laid_off) , max(percentage_laid_off)
FROM layoffs_staging2;

SELECT * 
FROM layoffs_staging2
WHERE percentage_laid_off= 1
ORDER BY funds_raised_millions DESC;


SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY 1
ORDER BY 2 DESC;


SELECT MIN(`date`), MAX(`date`) 
FROM layoffs_staging2;


SELECT country , SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY 1
ORDER BY 2 DESC;

SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY 1
ORDER BY 2 DESC;


SELECT `date`, SUM(total_laid_off) 
FROM layoffs_staging2
GROUP BY `date`
ORDER BY 1 DESC;


SELECT YEAR(`date`), SUM(total_laid_off) 
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;



SELECT MONTH(`date`), SUM(total_laid_off) 
FROM layoffs_staging2
GROUP BY MONTH(`date`)
ORDER BY 1 DESC;


SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY 1
ORDER BY 2 DESC;

SELECT company,industry, country, `date`, SUM(total_laid_off) OVER (PARTITION BY `date` ORDER BY `date` DESC) 
FROM layoffs_staging2
ORDER BY 5 DESC;

SELECT company,industry, country, YEAR(`date`),
 SUM(total_laid_off) OVER (PARTITION BY YEAR(`date`) ORDER BY YEAR(`date`)) 
FROM layoffs_staging2
ORDER BY 4 DESC;


-- TWO CODES FOR the same result
SELECT SUBSTRING(`date`,6,2) AS `MONTH` , SUM(total_laid_off) 
FROM layoffs_staging2
GROUP BY 1
ORDER BY 1 DESC;

SELECT MONTH(`date`), SUM(total_laid_off) 
FROM layoffs_staging2
GROUP BY MONTH(`date`)
ORDER BY 1 DESC;

------------
SELECT SUBSTRING(`date`,1,7) AS `MONTH` , SUM(total_laid_off) 
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY 1
ORDER BY 1 DESC;

WITH Rolling_total AS 
(SELECT SUBSTRING(`date`,1,7) AS `MONTH` , SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY 1
ORDER BY 1 DESC)
SELECT `MONTH` ,total_off, SUM(total_off) OVER(ORDER BY `MONTH`) AS total
FROM Rolling_total;

WITH company_year (company , years , total_laid_off) AS (
SELECT company, YEAR(`date`), SUM(total_laid_off) 
FROM layoffs_staging2
GROUP BY 1,2),
company_year_ranking AS 
(SELECT * ,
 DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM company_year
WHERE years IS NOT NULL)
SELECT * 
FROM  company_year_ranking 
WHERE Ranking <= 5;






