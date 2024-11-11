-- Exploratory Data Analysis

Select *
FROM layoffs_staging2;

Select Max(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;

Select *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

Select company, sum(total_laid_off)
FROM layoffs_staging2
Group By company 
Order By 2 desc;

Select min(`date`), max(`date`)
FROM layoffs_staging2;

Select industry, sum(total_laid_off)
FROM layoffs_staging2
Group By industry
Order By 2 desc;

Select COUNTRY, sum(total_laid_off)
FROM layoffs_staging2
Group By cOUNTRY
Order By 2 desc;

Select year (`date`), sum(total_laid_off)
FROM layoffs_staging2
Group By YEAR(`date`)
Order By 1 desc; #MOST RECENT DATE FIRST

Select STAGE, sum(total_laid_off)
FROM layoffs_staging2
Group By STAGE		
Order By 2 desc; 


-- Rolling total of layoffs per month, going year by year

SELECT SUBSTRING(`date`,1,7) AS `MONTH`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;

-- USE CTE to Create Monthly Totals Table
WITH ROLLING_TOTAL AS
(SELECT SUBSTRING(`date`,1,7) AS `MONTH`, SUM(total_laid_off) AS TOTAL_LAYOFFS
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC)

-- Reference CTE, display 3 columns 1) Month, 2) Direct CTE, 3) Rolling SUM from Direct CTE
SELECT `MONTH`, TOTAL_LAYOFFS, SUM(TOTAL_LAYOFFS) OVER(ORDER BY `MONTH`) AS ROLLING_TOTAL_LAYOFFS
FROM ROLLING_TOTAL
;


Select company, sum(total_laid_off)
FROM layoffs_staging2
Group By company 
Order By 2 desc;


Select company, YEAR(`date`), sum(total_laid_off)
FROM layoffs_staging2
Group By company, YEAR(`date`)
Order By 3 desc; #order by column 3, descending



-- Create 2 CTEs, then display based on ranking CTE
WITH 
-- Company Info
Company_Year (company, years, total_laid_off) AS #this is for column naming
(
Select company, YEAR(`date`), sum(total_laid_off)
FROM layoffs_staging2
Group By company, YEAR(`date`)
), 

-- Ranking the top company every year (top 5) using partitioning
Company_Year_Rank AS
(Select *, 
dense_rank() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Layoff_Ranking
From Company_Year
Where years is not Null
)

-- Only show top 5 rankings
Select * 
From Company_Year_Rank
WHERE Layoff_Ranking <= 5
;








