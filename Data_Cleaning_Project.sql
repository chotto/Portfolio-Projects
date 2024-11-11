-- Data Cleaning Pipeline for the 'layoffs' Table

-- Step 1: Display the original data to understand its contents
SELECT * 
FROM layoffs;

-- Step 2: Define Data Cleaning Steps
-- These steps include:
-- - Removing duplicates
-- - Standardizing values (e.g., trimming whitespace, correcting formats)
-- - Handling null or blank values
-- - Dropping unnecessary columns

-- Step 3: Create a staging table to safely clean data without altering the original table
CREATE TABLE layoffs_staging LIKE layoffs;

-- Step 4: Insert data from the original 'layoffs' table into 'layoffs_staging'
INSERT INTO layoffs_staging
SELECT * 
FROM layoffs;

-- Step 5: Display the staging table to confirm data was copied correctly
SELECT * 
FROM layoffs_staging;

-- Step 6: Identifying Duplicates
-- Use ROW_NUMBER() to assign each duplicate row a number based on partitioned columns
SELECT *,
    ROW_NUMBER() OVER (
        PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`
    ) AS row_num
FROM layoffs_staging;

-- Step 7: Define a CTE to identify duplicates based on more specific fields
-- Create a CTE (Common Table Expression) called 'duplicate_cte' 
WITH duplicate_cte AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, country, stage, funds_raised_millions
        ) AS row_num
    FROM layoffs_staging)

-- Display rows in 'duplicate_cte' where duplicates are found (row_num > 1)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- Step 8: Create a new staging table (layoffs_staging2) to store data without duplicates
CREATE TABLE layoffs_staging2 (
  `company` TEXT,
  `location` TEXT,
  `industry` TEXT,
  `total_laid_off` INT DEFAULT NULL,
  `percentage_laid_off` TEXT,
  `date` TEXT,
  `stage` TEXT,
  `country` TEXT,
  `funds_raised_millions` INT DEFAULT NULL,
  `row_num` INT  -- Add row number column to track duplicates
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Step 9: Populate 'layoffs_staging2' with data, assigning row numbers to detect duplicates
INSERT INTO layoffs_staging2
SELECT *,
    ROW_NUMBER() OVER (
        PARTITION BY company, location, `date`,industry, total_laid_off, percentage_laid_off, country, stage, funds_raised_millions
    ) AS row_num
FROM layoffs_staging;

-- Step 10: Remove all duplicate rows by deleting rows with row_num > 1
DELETE
FROM layoffs_staging2
WHERE row_num > 1;

-- Step 11: Verify that duplicates are removed by selecting from 'layoffs_staging2'
SELECT *
FROM layoffs_staging2;

-- Step 12: Standardize Data in 'layoffs_staging2'

-- Trim whitespace from the 'company' column
UPDATE layoffs_staging2
SET company = TRIM(company);

-- Check unique values in 'industry' to find standardization opportunities
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

-- Standardize 'industry' by grouping all variations of 'Crypto' under one label
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Standardize 'country' by trimming periods or unnecessary suffixes from country names
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Convert 'date' column from text to date format
-- First, check current formats using STR_TO_DATE()
SELECT `date`,
       STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

-- Update the 'date' column to the standardized date format
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Modify the 'date' column to have a DATE data type instead of text
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


-- Step 13: Manage Null and Blank Values

-- Identify rows where 'industry' is blank or NULL
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

-- We've observed AirBnB to have for missing values for industry
SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

-- Join self to check for missing 'industry' values where another row has the same company and location with a populated 'industry'
SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
    ON t1.company = t2.company #company and location must be the same for joined (to be the same industry)
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '') 
AND t2.industry IS NOT NULL; #join with the NOT NULL datapoints

#Classify the blanks as NULLs to group them together
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
    ON t1.company = t2.company
SET t1.industry = t2.industry #replace with the NOT NULL industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;


-- Identify any other rows where 'industry' is blank or NULL
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

-- We've observed Bally Sports to have for missing values for industry
SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';
-- Bally Sports does not have another row we can use to replace the industry

-- Identify rows where both 'total_laid_off' and 'percentage_laid_off' are NULL (not useful for this specific analysis)
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete 
DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Review 
Select *
FROM layoffs_staging2;
-- We need to remove row_num column (which we added earlier)

-- Delete row_num column
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- Review 
Select *
FROM layoffs_staging2;



































