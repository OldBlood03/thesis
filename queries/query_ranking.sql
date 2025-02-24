CREATE OR REPLACE FUNCTION char_replace(chars_to_remove text, string text)
RETURNS text AS $$
BEGIN
	FOR i IN 1..LENGTH(chars_to_remove) LOOP
	string := REPLACE(string, SUBSTRING(chars_to_remove, i, 1), '');
	END LOOP;
	RETURN string;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION is_empty(input_value text)
RETURNS boolean AS $$
BEGIN
    RETURN input_value IS NULL 
        OR TRIM(input_value) = ''
        OR input_value::text = 'NaN'
        OR input_value::text = 'undefined'
        OR input_value::text = 'null'
        OR input_value = '+'
        OR input_value !~ '\S'
        OR TRIM(char_replace('{}\"', input_value)) = ''
        OR LOWER(input_value) LIKE '%zbmath %license%';
END;
$$ LANGUAGE plpgsql;

WITH title (numerator) AS (
    SELECT COUNT(*) FROM dblp
    WHERE NOT is_empty(title)
),
doi (numerator) AS (
    SELECT COUNT(*) FROM dblp
    WHERE NOT is_empty(doi)
),
abstract (numerator) AS (SELECT 0),
total (denominator) AS (
    SELECT COUNT(*) FROM dblp
)
SELECT ROUND(title.numerator::numeric/denominator, 2) as title,
ROUND(doi.numerator::numeric/denominator, 2) as doi,
ROUND(abstract.numerator::numeric/denominator, 2) as abstract 
INTO TEMP TABLE temp_dblp
FROM title, doi, abstract, total;

WITH title (numerator) AS (
    SELECT COUNT(*) FROM arxiv
    WHERE NOT is_empty(title)
),
doi (numerator) AS (
    SELECT COUNT(*) FROM arxiv
    WHERE NOT is_empty(doi)
),
abstract (numerator) AS (
	SELECT COUNT(*) FROM arxiv
    	WHERE NOT is_empty(summary)
),
total (denominator) AS (
    SELECT COUNT(*) FROM arxiv
)

SELECT ROUND(title.numerator::numeric/denominator, 2) as title,
ROUND(doi.numerator::numeric/denominator, 2) as doi,
ROUND(abstract.numerator::numeric/denominator, 2) as abstract 
INTO TEMP TABLE temp_arxiv
FROM title, doi, abstract, total;

WITH title (numerator) AS (
    SELECT COUNT(*) FROM zbmath
    WHERE NOT is_empty(title)
),
doi (numerator) AS (
    SELECT COUNT(*) FROM zbmath
    WHERE NOT LOWER(links) LIKE '%doi%'
),
abstract (numerator) AS (
	SELECT COUNT(*) FROM zbmath
    	WHERE NOT is_empty(editorial_contributions)
),
total (denominator) AS (
    SELECT COUNT(*) FROM zbmath
)
SELECT ROUND(title.numerator::numeric/denominator, 2) as title,
ROUND(doi.numerator::numeric/denominator, 2) as doi,
ROUND(abstract.numerator::numeric/denominator, 2) as abstract 
INTO TEMP TABLE temp_zbmath
FROM title, doi, abstract, total;

SELECT *
INTO quality_ratios
FROM 
temp_dblp 
UNION ALL
SELECT * FROM temp_arxiv 
UNION ALL
SELECT * FROM temp_zbmath;

DROP TABLE temp_dblp, temp_arxiv, temp_zbmath;
