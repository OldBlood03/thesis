BEGIN TRANSACTION;

SELECT DISTINCT ON ("DOI") * INTO snowball_tables_combined FROM (
(SELECT "DOI", "volume-title", "author", "year" FROM arxiv_snowball_depth_1)
UNION ALL
(SELECT "DOI", "volume-title", "author", "year" FROM dblp_snowball_depth_1)
UNION ALL
(SELECT "DOI", "volume-title", "author", "year" FROM zbmath_snowball_depth_1)
) WHERE NOT is_empty("DOI");


DO $$

DECLARE
		rows INT;
		i INT;
		batch_size INT;
		num_pages INT := 20;
BEGIN
		SELECT COUNT(*) INTO rows FROM snowball_tables_combined;
		SELECT CEIL(rows/num_pages::numeric) INTO batch_size;

		FOR i IN 0..rows BY batch_size LOOP
			       EXECUTE format('CREATE TABLE snowball_page_%s AS 
                        SELECT * FROM snowball_tables_combined 
                        LIMIT %s OFFSET %s', 
                        (i/batch_size)::INT, batch_size, i);
		END LOOP;
END; $$;

COMMIT;
