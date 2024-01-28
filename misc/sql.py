Query_for_question_2 = """
DELIMITER //
CREATE PROCEDURE blood_donation_pipeline.churn_analysis_reset()
BEGIN
SET SQL_MODE = '';
DROP TABLE IF EXISTS blood_donation_pipeline.q2;
CREATE TABLE IF NOT EXISTS blood_donation_pipeline.q2 AS
WITH donation_details AS(	
SELECT 
	donor_id, 
	visit_date,
	LEAD(visit_date) OVER (PARTITION BY donor_id ORDER BY visit_date) AS next_visit_date
FROM 
	ds_data_granular
), 

donor_next_visit AS (
SELECT 
	donor_id,
	visit_date,
	next_visit_date,
	CASE 
		WHEN DATEDIFF(next_visit_date, visit_date) > 180 THEN 1
		ELSE 0 
		END AS is_churned
FROM 
	donation_details
),

donor_churn_count AS (
SELECT 
	donor_id,
	SUM(is_churned) AS visits_before_churn
FROM 
	donor_next_visit
GROUP BY 
	donor_id
),

churn_distribution AS (
SELECT
	visits_before_churn,
	COUNT(donor_id) AS num_donors
FROM
	donor_churn_count
GROUP BY
	visits_before_churn
),

total_donors AS (
SELECT
	COUNT(DISTINCT donor_id) AS total_donors
FROM
	donor_churn_count
)

SELECT 
    cd.visits_before_churn,
    cd.num_donors,
    ROUND(cd.num_donors / td.total_donors * 100, 2) AS percentage_of_total_donors
FROM 
    churn_distribution AS cd
CROSS JOIN
    total_donors AS td
ORDER BY 
    cd.visits_before_churn;
SET SQL_MODE = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';
END //
DELIMITER ;
"""