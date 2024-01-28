PREP_DATABASE = """
CREATE DATABASE IF NOT EXISTS blood_donation_pipeline
DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci 
"""
PREP_Q1_PROCEDURE = """
DELIMITER //
CREATE PROCEDURE IF NOT EXISTS blood_donation_pipeline.reset_q1()
BEGIN
SET SQL_MODE = '';
DROP TABLE IF EXISTS blood_donation_pipeline.q1;
CREATE TABLE IF NOT EXISTS blood_donation_pipeline.q1 AS
WITH result AS(
SELECT
	DATE_FORMAT(date, '%Y-%m') AS year_months,
    state,
    MIN(date) AS exact_date,
    SUM(daily) AS total_donations,
    SUM(donations_new) AS total_new_donations,
    SUM(donations_new) / SUM(daily) * 100 AS pct_tnd_from_total,
    SUM(donations_regular) AS total_regular_donations,
    SUM(donations_regular) / SUM(daily) * 100 AS pct_trd_from_total
FROM
	donations_state
GROUP BY
	1, 2),

calc AS(
SELECT
	year_months,
    exact_date,
    state,
    total_donations,
    total_new_donations,
    ROUND((total_new_donations - LAG(total_new_donations) OVER (PARTITION BY state ORDER BY year_months)) / LAG(total_new_donations) OVER (PARTITION BY state ORDER BY year_months), 2) * 100 AS pct_change_tnd,
    pct_tnd_from_total,
    total_regular_donations,
    ROUND((total_regular_donations - LAG(total_regular_donations) OVER (PARTITION BY state ORDER BY year_months)) / LAG(total_regular_donations) OVER (PARTITION BY state ORDER BY year_months), 2) * 100 AS pct_change_trd,
    pct_trd_from_total
FROM
	result)
    
SELECT
	*,
    ROUND(AVG(pct_change_tnd) OVER(PARTITION BY state), 2) AS overall_pct_tnd,
    ROUND(AVG(pct_tnd_from_total) OVER(PARTITION BY state), 2) AS overall_pct_tnd_from_total,
    ROUND(AVG(pct_change_trd) OVER(PARTITION BY state), 2) AS overall_pct_trd,
    ROUND(AVG(pct_trd_from_total) OVER(PARTITION BY state), 2) AS overall_pct_trd_from_total
FROM	
	calc;
SET SQL_MODE = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';
END //
DELIMITER ;
"""
PREP_DONATIONS_FACILITY = """
CREATE TABLE IF NOT EXISTS `donations_facility` (
  `date` date,
  `hospital` VARCHAR(255),
  `daily` int DEFAULT NULL,
  `blood_a` int DEFAULT NULL,
  `blood_b` int DEFAULT NULL,
  `blood_o` int DEFAULT NULL,
  `blood_ab` int DEFAULT NULL,
  `location_centre` int DEFAULT NULL,
  `location_mobile` int DEFAULT NULL,
  `type_wholeblood` int DEFAULT NULL,
  `type_apheresis_platelet` int DEFAULT NULL,
  `type_apheresis_plasma` int DEFAULT NULL,
  `type_other` int DEFAULT NULL,
  `social_civilian` int DEFAULT NULL,
  `social_student` int DEFAULT NULL,
  `social_policearmy` int DEFAULT NULL,
  `donations_new` int DEFAULT NULL,
  `donations_regular` int DEFAULT NULL,
  `donations_irregular` int DEFAULT NULL,
  KEY date_idx (date),
  KEY hospital_idx (hospital)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""
PREP_DONATIONS_STATE = """
CREATE TABLE IF NOT EXISTS `donations_state` (
  `date` date,
  `state` VARCHAR(255),
  `daily` int DEFAULT NULL,
  `blood_a` int DEFAULT NULL,
  `blood_b` int DEFAULT NULL,
  `blood_o` int DEFAULT NULL,
  `blood_ab` int DEFAULT NULL,
  `location_centre` int DEFAULT NULL,
  `location_mobile` int DEFAULT NULL,
  `type_wholeblood` int DEFAULT NULL,
  `type_apheresis_platelet` int DEFAULT NULL,
  `type_apheresis_plasma` int DEFAULT NULL,
  `type_other` int DEFAULT NULL,
  `social_civilian` int DEFAULT NULL,
  `social_student` int DEFAULT NULL,
  `social_policearmy` int DEFAULT NULL,
  `donations_new` int DEFAULT NULL,
  `donations_regular` int DEFAULT NULL,
  `donations_irregular` int DEFAULT NULL,
  KEY date_idx (date),
  KEY state_idx (state)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""
PREP_DS_DATA_GRANULAR = """
CREATE TABLE IF NOT EXISTS `ds_data_granular` (
  `donor_id` VARCHAR(255),
  `visit_date` date,
  `birth_date` int DEFAULT NULL,
  KEY visit_date_idx (visit_date),
  KEY birth_date_idx (birth_date),
  KEY donor_id_idx (donor_id),
  KEY covering_idx (visit_date, donor_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""
PREP_NEWDONORS_FACILITY = """
CREATE TABLE IF NOT EXISTS `newdonors_facility` (
  `date` date,
  `hospital` VARCHAR(255),
  `17-24` int DEFAULT NULL,
  `25-29` int DEFAULT NULL,
  `30-34` int DEFAULT NULL,
  `35-39` int DEFAULT NULL,
  `40-44` int DEFAULT NULL,
  `45-49` int DEFAULT NULL,
  `50-54` int DEFAULT NULL,
  `55-59` int DEFAULT NULL,
  `60-64` int DEFAULT NULL,
  `other` int DEFAULT NULL,
  `total` int DEFAULT NULL,
  KEY date_idx (date),
  KEY hospital_idx (hospital)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""
PREP_NEWDONORS_STATE = """
CREATE TABLE IF NOT EXISTS `newdonors_state` (
  `date` date,
  `state` VARCHAR(255),
  `17-24` int DEFAULT NULL,
  `25-29` int DEFAULT NULL,
  `30-34` int DEFAULT NULL,
  `35-39` int DEFAULT NULL,
  `40-44` int DEFAULT NULL,
  `45-49` int DEFAULT NULL,
  `50-54` int DEFAULT NULL,
  `55-59` int DEFAULT NULL,
  `60-64` int DEFAULT NULL,
  `other` int DEFAULT NULL,
  `total` int DEFAULT NULL,
  KEY date_idx (date),
  KEY state_idx (state)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""
QUERY_DATE = """
SELECT MAX(date) FROM blood_donation_pipeline.donations_facility;
"""
QUESTION_1_PROCEDURE = """
CALL blood_donation_pipeline.reset_q1();
"""
QUESTION_2_PROCEDURE = """
CALL blood_donation_pipeline.churn_analysis_reset();
"""