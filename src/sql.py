PREP_DATABASE = """
CREATE DATABASE IF NOT EXISTS blood_donation_pipeline
DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci 
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
  KEY birth_date_idx (birth_date)
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
