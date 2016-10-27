CREATE TABLE dts_subject_v1_0 (
  id int(11) NOT NULL AUTO_INCREMENT,
  study_protocol_id varchar(50) DEFAULT NULL,
  site_number varchar(50) DEFAULT NULL,
  subject_code varchar(45) DEFAULT NULL,
  subject_external_id varchar(45) DEFAULT NULL,
  randomization_number varchar(50) DEFAULT NULL,
  gender enum ('Male', 'Female', 'Not Known', 'Not Specified') DEFAULT NULL,
  initials varchar(45) DEFAULT NULL,
  enrollment_status enum ('Screening', 'Randomized', 'Screen Failed', 'Discontinued') DEFAULT NULL,
  date_of_birth date DEFAULT NULL,
  address varchar(255) DEFAULT NULL,
  city varchar(255) DEFAULT NULL,
  state varchar(255) DEFAULT NULL,
  region varchar(255) DEFAULT NULL,
  country varchar(255) DEFAULT NULL,
  postcode varchar(45) DEFAULT NULL,
  primary_race varchar(255) DEFAULT NULL,
  secondary_race varchar(255) DEFAULT NULL,
  ethnicity varchar(255) DEFAULT NULL,
  treatment varchar(50) DEFAULT NULL,
  arm varchar(50) DEFAULT NULL,
  ICF_signing_date datetime DEFAULT NULL,
  ICF_withdrawl_date date DEFAULT NULL,
  vendor_code varchar(255) NOT NULL,
  record_status varchar(50) NOT NULL DEFAULT 'new',
  record_created timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id)
)
ENGINE = INNODB
AUTO_INCREMENT = 214
AVG_ROW_LENGTH = 248
CHARACTER SET utf8
COLLATE utf8_unicode_ci;