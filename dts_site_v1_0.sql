CREATE TABLE dts_site_v1_0 (
  id int(11) NOT NULL AUTO_INCREMENT,
  study_protocol_id varchar(50) DEFAULT NULL,
  site_number varchar(50) DEFAULT NULL,
  site_name varchar(255) DEFAULT NULL,
  site_address varchar(255) DEFAULT NULL,
  site_city varchar(50) DEFAULT NULL,
  site_state varchar(50) DEFAULT NULL,
  site_country varchar(50) DEFAULT NULL,
  site_postal_code varchar(50) DEFAULT NULL,
  site_phone varchar(50) DEFAULT NULL,
  site_site_fax varchar(50) DEFAULT NULL,
  site_FPFV date DEFAULT NULL,
  site_LPLV date DEFAULT NULL,
  planned_enrollment int(11) DEFAULT NULL,
  site_PI varchar(50) DEFAULT NULL,
  site_PI_email varchar(50) DEFAULT NULL,
  site_coordinator varchar(50) DEFAULT NULL,
  site_coordinator_email varchar(50) DEFAULT NULL,
  vendor_code varchar(255) NOT NULL,
  record_status varchar(50) NOT NULL DEFAULT 'new',
  record_created timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id)
)
ENGINE = INNODB
AUTO_INCREMENT = 1
CHARACTER SET utf8
COLLATE utf8_unicode_ci;