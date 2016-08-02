CREATE TABLE `cl017-test`.dts_specimen_clinventory_v1_0 (
  id int(11) NOT NULL AUTO_INCREMENT,
  study_protocol_id varchar(50) DEFAULT NULL,
  study_protocol_version varchar(50) DEFAULT NULL,
  site_number varchar(50) DEFAULT NULL,
  site_country varchar(50) DEFAULT NULL,
  subject_number varchar(50) DEFAULT NULL,
  arm varchar(50) DEFAULT NULL,
  subject_DOB date DEFAULT NULL,
  subject_race varchar(50) DEFAULT NULL,
  subject_ethnecity varchar(50) DEFAULT NULL,
  subject_gender varchar(50) DEFAULT NULL,
  subject_enrollmetn_status varchar(50) DEFAULT NULL,
  specimen_identifier varchar(255) DEFAULT NULL,
  specimen_barcode varchar(255) DEFAULT NULL,
  specimen_type varchar(255) DEFAULT NULL,
  specimen_tube_type varchar(255) DEFAULT NULL,
  speciemen_quality varchar(50) DEFAULT NULL,
  specimen_status varchar(50) DEFAULT NULL,
  specimen_collection_date date DEFAULT NULL,
  specimen_comments varchar(255) DEFAULT NULL,
  specimen_collection_time time DEFAULT NULL,
  specimen_condition varchar(255) DEFAULT NULL,
  visit_date date DEFAULT NULL,
  visit_day varchar(50) DEFAULT NULL,
  visit_name varchar(50) DEFAULT NULL,
  visit_week varchar(50) DEFAULT NULL,
  timepoint varchar(50) DEFAULT NULL,
  specimen_volume varchar(255) DEFAULT NULL,
  specimen_volume_unit varchar(255) DEFAULT NULL,
  specimen_mass decimal(3, 2) DEFAULT NULL,
  specimen_mass_unit varchar(50) DEFAULT NULL,
  specimen_parent_id varchar(100) DEFAULT NULL,
  specimen_ischild varchar(50) DEFAULT NULL,
  specimen_child_type varchar(50) DEFAULT NULL,
  date_departed_site datetime DEFAULT NULL,
  received_date date DEFAULT NULL,
  shipped_date date DEFAULT NULL,
  shipped_to_location varchar(50) DEFAULT NULL,
  storage_vendor varchar(50) DEFAULT NULL,
  storage_facility_name varchar(255) DEFAULT NULL,
  storage_location varchar(1024) DEFAULT NULL,
  vendor_code varchar(255) NOT NULL,
  record_status varchar(50) NOT NULL DEFAULT 'new',
  record_created timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id)
)
ENGINE = INNODB
AUTO_INCREMENT = 1
CHARACTER SET utf8
COLLATE utf8_unicode_ci;