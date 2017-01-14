CREATE TABLE dts_assay_results_v1_1 (
  id int(11) NOT NULL AUTO_INCREMENT,
  study_protocol_id varchar(50) DEFAULT NULL,
  assay_protocol_id varchar(255) DEFAULT NULL,
  assay_protocol_version varchar (255) DEFAULT NULL,
  assay_name varchar(255) DEFAULT NULL,
  assay_code varchar (255) DEFAULT NULL,
  subject_number varchar(50) DEFAULT NULL,
  specimen_collect_date date DEFAULT NULL,
  visit_name varchar(50) DEFAULT NULL,
  lab_barcode varchar(255) DEFAULT NULL,
  anaylsis_barcode varchar(255) DEFAULT NULL,
  result_categorical varchar(45) DEFAULT NULL,
  result_numeric decimal (11, 4) DEFAULT NULL,
  result_type varchar(45) DEFAULT NULL,
  result_units varchar(45) DEFAULT NULL,
  assay_batch_id varchar(255) DEFAULT NULL,
  assay_run_id varchar(255) DEFAULT NULL,
  result_flag varchar(50) DEFAULT NULL,
  result_comment varchar(255) DEFAULT NULL,
  vendor_code varchar(255) NOT NULL,
  record_status varchar(50) NOT NULL DEFAULT 'new',
  record_created timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  INDEX IDX_dts_assay_results_v1_0_lab_barcode (lab_barcode),
  INDEX IDX_dts_assay_results_v1_0_study_protocol_id (study_protocol_id),
  INDEX IDX_dts_assay_results_v1_0_subject_num (subject_num)
)
ENGINE = INNODB
AUTO_INCREMENT = 111963
AVG_ROW_LENGTH = 281
CHARACTER SET utf8
COLLATE utf8_unicode_ci;