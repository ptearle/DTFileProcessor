CREATE TABLE dts_ivrs_v1_0 (
  id                           int(11)      NOT NULL AUTO_INCREMENT,
  study_protocol_id            varchar(50)  DEFAULT NULL,
  site_number                  varchar(50)  DEFAULT NULL,
  subject_number               varchar(50)  DEFAULT NULL,
  randomization_id             varchar(50)  DEFAULT NULL,
  vendor_code                  varchar(255) NOT NULL,
  record_status                varchar(50)  NOT NULL DEFAULT 'new',
  record_created               timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id)
)
ENGINE = INNODB
AUTO_INCREMENT = 1
CHARACTER SET utf8
COLLATE utf8_unicode_ci;