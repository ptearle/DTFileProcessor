CREATE TABLE dts_specimen_inventory_v1_0 (
  id                    int(11)      NOT NULL AUTO_INCREMENT,
  study_protocol_id     varchar(50)  DEFAULT NULL,
  site_number           varchar(50)  DEFAULT NULL,
  subject_num           varchar(50)  DEFAULT NULL,
  subject_gender        varchar(50)  DEFAULT NULL,
  subject_date_of_birth date         DEFAULT NULL,
  specimen_collect_date date         DEFAULT NULL,
  specimen_collect_time time         DEFAULT NULL,
  received_datetime     datetime     DEFAULT NULL,
  visit_name            varchar(50)  DEFAULT NULL,
  specimen_barcode      varchar(50)  DEFAULT NULL,
  specimen_identifier   varchar(50)  DEFAULT NULL,
  specimen_type         varchar(50)  DEFAULT NULL,
  specimen_name         varchar(50)  DEFAULT NULL,
  specimen_parent_id    varchar(50)  DEFAULT NULL,
  specimen_ischild      varchar(50)  DEFAULT NULL,
  specimen_condition    varchar(50)  DEFAULT NULL,
  specimen_status       varchar(50)  DEFAULT NULL,
  specimen_comment      varchar(255) DEFAULT NULL,
  shipped_date          datetime     DEFAULT NULL,
  shipped_location      varchar(50)  DEFAULT NULL,
  testing_description   varchar(255) DEFAULT NULL,
  vendor_code           varchar(255) NOT NULL,
  record_status         varchar(50)  NOT NULL DEFAULT 'new',
  record_created        timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id)
)
ENGINE = INNODB
AUTO_INCREMENT = 995
AVG_ROW_LENGTH = 281
CHARACTER SET utf8
COLLATE utf8_unicode_ci;