require 'mysql2'

class CA018003_QINV
  VISIT_MAP = {
      :PE0001     => 'SCR ARCHIVAL TISSUE',
      :PE0002     => 'SCR FRESH IHC QLAB A',
      :PE0003     => 'SCR FRESH TO LABCORP',
      :PE0004     => 'SCR FRESH IHC QLAB B',
      :PE0005     => 'SCR FRESH RNA QLAB A',
      :PE0006     => 'SCR FRESH RNA QLAB B',
      :UNSCHED_1  => 'C1 D1',
      :UNSCHED_10 => 'C1 D15 CYTOMETRY',
      :UNSCHED_12 => 'C3 D1',
      :UNSCHED_16 => 'EOT',
      :UNSCHED_18 => 'PROG/ UNSCH PBMC',
      :UNSCHED_2  => 'C1 D1 CYTOMETRY',
      :UNSCHED_20 => 'FU3 D100',
      :UNSCHED_21 => 'FU1 D30',
      :UNSCHED_22 => 'PROG/ UNSCH CYTOMETRY',
      :UNSCHED_23 => 'FU2 D60',
      :UNSCHED_24 => 'C5 D1',
      :UNSCHED_25 => 'C1 D28 TISSUE IHC A',
      :UNSCHED_26 => 'C1 D28 TISSUE IHC B',
      :UNSCHED_27 => 'C1 D28 TISSUE IHC C',
      :UNSCHED_28 => 'C1 D28 TISSUE RNA A',
      :UNSCHED_29 => 'C1 D28 TISSUE RNA B',
      :UNSCHED_3  => 'C1 D1 PBMC',
      :UNSCHED_35 => 'PROG/UNSCH IHC A',
      :UNSCHED_36 => 'PROG/ UNSCH IHC B',
      :UNSCHED_37 => 'PROG/ UNSCH IHC C',
      :UNSCHED_38 => 'PROG/ UNSCH RNA A',
      :UNSCHED_39 => 'PROG/ UNSCH RNA B',
      :UNSCHED_4	=> 'C1 D1 EOI PK',
      :UNSCHED_40 => 'PROG/ UNSCH',
      :UNSCHED_41 => 'TRACK 1 RUN-IN EOI PK',
      :UNSCHED_42 => 'TRACK 1 RUN-IN D1',
      :UNSCHED_43 => 'TRACK 1 RUN-IN D1 CYTO',
      :UNSCHED_44 => 'TRACK 1 RUN-IN D15',
      :UNSCHED_45 => 'TRACK 1 RUN-IN D15 CYTO',
      :UNSCHED_46 => 'TRACK 1 RUN-IN D15 PBMC',
      :UNSCHED_47 => 'TRACK 1 RUN-IN D1 PBMC',
      :UNSCHED_5  => 'C1 D28',
      :UNSCHED_6  => 'C1 D15 PBMC',
      :UNSCHED_7  => 'C1 D28 CYTOMETRY',
      :UNSCHED_8  => 'C1 D15',
      :UNSCHED_9  => 'C1 D28 PBMC'
  }.freeze

  SPECIMEN_TYPE = {
      :BLOCK                      => 'Tissue',
      :ADADRUG1                   => 'Serum',
      :ADADRUG2                   => 'Serum',
      :FRESHTISSUEIHCTOQ2         => 'Tissue',
      :FRESHTISSUERNA             => 'Tissue',
      :PATHOLOGYREPORTFRESHTISSUE => 'Pathology Report',
      :PKDRUG1                    => 'Serum',
      :PKDRUG2                    => 'PK DRUG 2',
      :SERUMFACTORSPRIMARY        => 'Serum',
      :SERUMFACTORSSECONDARY      => 'Serum',
  }.freeze

  SPECIMEN_STATUS = {
      :AtLab     => 'In Inventory',
      :Shipped   => 'In Transit',
  }.freeze

  def initialize(logger)
    @logger = logger
    @logger.debug "#{self.class.name} filer initialized"
  end

  def reader(inbound_file)
    @logger.info "#{self.class.name} reader start"
    @inbound_lines = CSV.read(inbound_file, headers: true, skip_blanks: true, skip_lines: '\r', encoding:'windows-1256:utf-8')
    @logger.info "#{self.class.name} reader end"
    @inbound_lines.length
  end

  def processor(this_connection)
    @logger.info "#{self.class.name} processor start"
    @processing_lines = @inbound_lines
    @logger.info "#{self.class.name} processor end"
    @processing_lines.length
  end

  def writer(vendor)
    @logger.info "#{self.class.name} writer start"

    values_clause = Array.new

    @processing_lines.each do |outline|

      visit_name         = VISIT_MAP[outline[22].to_sym]
      specimen_barcode   = outline[11].split('-')[0]+outline[11].split('-')[1].to_s.rjust(2, '0')

      subject_gender     = (outline[9] == 'F') ? 'Female' : 'Male'
      specimen_type      = (outline[13].nil?)  ? 'NULL'   : "'#{SPECIMEN_TYPE[outline[13].gsub(/[^a-zA-Z0-9]/, '').to_sym]}'"
      specimen_parent    = (outline[30].nil?)  ? 'NULL'   : "'#{outline[30]}'"
      specimen_ischild   = (outline[30].nil?)  ? "'N'"    : "'Y'"
      specimen_shipdate  = (outline[35].nil?)  ? 'NULL'   : "STR_TO_DATE('#{outline[35].strip}', '%Y-%m-%d')"
      specimen_condition = (outline[20].nil?)  ? 'NULL'   : "'#{outline[20]}'"
      specimen_status    = (outline[16].nil?)  ? 'NULL'   : "'#{SPECIMEN_STATUS[outline[16].gsub(/[^a-zA-Z0-9]/, '').to_sym]}'"

      values_clause <<
          " ('#{outline[0]}',"                                        + # study_protocol_id
              "  '#{outline[2]}',"                                        + # site_number
              "  '#{outline[4]}',"                                        + # subject_number
              "  '#{subject_gender}',"                                    + # subject_gender
              "  STR_TO_DATE('#{outline[6].strip}',  '%Y-%m-%d'),"        + # subject_DOB
              "  STR_TO_DATE('#{outline[17].strip}', '%Y-%m-%d'),"        + # specimen_collect_date
              "  STR_TO_DATE('#{outline[19].strip}', '%k:%i'),"           + # specimen_collect_time
              "  STR_TO_DATE('#{outline[34].strip}', '%Y-%m-%d'),"        + # specimen_receive_datetime
              ' NULL,'                                                    + # treatment
              ' NULL,'                                                    + # arm
              "  '#{visit_name}',"                                        + # visit_name
              "  '#{specimen_barcode}',"                                  + # specimen_barcode
              "  '#{outline[11].strip}',"                                 + # specimen_identifier
              "   #{specimen_type},"                                      + # specimen_type
              "   #{outline[13].insert_value}"                            + # specimen_name
              "   #{specimen_parent},"                                    + # specimen_parent
              "   #{specimen_ischild},"                                   + # specimen_ischild
              "   #{specimen_condition},"                                 + # specimen_condition
              "   #{specimen_status},"                                    + # specimen_status
              '  NULL,'                                                   + # specimen_comment
              "   #{specimen_shipdate},"                                  + # specimen_shipdate
              '  NULL,'                                                   + # shipped_location
              '  NULL,'                                                   + # testing_description
              "  '#{vendor}'"                                             + # vendor_code
              ')'
    end

    @logger.info "#{self.class.name} writer end"
    values_clause
  end
end