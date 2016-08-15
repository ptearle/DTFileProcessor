require 'mysql2'

class Regeneron_site
  def initialize(logger)
    @logger = logger
    @logger.info "#{self.class.name} filer initialized"
  end

  def reader(inbound_file)
    @logger.debug "File name is ->#{inbound_file}<-"
    @logger.info "#{self.class.name} reader start"
    @inbound_lines = CSV.read(inbound_file, headers: true, skip_blanks: true, skip_lines: '\r', encoding:'windows-1256:utf-8')
    @logger.info "#{self.class.name} reader end"
    @inbound_lines.length
  end

  def processor(this_connection)
    @logger.info "#{self.class.name} processor"

    @processing_lines = Array.new
    lines = 0
    num_distinct_lines = 0

    @inbound_lines.each do |specline|
      found = false
      lines += 1

      @processing_lines.each do |distinct_line|

        # see if we actually need the line in the file ... Site Id already seen, then ignore
        if specline[2] == distinct_line[2]
          found = true
          break
        end
      end

      if !found
        @processing_lines << specline
        num_distinct_lines += 1
      end
    end

    @logger.info "#{self.class.name} processor end"
    @processing_lines.length
  end

  def writer(vendor)
    @logger.info "#{self.class.name} writer start"

    if @processing_lines.count == 0
      logger.info ("Nothing to insert :(")
      exit 0
    end

    values_clause = Array.new

    @processing_lines.each do |outline|

      values_clause <<
          " (#{outline[0].insert_value}"                          + # study_protocol_id
              "  #{outline[2].insert_value}"                          + # site_number
              "  #{outline[3].insert_value}"                          + # site_name
              ' NULL,'                                                + # site_address
              ' NULL,'                                                + # site_city
              ' NULL,'                                                + # site_state
              "  #{outline[6].insert_value}"                          + # site_country
              ' NULL,'                                                + # site_postal_code
              ' NULL,'                                                + # site_phone
              ' NULL,'                                                + # site_fax
              ' NULL,'                                                + # site_FPFV
              ' NULL,'                                                + # site_LPLV
              ' NULL,'                                                + # planned_enrollment
              "  #{outline[4].insert_value}"                          + # site_PI
              ' NULL,'                                                + # site_PI_email
              ' NULL,'                                                + # site_coordinator
              ' NULL,'                                                + # site_coordinator_email
              "  '#{vendor}'"                                         + # vendor_code
              ' )'
    end

    @logger.info "#{self.class.name} writer end"
    values_clause
  end
end

class Regeneron_subject
  def initialize(logger)
    @logger = logger
    @logger.info "#{self.class.name} filer Initialized"
  end

  def reader(inbound_file)
    @logger.debug "File name is ->#{inbound_file}<-"
    @logger.info "#{self.class.name} reader start"
    @inbound_lines = CSV.read(inbound_file, headers: true, skip_blanks: true, skip_lines: '\r', encoding:'windows-1256:utf-8')
    @logger.info "#{self.class.class.name} reader end"
    @inbound_lines.length
  end

  def processor(this_connection)
    @logger.info "#{self.class.name} processor start'"
    @processing_lines = @inbound_lines
    @logger.info "#{self.class.name} processor end"
    @processing_lines.length
  end

  def writer(vendor)
    @logger.info 'R668AD1021_site writer start'

    if @processing_lines.count == 0
      logger.info ("Nothing to insert :(")
      exit 0
    end

    values_clause = Array.new

    @processing_lines.each do |outline|

      values_clause <<
          " (#{outline[0].insert_value}"                             + # study_protocol_id
              "  #{outline[2].insert_value}"                         + # site_number
              "  #{outline[1][-3..-1].insert_value}"                 + # subject_code
              "  #{outline[1].insert_value}"                         + # subject_external_id
              ' NULL,'                                               + # gender
              ' NULL,'                                               + # randomization_number
              ' NULL,'                                               + # initials
              ' NULL,'                                               + # enrollment_status
              ' NULL,'                                               + # date_of_birth
              ' NULL,'                                               + # address
              ' NULL,'                                               + # city
              ' NULL,'                                               + # state
              ' NULL,'                                               + # region
              ' NULL,'                                               + # country
              ' NULL,'                                               + # postcode
              ' NULL,'                                               + # primary_race
              ' NULL,'                                               + # secondary_race
              ' NULL,'                                               + # ethnicity
              ' NULL,'                                               + # treatment_arm
              ' NULL,'                                               + # track
              "  '#{vendor}'"                                        + # vendor_code
              " )"
    end

    @logger.info "#{self.class.name} writer end"
    values_clause
  end
end

class Regeneron_RGinv

  SPECIMEN_TYPE = {
      'Urine, no preservative' => 'Urine',
      'Plasma, NaCitrate'      => 'Plasma',
      'Biopsy'                 => 'Biopsy',
      'Hu Whole Blood'         => 'Whole Blood',
      'Hu Plasma (EDTA)'       => 'Plasma',
      'Hu Serum'               => 'Serum',
      'Other'                  => 'Other',
  }.freeze

  VISIT_MAP = {
      :V1       => 'VISIT1',
      :V2       => 'VISIT2',
      :V3       => 'VISIT3',
      :V4       => 'VISIT4',
      :V5       => 'VISIT5',
      :V6       => 'VISIT6',
      :V7       => 'VISIT7',
      :V8       => 'VISIT8',
      :V9       => 'VISIT9',
      :V10      => 'VISIT10',
      :V11      => 'VISIT11',
      :V12      => 'VISIT12',
      :V13      => 'VISIT13',
      :V14      => 'VISIT14',
      :V15      => 'VISIT15',
      :V16      => 'VISIT16',
      :V17      => 'VISIT17',
      :V18      => 'VISIT18',
      :V19      => 'VISIT19',
      :V20      => 'VISIT20',
      :V21      => 'VISIT21',
      :V22      => 'VISIT22',
      :V23      => 'VISIT23',
      :V24      => 'VISIT24',
      :V25      => 'VISIT25',
      :V26      => 'VISIT26',
      :VET  	  => 'ET',
      :VUNSCHED => 'RT',
  }.freeze


  def initialize(logger)
    @logger = logger
    @logger.info "#{self.class.name} filer Initialized"
  end

  def reader(inbound_file)
    @logger.debug "File name is ->#{inbound_file}<-"
    @logger.info "#{self.class.name} reader start"
    @inbound_lines = CSV.read(inbound_file, headers: true, skip_blanks: true, skip_lines: '\r', encoding:'windows-1256:utf-8')
    @logger.info "#{self.class.name} reader end"
    @inbound_lines.length
  end

  def processor(this_connection)
    @logger.info "#{self.class.name} processor start'"
    @processing_lines = @inbound_lines
    @logger.info "#{self.class.name} processor end"
    @processing_lines.length
  end

  def writer(vendor)
    @logger.info "#{self.class.name} writer start"

    if @processing_lines.count == 0
      logger.info ("Nothing to insert :(")
      exit 0
    end

    values_clause = Array.new

    @processing_lines.each do |outline|

      if outline[1].nil? and outline[6].nil?
        outline[1] = 'De-identified'
        outline[6] = 'De-identified'
      end

      values_clause <<
          " (#{outline[0].insert_value}"                             + # study_protocol_id
              "  #{outline[1].insert_value}"                             + # site_number
              "  #{outline[6].insert_value}"                             + # subject_code
              ' NULL,'                                                   + # subject_gender
              ' NULL,'                                                   + # subject_DOB
              "  STR_TO_DATE(#{outline[12].insert_value} '%c/%e/%Y'),"   + # specimen_collect_date
              "  STR_TO_DATE(#{outline[12].insert_value} '%c/%e/%Y %T'),"+ # specimen_collect_time
              "  STR_TO_DATE(#{outline[17].insert_value} '%c/%e/%Y %T'),"+ # specimen_receive_datetime
              "  #{VISIT_MAP[('V' + outline[7]).to_sym].insert_value}"   + # visit_name
              "  #{outline[9].insert_value}"                             + # specimen_barcode
              "  #{outline[4].insert_value}"                             + # specimen_identifier
              "  #{SPECIMEN_TYPE[outline[11].strip].insert_value}"       + # specimen_type
              "  #{outline[10].insert_value}"                            + # specimen_name
              ' NULL,'                                                   + # specimen_parent
              "  'N',"                                                   + # specimen_ischild
              "  #{outline[13].insert_value}"                            + # specimen_condition
              "  'In Inventory',"                                        + # specimen_status
              "  #{outline[28].insert_value}"                            + # specimen_comment
              ' NULL,'                                                   + # shipped_date
              ' NULL,'                                                   + # shipped_location
              ' NULL,'                                                   + # testing_description
              "  '#{vendor}'"                                            + # vendor_code
              " )"
    end

    @logger.info "#{self.class.name} writer end"
    values_clause
  end
end

class Regeneron_LCRP

  SPECIMEN_TYPE = {
      :S0	  => 'UNKNOWN',
      :S13	=> 'Plasma',
      :S16	=> 'Urine',
      :S160	=> 'Plasma',
      :S2	  => 'Serum',
      :S3   => 'Serum',
      :S31	=> 'Whole Blood',
      :S4	  => 'Serum',
      :S46	=> 'Whole Blood',
      :S50  => 'Whole Blood',
      :S725	=> 'DNA',
      :S908	=> 'Whole Blood',
      :S9   => 'Plasma',
      :S533 => 'Biospy',
      :S15  => 'Urine',
  }.freeze

  def initialize(logger)
    @logger = logger
    @logger.info "#{self.class.name} filer Initialized"
  end

  def reader(inbound_file)
    @logger.debug "File name is ->#{inbound_file}<-"
    @logger.info "#{self.class.name} reader start"
    @inbound_lines = CSV.read(inbound_file, col_sep: '|', headers: true, skip_blanks: true, skip_lines: '\r', encoding:'windows-1256:utf-8')
    @logger.info "#{self.class.name} reader end"
    @inbound_lines.length
  end

  def processor(this_connection)
    @logger.info "#{self.class.name} processor start'"

    @processing_lines = Array.new
    lines = 0
    num_distinct_lines = 0

    @inbound_lines.each do |specline|
      found = false
      lines += 1

      if specline[15] == '0' or specline[34].nil?
        next
      end

      @processing_lines << specline
    end

    @logger.info "#{self.class.name} processor end"
    @processing_lines.length
  end

  def writer(vendor)
    @logger.info "#{self.class.name} writer start"

    if @processing_lines.count == 0
      logger.info ("Nothing to insert :(")
      exit 0
    end

    values_clause = Array.new

    @processing_lines.each do |outline|

      specimen_ext     = (outline[15].length == 1) ? "0#{outline[15]}" : outline[15]
      specimen_barcode = "#{outline[12]}-#{specimen_ext}"

      if outline[2].strip == 'SMART'      or
         outline[2].strip == 'DR.SMART'   or
         outline[2].strip == 'Dr.Smart'   or
         outline[2].strip == 'SMRT800202' or
         outline[2].strip == 'SMRT602090'
        site_id     = 'De-Identified'
        subject_id  = 'De-Identified'
      else
        site_id     = outline[2].rjust(6, '0')
        subject_id  = outline[3][-3..-1]
      end

      shipped_date  = (outline[35].nil?) ? ' NULL,' : "  STR_TO_DATE(#{outline[35].insert_value} '%d%b%Y'),"
      specimen_type = (outline[23].nil?) ? ' NULL,' : " #{SPECIMEN_TYPE[('S' + outline[23]).to_sym].insert_value}"

      if outline[19].nil? and outline[20].nil?
        testing_desc  = ' NULL,'
      elsif outline[20].nil?
        testing_desc  = "'#{outline[19].strip}',"
      elsif outline[19].nil?
        testing_desc  = "'#{outline[20].strip}',"
      else
        testing_desc  = "'#{outline[19].strip} / #{outline[20].strip}',"
      end

      @logger.debug("Shipped date ->#{outline[35]}<-")

      values_clause <<
          " (#{outline[1].insert_value}"                                    + # study_protocol_id
              "  #{site_id.insert_value}"                                   + # site_number
              "  #{subject_id.insert_value}"                                + # subject_code
              ' NULL,'                                                      + # subject_gender
              ' NULL,'                                                      + # subject_DOB
              "  STR_TO_DATE(#{outline[6].insert_value} '%d%b%Y'),"         + # specimen_collect_date
              "  STR_TO_DATE(#{outline[7].insert_value} '%H:%i'),"          + # specimen_collect_time
              "  STR_TO_DATE(#{outline[34].insert_value} '%d%b%Y'),"        + # specimen_receive_datetime
              "  #{outline[10].insert_value}"                               + # visit_name
              "  #{specimen_barcode.insert_value}"                          + # specimen_barcode
              ' NULL,'                                                      + # specimen_identifier
              "  #{specimen_type}"                                          + # specimen_type
              "  #{outline[24].insert_value}"                               + # specimen_name
              ' NULL,'                                                      + # specimen_parent
              "  'N',"                                                      + # specimen_ischild
              "  #{outline[26].insert_value}"                               + # specimen_condition
              "  'In Inventory',"                                           + # specimen_status
              "  #{outline[28].insert_value}"                               + # specimen_comment
              "  #{shipped_date}"                                           + # shipped_date
              "  #{outline[40].insert_value}"                               + # shipped_location
              "  #{testing_desc}"                                           + # testing_description
              "  '#{vendor}'"                                               + # vendor_code
              " )"
    end

    @logger.info "#{self.class.name} writer end"
    values_clause
  end
end
