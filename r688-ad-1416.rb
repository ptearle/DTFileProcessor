require 'mysql2'

class R688_AD_1416_site
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
              "'Activated',"                                          + # site_status
              "  '#{vendor}'"                                         + # vendor_code
              ' )'
    end

    @logger.info "#{self.class.name} writer end"
    values_clause
  end
end

class R688_AD_1416_subject
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
          "(#{outline[0].insert_value}"                         + # study_protocol_id
              " #{outline[2].insert_value}"                         + # site_number
              " #{outline[1][-3..-1].insert_value}"                 + # subject_code
              " #{outline[1].insert_value}"                         + # subject_external_id
              ' NULL,'                                              + # randomization_number
              ' NULL,'                                              + # gender
              ' NULL,'                                              + # initials
              ' NULL,'                                              + # enrollment_status
              ' NULL,'                                              + # date_of_birth
              ' NULL,'                                              + # address
              ' NULL,'                                              + # city
              ' NULL,'                                              + # state
              ' NULL,'                                              + # region
              ' NULL,'                                              + # country
              ' NULL,'                                              + # postcode
              ' NULL,'                                              + # primary_race
              ' NULL,'                                              + # secondary_race
              ' NULL,'                                              + # ethnicity
              ' NULL,'                                              + # treatment_arm
              ' NULL,'                                              + # track
              " '#{vendor}'"                                        + # vendor_code
              ")"
    end

    @logger.info "#{self.class.name} writer end"
    values_clause
  end
end

class R688_AD_1416_RGRNinv

  SPECIMEN_TYPE = {
      'Hu Whole Blood'         => 'Plasma',
      'Hu Plasma (EDTA)'       => 'Plasma',
      'Hu Serum'               => 'Serum',
  }.freeze

  VISIT_MAP = {
      :V1          => 'Visit 1',
      :V2          => 'Visit 2 - Baseline',
#      :V3          => 'Visit 3',
      :V4          => 'Visit 4',
#      :V5          => 'Visit 5',
      :V6          => 'Visit 6',
#      :V7          => 'Visit 7',
#      :V8          => 'Visit 8',
#      :V9          => 'Visit 9',
      :V10         => 'Visit 10',
#      :V11         => 'Visit 11',
      :V14         => 'Visit 14',
      :V18         => 'Visit 18',
      :V19         => 'Visit 19',
      :V20         => 'Visit 20',
      :V21         => 'Visit 21 - End of Study',
      :VET     	   => 'Early Termination',
      :VUNSCHED    => 'Unscheduled',
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

      values_clause <<
          "(#{outline[0].insert_value}"                                  + # study_protocol_id
              " #{outline[2].insert_value}"                              + # site_number
              " #{outline[6].insert_value}"                              + # subject_number
              ' NULL,'                                                   + # subject_gender
              ' NULL,'                                                   + # subject_DOB
              " STR_TO_DATE(#{outline[12].insert_value} '%c/%e/%Y'),"    + # specimen_collect_date
              " STR_TO_DATE(#{outline[12].insert_value} '%c/%e/%Y %T')," + # specimen_collect_time
              " STR_TO_DATE(#{outline[17].insert_value} '%c/%e/%Y %T')," + # specimen_receive_datetime
              " #{VISIT_MAP[('V' + outline[7]).to_sym].insert_value}"    + # visit_name
              " #{outline[9].insert_value}"                              + # specimen_barcode
              " #{outline[4].insert_value}"                              + # specimen_identifier
              " #{SPECIMEN_TYPE[outline[11].strip].insert_value}"        + # specimen_type
              " #{outline[10].insert_value}"                             + # specimen_name
              ' NULL,'                                                   + # specimen_parent
              " 'N',"                                                    + # specimen_ischild
              " #{outline[13].insert_value}"                             + # specimen_condition
              " 'In Inventory',"                                         + # specimen_status
              ' NULL,'                                                   + # specimen_comment
              ' NULL,'                                                   + # shipped_date
              ' NULL,'                                                   + # shipped_location
              ' NULL,'                                                   + # testing_description
              " '#{vendor}'"                                             + # vendor_code
              ")"
    end

    @logger.info "#{self.class.name} writer end"
    values_clause
  end
end

class R688_AD_1416_PPDLInv

  SPECIMEN_TYPE = {
      :S0	  => 'UNKNOWN',
      :S2	  => 'Serum',
      :S3   => 'Serum',
      :S4	  => 'Serum',
      :S9   => 'Plasma',
      :S12	=> 'Plasma',
      :S13	=> 'Plasma',
      :S15  => 'Urine',
      :S16	=> 'Urine',
      :S24	=> 'Whole Blood',
      :S31	=> 'Whole Blood',
      :S46	=> 'Whole Blood',
      :S50  => 'Whole Blood',
      :S160	=> 'Plasma',
      :S269 => 'Plasma',
      :S270 => 'Plasma',
      :S533 => 'Biospy',
      :S353 => 'Whole Blood',
      :S370 => 'RNA',
      :S725	=> 'DNA',
      :S908	=> 'Whole Blood',
  }.freeze

  Early Termination
  Genomics DNA
  Genomics RNA
  Screening
  TB Screen
  Unscheduled 1
  Unscheduled 10
  Unscheduled 12
  Unscheduled 13
  Unscheduled 14
  Unscheduled 15
  Unscheduled 2
  Unscheduled 3
  Unscheduled 4
  Unscheduled 5
  Unscheduled 6
  UV
  Visit 10 Week 8
  Visit 14 Week 12
  Visit 18 Week 16
  Visit 19 Week 20
  Visit 2 Baseline
  Visit 20 Week 24
  Visit 21 Week 28
  Visit 4 Week 2
  Visit 6 Week 4


  VISIT_MAP = {
      :V1   => 'Visit 1',
      :V2   => 'Visit 2 - Baseline',
      :V3   => 'Visit 3',
      :V4   => 'Visit 4',
      :V5   => 'Visit 5',
      :V6   => 'Visit 6',
      :V7   => 'Visit 7',
      :V8   => 'Visit 8 - End of Treatment',
      :V9   => 'Visit 9',
      :V10  => 'Visit 10',
      :V11  => 'Visit 11',
      :EOS  => 'EOS/ET Visit 12 - End of Study',
      :ET   => 'Early Termination',
      :RT   => 'Unscheduled',
  }.freeze

  SPECIMEN_STATUS = {
  Destroyed
  Discarded
  In Lab
  In Storage
  Lab Discard
  Pulled For Lab
  Received
  Residual Storage
  Routed
  Shipped Clinical Site
  Shipped Missing Sample
  Shipped Regeneron Pharmaceuticals, Inc - Tarrytown
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

      if outline[2].strip == '999'
        site_id     = 'De-Identified'
        subject_id  = 'De-Identified'
      else
        site_id     = outline[2].rjust(6, '0')
        subject_id  = outline[3][-3..-1]
      end

      shipped_date  = (outline[35].nil?) ? ' NULL,' : "  STR_TO_DATE(#{outline[35].insert_value} '%d%b%Y'),"
      specimen_type = (outline[23].nil?) ? ' NULL,' : " #{SPECIMEN_TYPE[('S' + outline[23]).to_sym].insert_value}"

      if outline[18].nil? and outline[19].nil?
        testing_desc  = ' NULL,'
      elsif outline[19].nil?
        testing_desc  = "'#{outline[18].strip}',"
      elsif outline[18].nil?
        testing_desc  = "'#{outline[19].strip}',"
      else
        testing_desc  = "'#{outline[18].strip} / #{outline[19].strip}',"
      end

      unless outline[10] == 'T'
        visit_name = " #{VISIT_MAP[outline[10].to_sym]}"
      else
        case outline[8]
          when 'DNA ISOLATION'
            visit_name = 'DNA Isolation Visit'
          when 'COLCHICINE'
            visit_name = 'Colchicine Rescue Visit'
          when 'TITRATION VISIT'
            visit_name = 'Allopurinol Titration Visit'
          else
        end
      end

      values_clause <<
          " (#{outline[1].insert_value}"                                    + # study_protocol_id
              "  #{site_id.insert_value}"                                   + # site_number
              "  #{subject_id.insert_value}"                                + # subject_code
              ' NULL,'                                                      + # subject_gender
              ' NULL,'                                                      + # subject_DOB
              "  STR_TO_DATE(#{outline[6].insert_value} '%d%b%Y'),"         + # specimen_collect_date
              "  STR_TO_DATE(#{outline[7].insert_value} '%H:%i'),"          + # specimen_collect_time
              "  STR_TO_DATE(#{outline[34].insert_value} '%d%b%Y'),"        + # specimen_receive_datetime
              "  #{visit_name.insert_value}"                                + # visit_name
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
