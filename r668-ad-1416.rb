require 'mysql2'

class R668_AD_1416_site
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

class R668_AD_1416_subject
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

class R668_AD_1416_RGRNInv

  SPECIMEN_TYPE = {
      'Hu Whole Blood'         => 'Whole Blood',
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
              " #{outline[1].insert_value}"                              + # site_number
              " #{outline[6].insert_value}"                              + # subject_number
              ' NULL,'                                                   + # subject_gender
              ' NULL,'                                                   + # subject_DOB
              " STR_TO_DATE(#{outline[12].insert_value} '%c/%e/%Y'),"    + # specimen_collect_date
              " STR_TO_DATE(#{outline[12].insert_value} '%c/%e/%Y %T')," + # specimen_collect_time
              " STR_TO_DATE(#{outline[17].insert_value} '%c/%e/%Y %T')," + # specimen_receive_datetime
              " #{VISIT_MAP[('V' + outline[7]).to_sym].insert_value}"    + # visit_name
              " #{outline[3].insert_value}"                              + # specimen_barcode
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

class R668_AD_1416_PPDLInv

  SPECIMEN_TYPE = {
      'Whole Blood (EDTA)'  => 'Whole Blood',
      'Plasma (EDTA)'       => 'Plasma',
      'Serum'               => 'Serum',
  }.freeze

  SPECIMEN_STATUS = {
      :Destroyed                                    => 'Destroyed',
      :Discarded                                    => 'Destroyed',
      :InLab                                        => 'In Inventory',
      :InStorage                                    => 'In Storage',
      :LabDiscard                                   => 'Destroyed',
      :PulledForLab                                 => 'In Inventory',
      :Received                                     => 'In Inventory',
      :ResidualStorage                              => 'In Storage',
      :Routed                                       => 'In Transit',
      :ShippedClinicalSite                          => 'In Transit',
      :ShippedMissingSample                         => 'In Transit',
      :ShippedRegeneronPharmaceuticalsIncTarrytown  => 'In Transit',
  }.freeze

  VISIT_MAP = {
      :Screening        => 'Visit 1',
      :GenomicsDNA      => 'Visit 1',
      :GenomicsRNA      => 'Visit 1',
      :TBScreen         => 'Visit 1',
      :Visit2Baseline   => 'Visit 2 - Baseline',
      #      :V3          => 'Visit 3',
      :Visit4Week2      => 'Visit 4',
      #      :V5          => 'Visit 5',
      :Visit6Week4      => 'Visit 6',
      #      :V7          => 'Visit 7',
      #      :V8          => 'Visit 8',
      #      :V9          => 'Visit 9',
      :Visit10Week8     => 'Visit 10',
      #      :V11         => 'Visit 11',
      #      :V12         => 'Visit 12',
      #      :V13         => 'Visit 13',
      :Visit14Week12    => 'Visit 14',
      #      :V15         => 'Visit 15',
      #      :V16         => 'Visit 16',
      #      :V17         => 'Visit 17',
      :Visit18Week16    => 'Visit 18',
      :Visit19Week20    => 'Visit 19',
      :Visit20Week24    => 'Visit 20',
      :Visit21Week28    => 'Visit 21 - End of Study',
      :EarlyTermination => 'Early Termination',
      :UV               => 'Unscheduled',
      :Unscheduled1     => 'Unscheduled',
      :Unscheduled2     => 'Unscheduled',
      :Unscheduled3     => 'Unscheduled',
      :Unscheduled4     => 'Unscheduled',
      :Unscheduled5     => 'Unscheduled',
      :Unscheduled6     => 'Unscheduled',
      :Unscheduled10    => 'Unscheduled',
      :Unscheduled12    => 'Unscheduled',
      :Unscheduled13    => 'Unscheduled',
      :Unscheduled14    => 'Unscheduled',
      :Unscheduled15    => 'Unscheduled',
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
      @logger.info ("Nothing to insert :(")
      exit 0
    end

    values_clause = Array.new

    @processing_lines.each do |outline|

      if outline[2].strip == '999'
        site_id     = 'De-Identified'
        subject_id  = 'De-Identified'
      else
        site_id     =   outline[5].rjust(6, '0')
        subject_id  =  (outline[6].nil?)   ? ' NULL,' : outline[6][-3..-1]
      end

      shipped_date    = (outline[11].nil?) ? ' NULL,' : "  STR_TO_DATE(#{outline[11].insert_value} '%d%b%Y'),"
      specimen_type   = (outline[15].nil?) ? ' NULL,' : " #{SPECIMEN_TYPE[outline[15]].insert_value}"
      specimen_status = (outline[18].nil?) ? ' NULL,' : " #{SPECIMEN_STATUS[outline[18].gsub(/[^a-zA-Z]/, '').to_sym].insert_value}"

      visit_name = " #{VISIT_MAP[outline[8].gsub(/\s+/, '').to_sym].insert_value}"

      values_clause <<
          " (#{outline[1].insert_value}"                                    + # study_protocol_id
              "  #{site_id.insert_value}"                                   + # site_number
              "  #{subject_id.insert_value}"                                + # subject_code
              ' NULL,'                                                      + # subject_gender
              ' NULL,'                                                      + # subject_DOB
              "  STR_TO_DATE(#{outline[9].insert_value} '%d-%b-%Y'),"       + # specimen_collect_date
              ' NULL,'                                                      + # specimen_collect_time
              "  STR_TO_DATE(#{outline[10].insert_value} '%d-%b-%Y'),"      + # specimen_receive_datetime
              "  #{visit_name}"                                             + # visit_name
              "  #{outline[12].insert_value}"                               + # specimen_barcode
              "  #{outline[13].insert_value}"                               + # specimen_identifier
              "  #{specimen_type}"                                          + # specimen_type
              "  #{outline[14].insert_value}"                               + # specimen_name
              ' NULL,'                                                      + # specimen_parent
              "  'N',"                                                      + # specimen_ischild
              "  #{outline[17].insert_value}"                               + # specimen_condition
              "  #{specimen_status}"                                        + # specimen_status
              "  #{outline[19].insert_value}"                               + # specimen_comment
              "  #{shipped_date}"                                           + # shipped_date
              ' NULL,'                                                      + # shipped_location
              ' NULL,'                                                      + # testing_description
              "  '#{vendor}'"                                               + # vendor_code
              " )"
    end

    @logger.info "#{self.class.name} writer end"
    values_clause
  end
end
