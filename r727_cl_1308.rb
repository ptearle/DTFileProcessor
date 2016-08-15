require 'mysql2'

class R727_CL_1308_site
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

    @inbound_lines.each do |inline|
      found = false
      lines += 1

      @processing_lines.each do |distinct_line|

        # see if we actually need the line in the file ... Site Id already seen, then ignore
        if inline[2] == distinct_line[2]
          found = true
          break
        end
      end

      if !found
        @processing_lines << inline
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
          "(#{outline[0].insert_value}"                           + # study_protocol_id
          " #{outline[2].insert_value}"                           + # site_number
          " #{outline[3].insert_value}"                           + # site_name
          ' NULL,'                                                + # site_address
          ' NULL,'                                                + # site_city
          ' NULL,'                                                + # site_state
          " #{outline[6].insert_value}"                           + # site_country
          ' NULL,'                                                + # site_postal_code
          ' NULL,'                                                + # site_phone
          ' NULL,'                                                + # site_fax
          ' NULL,'                                                + # site_FPFV
          ' NULL,'                                                + # site_LPLV
          ' NULL,'                                                + # planned_enrollment
          " #{outline[4].insert_value}"                           + # site_PI
          ' NULL,'                                                + # site_PI_email
          ' NULL,'                                                + # site_coordinator
          ' NULL,'                                                + # site_coordinator_email
          "'Activated',"                                          + # site_status
          " '#{vendor}'"                                          + # vendor_code
          ')'
    end

    @logger.info "#{self.class.name} writer end"
    values_clause
  end
end

class R727_CL_1308_subject
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

class R727_CL_1308_RGRNinv

  SPECIMEN_TYPE = {
      'Hu Whole Blood'      => 'Whole Blood',
      'Hu Plasma (EDTA)'    => 'Plasma',
      'Hu Serum'            => 'Serum',
      'DNA extract'         => 'DNA',
  }.freeze

  VISIT_MAP = {
#                  'Visit 1',
#                  'Visit 2',
      :V3       => 'Visit 3 - Baseline',
#                  'Visit 4',
#                  'Visit 5',
#                  'Visit 6',
#                  'Visit 7',
#                  'Visit 8',
      :V9       => 'Visit 9',
#                  'Visit 10',
      :V11      => 'Visit 11',
      :V12      => 'Visit 12',
      :V13      => 'Visit 13',
      :V14      => 'Visit 14',
      :V15      => 'Visit 15',
      :V16      => 'Visit 16',
      :V17      => 'Visit 17 - End of Treatment',
      :V18      => 'Visit 18 - End of Study',
      :VET      => 'Early Termination',
      :VUNSCHED => 'Unscheduled',
      :VMON     => 'Visit 8 - End of Treatment',
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
          "(#{outline[0].insert_value}"                              + # study_protocol_id
          " #{outline[1].insert_value}"                              + # site_number
          " #{outline[6].insert_value}"                              + # subject_code
          ' NULL,'                                                   + # subject_gender
          ' NULL,'                                                   + # subject_DOB
          " STR_TO_DATE(#{outline[12].insert_value} '%c/%e/%Y'),"    + # specimen_collect_date
          " STR_TO_DATE(#{outline[12].insert_value} '%c/%e/%Y %T')," + # specimen_collect_time
          " STR_TO_DATE(#{outline[17].insert_value} '%c/%e/%Y %T')," + # specimen_receive_datetime
          " #{VISIT_MAP[('V' + outline[7].sub(/#{'/'}/, '_')).to_sym].insert_value}"   + # visit_name
          " #{outline[9].insert_value}"                              + # specimen_barcode
          " #{outline[4].insert_value}"                              + # specimen_identifier
          " #{SPECIMEN_TYPE[outline[11].strip].insert_value}"        + # specimen_type
          " #{outline[10].insert_value}"                             + # specimen_name
          ' NULL,'                                                   + # specimen_parent
          " 'N',"                                                    + # specimen_ischild
          " #{outline[13].insert_value}"                             + # specimen_condition
          " 'In Inventory',"                                         + # specimen_status
          " #{outline[28].insert_value}"                             + # specimen_comment
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

class R727_CL_1308_MDPCInv

  VISIT_MAP = {
:Visit_1                 => 'Visit 1',
#            'Visit 2',
:Visit_3                 => 'Visit 3 - Baseline',
# 'Visit 4',
:Visit_5                 => 'Visit 5',
:Visit_6                 => 'Visit 6',
:Visit_7                 => 'Visit 7',
:Visit_8                 => 'Visit 8',
:Visit_9                 => 'Visit 9',
# Visit 10',
# 'Visit 11',
# 'Visit 12',
# 'Visit 13',
# 'Visit 14',
:Visit_15                => 'Visit 15',
# 'Visit 16',
:Visit_17                => 'Visit 17 - End of Treatment',
# 'Visit 18 - End of Study',
:Early_Termination_Visit => 'Early Termination',
:Unscheduled_Visit       => 'Unscheduled',
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

    @processing_lines = Array.new
    lines = 0

    @inbound_lines.each do |specline|
      found = false
      lines += 1

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

      site_id         = outline[7].gsub(/-/, '')
      subject_id      = outline[3][-3..-1]
      specimen_status = (outline[16] == 'In Storage') ? 'In Inventory' : outline[16]

      values_clause <<
          "(#{outline[0].insert_value}"                                    + # study_protocol_id
          " #{site_id.insert_value}"                                       + # site_number
          " #{subject_id.insert_value}"                                    + # subject_code
          ' NULL,'                                                         + # subject_gender
          ' NULL,'                                                         + # subject_DOB
          " STR_TO_DATE(#{outline[4].insert_value} '%Y-%m-%dT%k:%i:%s'),"  + # specimen_collect_date
          " STR_TO_DATE(#{outline[4].insert_value} '%Y-%m-%dT%k:%i:%s'),"  + # specimen_collect_time
          " STR_TO_DATE(#{outline[17].insert_value} '%Y-%m-%dT%k:%i:%s')," + # specimen_receive_datetime
          " #{VISIT_MAP[(outline[11].sub(/ /, '_')).to_sym].insert_value}" + # visit_name
          " #{outline[2].insert_value}"                                    + # specimen_barcode
          " #{outline[3].insert_value}"                                    + # specimen_identifier
          " 'Serum',"                                                      + # specimen_type
          ' NULL,'                                                         + # specimen_name
          " #{outline[13].insert_value}"                                   + # specimen_parent
          " #{outline[14].insert_value}"                                   + # specimen_ischild
          ' NULL,'                                                         + # specimen_condition
          " #{specimen_status.insert_value}"                               + # specimen_status
          ' NULL,'                                                         + # specimen_comment
          ' NULL,'                                                         + # shipped_date
          ' NULL,'                                                         + # shipped_location
          ' NULL,'                                                         + # testing_description
          " '#{vendor}'"                                                   + # vendor_code
          ")"
    end

    @logger.info "#{self.class.name} writer end"
    values_clause
  end
end
