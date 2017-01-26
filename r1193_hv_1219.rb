require 'mysql2'

class R1193_HV_1219_site
  def initialize(logger)
    @logger = logger
    @logger.debug "#{self.class.name} filer initialized"
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
        if specline['SITEID'] == distinct_line['SITEID']
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
          " (#{outline['STUDYID'].insert_value}"                              + # study_protocol_id
              " #{outline['SITEID'].insert_value}"                     + # site_number
              ' NULL,'                                                + # site_name
              ' NULL,'                                                + # site_address
              ' NULL,'                                                + # site_city
              ' NULL,'                                                + # site_state
              ' NULL,'                                                + # site_country
              ' NULL,'                                                + # site_postal_code
              ' NULL,'                                                + # site_phone
              ' NULL,'                                                + # site_fax
              ' NULL,'                                                + # site_FPFV
              ' NULL,'                                                + # site_LPLV
              ' NULL,'                                                + # planned_enrollment
              ' NULL,'                                                + # site_PI
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

class R1193_HV_1219_subject

  ARM = {
      :GroupA  => 'Group A',
      :GroupB  => 'Group B',
 }.freeze

  def initialize(logger)
    @logger = logger
    @logger.debug "#{self.class.name} filer Initialized"
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
    @logger.info "#{self.class.name} writer start"

    if @processing_lines.count == 0
      logger.info ("Nothing to insert :(")
      exit 0
    end

    values_clause = Array.new

    @processing_lines.each do |outline|

      subject_arm         = (outline[9].nil?)  ? ' NULL,' : " #{ARM[outline['ACTARMCD'].gsub(/[^a-zA-Z0-9]/, '').to_sym].insert_value}"
      icf_signing_date    = (outline[7].nil?)  ? ' NULL,' : "  STR_TO_DATE(#{outline['LSTICDTC'].insert_value} '%Y-%m-%d'),"
      icf_withdrawl_date  = (outline[10].nil?) ? ' NULL,' : "  STR_TO_DATE(#{outline['DSWDDTC'].insert_value} '%Y-%m-%d'),"
      enrollmeent_status  = 'Randomized'.insert_value
      treatment           = 'REGN1193'.insert_value

      values_clause <<
          "(#{outline['STUDYID'].insert_value}"                     + # study_protocol_id
              " #{outline['SITEID'].insert_value}"                  + # site_number
              " #{outline['SUBJID'][-3..-1].insert_value}"          + # subject_code
              " #{outline['SUBJID'].insert_value}"                  + # subject_external_id
              ' NULL,'                                              + # randomization_number
              ' NULL,'                                              + # gender
              ' NULL,'                                              + # initials
              "  #{enrollmeent_status}"                             + # enrollment_status
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
              "  #{treatment}"                                      + # treatment
              "  #{subject_arm}"                                    + # arm
              "  #{icf_signing_date}"                               + # ICF_signing_date
              "  #{icf_withdrawl_date}"                             + # ICF withdrawl_date
              " '#{vendor}'"                                        + # vendor_code
              ")"
    end

    @logger.info "#{self.class.name} writer end"
    values_clause
  end
end

class R1193_HV_1219_Inventory
  VISIT_MAP = {
      :V2               => 'Visit 2',
      :V3               => 'Visit 3',
      :V4               => 'Visit 4 (Baseline)',
      :V5               => 'Visit 5',
      :V6               => 'Visit 6',
      :V7               => 'Visit 7',
      :V8               => 'Visit 8',
      :V9               => 'Visit 9',
      :V10              => 'Visit 10',
      :V11              => 'Visit 11',
      :V12              => 'Visit 12',
      :V13              => 'Visit 13',
      :V14              => 'Visit 14',
      :V15              => 'Visit 15',
      :V16              => 'Visit 16 End of Study/ ET',
      :VET              => 'Visit 16 End of Study/ ET'
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
    @logger.info "#{self.class.name} processor"

    @processing_lines = Array.new

    @inbound_lines.each do |specline|
      if specline['subject'].nil? or specline['subject'] == ''
        next
      end

      if specline['aliquot_name'].nil? || specline['aliquot_name'] == ''
        @logger.info '================= QUERY NEED ===================='
        @logger.info "ECORD EXCLUDED - Missing aliquot_name on specimen idaliquot_id ->#{specline['aliquot_id']}<-"
        @logger.info "Full record ->#{specline}<-"
        next
      end

      if specline['visit'].nil? || specline['visit'] == ''
        @logger.info '================= QUERY NEED ===================='
        @logger.info "RECORD EXCLUDED -  missing visit for specimen id ->#{specline['aliquot_name']}<-"
        @logger.info "Full record ->#{specline}<-"
        next
      else
        visit_name = VISIT_MAP[('V'+specline['visit'].gsub(/[^a-zA-Z0-9]/, '')).to_sym]

        if visit_name.nil? || visit_name == ''
          @logger.info '================= QUERY NEED ===================='
          @logger.info "RECORD EXCLUDED - Unknown visit_name ->#{specline['visit']}<- specimen id ->#{specline['aliquot_name']}<-"
          @logger.info "Full record ->#{specline}<-"
          next
        else

        end
      end

      @processing_lines << specline
    end

    @logger.info "#{self.class.name} processor end"
    @processing_lines.length
  end

  def writer(vendor)
    @logger.info "#{self.class.name} writer start"

    values_clause = Array.new

    @processing_lines.each do |outline|

      visit_name         = (outline['visit'].nil?)                  ? ' NULL,' : VISIT_MAP[('V'+outline['visit'].gsub(/[^a-zA-Z0-9]/, '')).to_sym].insert_value
      visit_date         = (outline['date_time_sample_drawn'].nil?) ? ' NULL,' : "STR_TO_DATE(#{outline['date_time_sample_drawn'].insert_value} '%c/%e/%Y'),"
      subject_gender     = 'NULL,'
      date_of_birth      = 'NULL,'
      subject_enrollment = 'NULL,'
      collection_date    = (outline['date_time_sample_drawn'].nil?) ? ' NULL,' : "STR_TO_DATE(#{outline['date_time_sample_drawn'].insert_value} '%c/%e/%Y'),"
      collection_time    = (outline['date_time_sample_drawn'].nil?) ? ' NULL,' : "STR_TO_DATE(#{outline['date_time_sample_drawn'].insert_value} '%T'),"
      received_date      = (outline['received_on'].nil?)            ? ' NULL,' : "STR_TO_DATE(#{outline['received_on'].insert_value} '%c/%e/%Y'),"
      specimen_shipdate  = 'NULL,'
      specimen_condition = outline['condition'].insert_value
      specimen_status    = 'In Storage'.insert_value
      designation        = outline['designation'].insert_value
      specimen_type      = (outline['sample_type'] == 'Hu Serum') ? 'Serum'.insert_value : 'Plasma'.insert_value
      storage_faciliy    = 'Regeneron'.insert_value

      values_clause <<
          " (#{outline['study'].insert_value}"                            + # study_protocol_id
              ' 1,'                                                       + # study_protocol_version
              " #{outline['site'].insert_value}"                          + # site_number,
              ' NULL,'                                                    + # collection_site_country,
              ' NULL,'                                                    + # date_departed_site,
              " #{outline['subject'].insert_value}"                       + # subject_num,
              " #{subject_gender}"                                        + # subject_gender,
              " #{date_of_birth}"                                         + # subject_date_of_birth,
              ' NULL,'                                                    + # subject_race,
              ' NULL,'                                                    + # subject_ethnicity,
              " #{subject_enrollment}"                                    + # subject_enrollment_status,
              " #{collection_date}"                                       + # specimen_collect_date,
              " #{collection_time}"                                       + # specimen_collect_time,
              " #{received_date}"                                         + # received_datetime,
              ' NULL,'                                                    + # treatment,
              ' NULL,'                                                    + # arm,
              " #{visit_name}"                                            + # visit_name,
              " #{visit_date}"                                            + # visit_date,
              ' NULL,'                                                    + # visit_day,
              ' NULL,'                                                    + # visit_week,
              ' NULL,'                                                    + # visit_timepoint,
              " #{outline['aliquot_name'].insert_value}"                  + # specimen_barcode,
              " #{outline['aliquot_id'].insert_value}"                    + # specimen_identifier,
              " #{specimen_type}"                                         + # specimen_type,
              ' NULL,'                                                    + # specimen_name,
              ' NULL,'                                                    + # specimen_volume,
              ' NULL,'                                                    + # specimen_volume_units,
              ' NULL,'                                                    + # specimen_mass,
              ' NULL,'                                                    + # specimen_mass_units,
              " #{designation}"                                           + # specimen_designation,
              ' NULL,'                                                    + # specimen_designation_detail,
              ' NULL,'                                                    + # specimen_collection_tube_type,
              ' NULL,'                                                    + # specimen_quality,
              ' NULL,'                                                    + # specimen_parent_id,
              ' NULL,'                                                    + # specimen_ischild,
              ' NULL,'                                                    + # specimen_child_type,
              " #{specimen_condition}"                                    + # specimen_condition,
              " #{specimen_status}"                                       + # specimen_status,
              ' NULL,'                                                    + # specimen_comment,
              " #{specimen_shipdate}"                                     + # shipped_date,
              " #{outline[38].insert_value}"                              + # shipped_location,
              " #{storage_faciliy}"                                       + # storage_vendor,
              " #{storage_faciliy}"                                       + # storage_facility_name,
              " #{outline['location'].insert_value}"                      + # storage_location,
              ' NULL,'                                                    + # testing_description,
              " '#{vendor}'"                                              + # vendor_code
              ')'
    end

    @logger.info "#{self.class.name} writer end"
    values_clause
  end
end
