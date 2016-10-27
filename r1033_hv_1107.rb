require 'mysql2'

class R1033_HV_1107_site
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
          " (#{outline[0].insert_value}"                              + # study_protocol_id
              " #{outline[2].rjust(6,'0').insert_value}"              + # site_number
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
              ' )'
    end

    @logger.info "#{self.class.name} writer end"
    values_clause
  end
end

class R1033_HV_1107_subject


  ARM = {
      :A15IV  => 'Cohort 1 - 1.5mg/kg IV or placebo',
      :A30IV   => 'Cohort 2 - 3mg/kg IV or placebo',
      :A60IV  => 'Cohort 3 - 6mg/kg IV or placebo',
      :A100IV   => 'Cohort 4 - 10mg/kg IV or placebo',
      :A100SC  => 'Cohort 5 - 100mg SC or placebo',
      :A200SC   => 'Cohort 6 - 200mg SC or placebo',
      :A400SC  => 'Cohort 7 - 400mg SC or placebo',
      :Placebo   => 'Cohort 8 - 6mg/kg IV or placebo',
      :REGN1193  => 'Cohort 9 - MTD IV or placebo',
  }.freeze

  TREATMENT = {
      :PartA  => 'Part A',
      :PartB  => 'Part B',
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
    @logger.info 'R668AD1021_site writer start'

    if @processing_lines.count == 0
      logger.info ("Nothing to insert :(")
      exit 0
    end

    values_clause = Array.new

    @processing_lines.each do |outline|

#     subject_treatment   = (outline[9].nil?)  ? ' NULL,' : "#{TREATMENT[('T' + outline[9][2..3]).to_sym]}".insert_value
      subject_treatment   = 'REGN1033'.insert_value
      subject_arm         = (outline[9].nil?)  ? ' NULL,' : "#{ARM[('A' + outline[10].gsub(/[^a-zA-Z0-9]/, '')).to_sym]}".insert_value
      icf_signing_date    = (outline[7].nil?)  ? ' NULL,' : "STR_TO_DATE(#{outline[7].insert_value} '%m/%d/%Y'),"
      icf_withdrawl_date  = (outline[10].nil?) ? ' NULL,' : "STR_TO_DATE(#{outline[10].insert_value} '%m/%d/%Y'),"

      values_clause <<
          "(#{outline[0].insert_value}"                             + # study_protocol_id
              " #{outline[2].rjust(6,'0').insert_value}"            + # site_number
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
              " #{subject_treatment}"                               + # treatment
              " #{subject_arm}"                                     + # arm
              " #{icf_signing_date}"                                + # ICF_signing_date
              " #{icf_withdrawl_date}"                              + # ICF withdrawl_date
              " '#{vendor}'"                                        + # vendor_code
              ")"
    end

    @logger.info "#{self.class.name} writer end"
    values_clause
  end
end

class R1033_HV_1107_RGRNInv

  SPECIMEN_TYPE = {
      'Hu Whole Blood'      => 'Whole Blood',
      'Hu Plasma (CTAD)'    => 'Plasma',
      'Hu Serum'            => 'Serum',
  }.freeze

  VISIT_MAP = {
    :V1          => 'Visit 1',
    :V2          => 'Visit 2',
    :V3          => 'Visit 3',
    :V4          => 'Visit 4',
    :V5          => 'Visit 5',
    :V6          => 'Visit 6',
    :V7          => 'Visit 7',
    :V8          => 'Visit 8',
    :V9A         => 'Visit 9',
    :V10A        => 'Visit 10',
    :V11A        => 'Visit 11',
    :V12A        => 'Visit 12',
    :V13         => 'Visit 13 - End of Study',
    :VET     	   => 'Early Termination',
    :VUNSCHED    => 'Unscheduled',
  }.freeze

  def initialize(logger)
    @logger = logger
    @logger.debug "#{self.class.name} filer Initialized"
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

    my_select = "SELECT   MAX(dsv.id),
                          dsv.site_number,
                          dsv.subject_code,
                          dsv.treatment,
                          dsv.arm
                 FROM     dts_subject_v1_0 dsv
                 WHERE    dsv.study_protocol_id = 'R1033-HV-1107'
                 GROUP BY dsv.site_number,
                          dsv.subject_code;"

    @my_subjects = this_connection.query(my_select)

    @processing_lines = Array.new
    lines = 0

    @inbound_lines.each do |specline|
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

      if outline[1].nil?
        site_id       = 'De-Identified'
        subject_id    = 'De-Identified'
        treatment     = 'De-Identified'
        arm           = 'De-Identified'
        visit         = 'Unscheduled'
        specimen_type = 'DNA'
      else
        site_id       = outline[1][-3..-1].rjust(6,'0')
        subject_id    = outline[6]
        treatment     = @my_subjects.find {|x| x['site_number'] == site_id && x['subject_code'] == subject_id}['treatment']
        arm           = @my_subjects.find {|x| x['site_number'] == site_id && x['subject_code'] == subject_id}['arm']
        visit         = VISIT_MAP[('V' + outline[7]).to_sym]
        specimen_type = SPECIMEN_TYPE[outline[11]]
      end

      values_clause <<
          "(#{outline[0].insert_value}"                                  + # study_protocol_id
              " #{site_id.insert_value}"                                 + # site_number
              " #{subject_id.insert_value}"                              + # subject_number
              ' NULL,'                                                   + # subject_gender
              ' NULL,'                                                   + # subject_DOB
              " STR_TO_DATE(#{outline[12].insert_value} '%c/%e/%Y'),"    + # specimen_collect_date
              " STR_TO_DATE(#{outline[12].insert_value} '%c/%e/%Y %T')," + # specimen_collect_time
              " STR_TO_DATE(#{outline[17].insert_value} '%c/%e/%Y %T')," + # specimen_receive_datetime
              " #{treatment.insert_value}"                               + # treatment
              " #{arm.insert_value}"                                     + # arm
              " #{visit.insert_value}"                                   + # visit_name
              " #{outline[9].insert_value}"                              + # specimen_barcode
              " #{outline[4].insert_value}"                              + # specimen_identifier
              " #{specimen_type.insert_value}"                           + # specimen_type
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

class R1033_HV_1107_MDPCInv

  SPECIMEN_TYPE = {
      :PLAS	  => 'Plasma',
      :SER    => 'Serum',
  }.freeze

  VISIT_MAP = {
:V1    => 'Visit 1',
:V2    => 'Visit 2',
:V3    => 'Visit 3',
:V4    => 'Visit 4',
:V5    => 'Visit 5',
:V6    => 'Visit 6',
:V7    => 'Visit 7',
:V8    => 'Visit 8',
:V9    => 'Visit 9',
:V10   => 'Visit 10',
:V11   => 'Visit 11',
:V12   => 'Visit 12',
:V13ET => 'Visit 13  - End of Study',
:VU1   => 'Unscheduled',
:VU2   => 'Unscheduled',
  }.freeze

  def initialize(logger)
    @logger = logger
    @logger.debug "#{self.class.name} filer Initialized"
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

    my_select = "SELECT   MAX(dsv.id),
                          dsv.site_number,
                          dsv.subject_code,
                          dsv.treatment,
                          dsv.arm
                 FROM     dts_subject_v1_0 dsv
                 WHERE    dsv.study_protocol_id = 'R1033-HV-1107'
                 GROUP BY dsv.site_number,
                          dsv.subject_code;"

    @my_subjects = this_connection.query(my_select)

    @processing_lines = Array.new
    lines = 0

    @inbound_lines.each do |specline|
      lines += 1
      @processing_lines << specline
    end

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
      specimen_ident   = outline[2]+outline[3]
      specimen_barcode = outline[2]
      site_id          = outline[7][-3..-1].rjust(6, '0')
      subject_id       = outline[10][-3..-1]
      shipped_date     = (outline[18].nil?) ? ' NULL,' : "  STR_TO_DATE(#{outline[18].insert_value} '%Y-%m-%d'),"
      specimen_type    = SPECIMEN_TYPE[outline[8].to_sym]
      visit_name       = VISIT_MAP[('V' + outline[6]).to_sym]

      if @my_subjects.find {|x| x['site_number'] == site_id && x['subject_code'] == subject_id}.nil?
        treatment        = 'UNKNOWN'
        arm              = 'UNKNOWN'
        @logger.warn "Subject ->#{subject_id}<- from Site ->#{site_id}<- for specimen ->#{specimen_ident}<- is not in the subjects file"
      else
        treatment        = @my_subjects.find {|x| x['site_number'] == site_id && x['subject_code'] == subject_id}['treatment']
        arm              = @my_subjects.find {|x| x['site_number'] == site_id && x['subject_code'] == subject_id}['arm']
      end

      values_clause <<
          " (#{outline[0].insert_value}"                                         + # study_protocol_id
              "  #{site_id.insert_value}"                                        + # site_number
              "  #{subject_id.insert_value}"                                     + # subject_code
              ' NULL,'                                                           + # subject_gender
              ' NULL,'                                                           + # subject_DOB
              "  STR_TO_DATE(#{outline[4].insert_value}  '%Y-%m-%d'),"           + # specimen_collect_date
              "  STR_TO_DATE(#{outline[4].insert_value}  '%Y-%m-%dT%k:%i:%s'),"  + # specimen_collect_time
              "  STR_TO_DATE(#{outline[17].insert_value} '%Y-%m-%dT%k:%i:%s'),"  + # specimen_receive_datetime
              "  #{treatment.insert_value}"                                      + # treatment
              "  #{arm.insert_value}"                                            + # arm
              "  #{visit_name.insert_value}"                                     + # visit_name
              "  #{specimen_barcode.insert_value}"                               + # specimen_barcode
              "  #{specimen_ident.insert_value}"                                 + # specimen_identifier
              "  #{specimen_type.insert_value}"                                  + # specimen_type
              "  #{outline[12].insert_value}"                                    + # specimen_name
              ' NULL,'                                                           + # specimen_parent
              "  'N',"                                                           + # specimen_ischild
              ' NULL,'                                                           + # specimen_condition
              "  #{outline[16].insert_value}"                                    + # specimen_status
              "  #{outline[5].insert_value}"                                     + # specimen_comment
              "  #{shipped_date}"                                                + # shipped_date
              ' NULL,'                                                           + # shipped_location
              ' NULL,'                                                           + # testing_description
              "  '#{vendor}'"                                                    + # vendor_code
              " )"
    end

    @logger.info "#{self.class.name} writer end"
    values_clause
  end
end
