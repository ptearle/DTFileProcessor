require 'mysql2'

class R2222_HV_1326_site
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
              " #{outline[1][0..5].insert_value}"                     + # site_number
              " #{outline[3].insert_value}"                           + # site_name
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

class R2222_HV_1326_subject

  ARM = {
      :REGN22223mgkgIV  => 'Cohort IV1 – 3mgkg IV',
      :REGN22223mgkgIM  => 'Cohort IM1 – 3mg/kg IM Q4W',
      :REGN222210mgkgIM => 'Cohort IM2 – 10mg/kg IM Q4W',
      :REGN222210mgkgIV => 'Cohort IV2 – 10mg/kg IV',
      :REGN222230mgkgIV => 'Cohort IV3 – 30mg/kg IV Q4W',
      :PlaceboIM        => 'Placebo IM',
      :PlaceboIV        => 'Placebo IV',
  }.freeze

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

      subject_arm         = (outline[9].nil?) ? '  NULL,'  : " #{ARM[outline[9].gsub(/[^a-zA-Z0-9]/, '').to_sym].insert_value}"
      icf_signing_date    = (outline[7].nil?)  ? ' NULL,' : "  STR_TO_DATE(#{outline[7].insert_value} '%m/%d/%Y'),"
      icf_withdrawl_date  = (outline[10].nil?) ? ' NULL,' : "  STR_TO_DATE(#{outline[10].insert_value} '%m/%d/%Y'),"

      values_clause <<
          "(#{outline[0].insert_value}"                             + # study_protocol_id
              " #{outline[1][0..5].insert_value}"                   + # site_number
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
              " 'REGN2222',"                                        + # treatment
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

class R2222_HV_1326_RGRNInv

  SPECIMEN_TYPE = {
      'HU Whole Blood'      => 'Whole Blood',
      'Hu Plasma (EDTA)'    => 'Plasma',
      'Hu Serum'            => 'Serum',
  }.freeze

  VISIT_MAP1 = {
		:V1          => 'Visit 1',
		:V2          => 'Visit 2',
		:V3          => 'Visit 3 – Baseline',
		:V4          => 'Visit 4',
		:V5          => 'Visit 5',
		:V6          => 'Visit 6',
		:V7          => 'Visit 7',
		:V8          => 'Visit 8',
		:V9          => 'Visit 9',
		:V10         => 'Visit 10',
		:V11         => 'Visit 11 – End of Study',
		:VET     	   => 'Early Termination',
		:VUNSCHED    => 'Unscheduled',
  }.freeze

  VISIT_MAP2 = {
    :V1          => 'Visit 1',
    :V2          => 'Visit 2',
    :V3          => 'Visit 3 - Baseline',
    :V4          => 'Visit 4',
    :V5          => 'Visit 5',
    :V6          => 'Visit 6',
    :V7          => 'Visit 7',
    :V8          => 'Visit 8',
    :V9          => 'Visit 9',
    :V10         => 'Visit 10',
    :V11         => 'Visit 11',
    :V12         => 'Visit 12',
    :V13         => 'Visit 13',
    :V14         => 'Visit 14',
    :V15         => 'Visit 15',
    :V16         => 'Visit 16',
    :V17         => 'Visit 17 – End of Study',
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

    my_select = "SELECT   MAX(dsv.id),
                          dsv.site_number,
                          dsv.subject_code,
                          dsv.arm
                 FROM     dts_subject_v1_0 dsv
                 WHERE    dsv.study_protocol_id = 'R2222-HV-1326'
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
        site_id     = 'De-Identified'
        subject_id  = 'De-Identified'
        arm         = 'De-Identified'
        visit       = 'Unscheduled'
      else
        site_id     = outline[1]
        subject_id  = outline[6]
        arm         = @my_subjects.find {|x| x['site_number'] == site_id && x['subject_code'] == subject_id}['arm']

        case arm
          when 'Cohort IV1 – 3mgkg IV',
               'Cohort IV2 – 10mg/kg IV',
               'Placebo IV'
            visit = VISIT_MAP1[('V' + outline[7]).to_sym]
          when 'Cohort IV2 – 10mg/kg IV',
               'Cohort IM1 – 3mg/kg IM Q4W',
               'Cohort IM2 – 10mg/kg IM Q4W',
               'Placebo IM'
            visit = VISIT_MAP2[('V' + outline[7]).to_sym]
          else
            visit = 'UNKNOWN'
        end
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
              " 'REGN2222',"                                             + # treatment
              " #{arm.insert_value}"                                     + # arm
              " #{visit.insert_value}"                                   + # visit_name
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

class R2222_HV_1326_LCRPInv

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
      :S164 => 'Urine',
      :S269 => 'Plasma',
      :S270 => 'Plasma',
      :S533 => 'Biospy',
      :S353 => 'Whole Blood',
      :S370 => 'RNA',
      :S725	=> 'DNA',
      :S875 => 'Tissue',
      :S877 => 'Swab',
      :S908	=> 'Whole Blood',
      :S955 => 'Tissue',
      :S947 => 'Plasma',
  }.freeze

  VISIT_MAP1 = {
      :V1          => 'Visit 1',
      :V12V2       => 'Visit 2',
      :V12BL       => 'Visit 3 – Baseline',
      :V12V4       => 'Visit 4',
      :V12V5       => 'Visit 5',
      :V12V6       => 'Visit 6',
      :V12V7       => 'Visit 7',
      :V12V8       => 'Visit 8',
      :V12V9       => 'Visit 9',
      :V12V10      => 'Visit 10',
      :VIV1IV2V11  => 'Visit 11 – End of Study',
      :VIV1IV2ET   => 'Early Termination',
      :DID         => 'Unscheduled',
      :DNA         => 'Unscheduled',
  }.freeze

  VISIT_MAP2 = {
      :V1          => 'Visit 1',
      :VIVMMV2     => 'Visit 2',
      :VIV3BL      => 'Visit 3 - Baseline',
      :VIVMMBL     => 'Visit 3 - Baseline',
      :VIVMMV4     => 'Visit 4',
      :VIVMMV5     => 'Visit 5',
      :VIVMMV6     => 'Visit 6',
      :VIVMMV7     => 'Visit 7',
      :VIVMMV8     => 'Visit 8',
      :VIVMMV9     => 'Visit 9',
      :VIVMMV10    => 'Visit 10',
      :VIVMMV11    => 'Visit 11',
      :VIVMMV12    => 'Visit 12',
      :VIVMMV13    => 'Visit 13',
      :VIVMMV14    => 'Visit 14',
      :VIVMMV15    => 'Visit 15',
      :VIVMMV16    => 'Visit 16',
      :VIV3M1M2V17 => 'Visit 17 – End of Study',
      :VIV3M1M2ET  => 'Early Termination',
      :DID         => 'Unscheduled',
      :DNA         => 'Unscheduled',
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

    my_select = "SELECT   MAX(dsv.id),
                          dsv.site_number,
                          dsv.subject_code,
                          dsv.arm
                 FROM     dts_subject_v1_0 dsv
                 WHERE    dsv.study_protocol_id = 'R2222-HV-1326'
                 GROUP BY dsv.site_number,
                          dsv.subject_code;"

    @my_subjects = this_connection.query(my_select)

    @processing_lines = Array.new
    lines = 0
    num_distinct_lines = 0

    @inbound_lines.each do |specline|
      found = false
      lines += 1

      # If the specimen number is zero or there is no receive date, ignore
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

      if outline[2].strip == 'DR.SMART'
        site_id     = 'De-Identified'
        subject_id  = 'De-Identified'
        arm         = 'De-Identified'
        visit       = 'Unscheduled'
      else
        site_id     = outline[2].rjust(6, '0')
        subject_id  = outline[3][-3..-1]

        if @my_subjects.find {|x| x['subject_code'] == subject_id}.nil?
          @logger.warn "Subject ->#{subject_id}<- at site #{site_id} missing from subject file for specimen #{specimen_barcode}, estimating arm"

          if  outline[10][0..2] == '1-2'
            arm = 'Cohort IV1 – 3mgkg IV'
          else
            arm = 'Cohort IM1 – 3mg/kg IM Q4W'
          end
        else
          arm = @my_subjects.find {|x| x['subject_code'] == subject_id}['arm']
        end

        case arm
          when 'Cohort IV1 – 3mgkg IV',
               'Cohort IV2 – 10mg/kg IV',
               'Placebo IV'
            visit = VISIT_MAP1[('V' + outline[10].gsub(/[^a-zA-Z0-9]/, '')).to_sym]
          when 'Cohort IV3 – 30mg/kg IV Q4W',
               'Cohort IM1 – 3mg/kg IM Q4W',
               'Cohort IM2 – 10mg/kg IM Q4W',
               'Placebo IM'
            visit = VISIT_MAP2[('V' + outline[10].gsub(/[^a-zA-Z0-9]/, '')).to_sym]
          else
            visit = 'UNKNOWN'
        end
      end

      shipped_date    = (outline[35].nil?) ? ' NULL,' : "  STR_TO_DATE(#{outline[35].insert_value} '%d%b%Y'),"
      specimen_type   = (outline[23].nil?) ? ' NULL,' : " #{SPECIMEN_TYPE[('S' + outline[23]).to_sym].insert_value}"
      specimen_status = (outline[33].nil?) ? "'In Inventory'," : "'In Transit',"

      if outline[18].nil? and outline[19].nil?
        testing_desc  = ' NULL,'
      elsif outline[19].nil?
        testing_desc  = "'#{outline[18].strip}',"
      elsif outline[18].nil?
        testing_desc  = "'#{outline[19].strip}',"
      else
        testing_desc  = "'#{outline[18].strip} / #{outline[19].strip}',"
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
              " 'REGN2222',"                                                + # treatment
              "  #{arm.insert_value}"                                       + # arm
              "  #{visit.insert_value}"                                     + # visit_name
              "  #{specimen_barcode.insert_value}"                          + # specimen_barcode
              ' NULL,'                                                      + # specimen_identifier
              "  #{specimen_type}"                                          + # specimen_type
              "  #{outline[24].insert_value}"                               + # specimen_name
              ' NULL,'                                                      + # specimen_parent
              "  'N',"                                                      + # specimen_ischild
              "  #{outline[26].insert_value}"                               + # specimen_condition
              "  #{specimen_status}"                                        + # specimen_status
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
