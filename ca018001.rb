require 'mysql2'

class CA018001_CTMS
  def initialize(logger)
    @logger = logger
    @logger.debug "#{self.class.name} filer initialized"
  end

  def reader(inbound_file)
    @logger.debug "File name is ->#{inbound_file}<-"
    @logger.info "#{self.class.name} reader start"
    @inbound_lines = CSV.read(inbound_file, headers: true, skip_blanks: true, skip_lines: "\r\n", encoding: 'windows-1256:utf-8')
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

      # Only process Principle Investigator lines

      if inline[28] != 'Yes'
        next
      end

      # if site id is not there error and ignore

      if inline[4] == ''
        @logger.error 'Missing site id, unable to load ...'
        @logger.error "->#{inline}<-"
        next
      end

      @processing_lines.each do |distinct_line|

        # see if we actually need the line in the file ... Site Id already seen, then ignore
        if inline[4] == distinct_line[4]
          # If there is a dup ... log as an error and ignore
          @logger.error 'Duplicate Site line found ...'
          @logger.error "->#{distinct_line}<-"
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
      @logger.info ("Nothing to insert :(")
      exit 0
    end

    values_clause = Array.new

    @processing_lines.each do |outline|

      address = outline[10] + ((outline[11].nil?) ? '' : ", #{outline[11]}") + ((outline[12].nil?) ? '' : ", #{outline[12]}")

      state   = (outline[14] == 'MA') ? 'Massachusetts' : outline[14]

      values_clause <<
              " (#{outline[5].insert_value}"                          + # study_protocol_id
              "  #{outline[4].insert_value}"                          + # site_number
              "  #{outline[9].insert_value}"                          + # site_name
              "  #{address.insert_value}"                             + # site_address
              "  #{outline[13].insert_value}"                         + # site_city
              "  #{state.insert_value}"                               + # site_state
              "  #{outline[2].insert_value}"                          + # site_country
              "  #{outline[15].insert_value}"                         + # site_postal_code
              "  #{outline[17].insert_value}"                         + # site_phone
              "  #{outline[18].insert_value}"                         + # site_fax
              ' NULL,'                                                + # site_FPFV
              ' NULL,'                                                + # site_LPLV
              ' NULL,'                                                + # planned_enrollment
              "  #{outline[3].insert_value}"                          + # site_PI
              "  #{outline[16].insert_value}"                         + # site_PI_email
              ' NULL,'                                                + # site_coordinator
              ' NULL,'                                                + # site_coordinator_email
              "  #{outline[20].insert_value}"                         + # site_status
              "  '#{vendor}'"                                         + # vendor_code
              ' )'
    end

    @logger.info "#{self.class.name} writer end"
    values_clause
  end
end


class CA018001_ivrt
  def initialize(logger)
    @logger = logger
    @logger.debug "#{self.class.name} filer Initialized"
  end

  def reader(inbound_file)
    @logger.debug "File name is ->#{inbound_file}<-"
    @logger.info "#{self.class.name} reader start"
    @inbound_lines = CSV.read(inbound_file, headers: true, skip_blanks: true, encoding:'windows-1256:utf-8')
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
    @logger.info "#{self.class.name} writer start'"

    if @processing_lines.count == 0
      logger.info ("Nothing to insert :(")
      exit 0
    end

    values_clause = Array.new

    @processing_lines.each do |outline|

      @logger.debug "Field 0 is ->#{outline[0]}<-"

      external_id = outline[1].strip.rjust(4, '0')+'-'+outline[0].strip.rjust(5, '0')

      case outline[3]
        when 'Screening'
          values_clause <<
              " ('CA018-001',"                                          + # study_protocol_id
                  " #{outline[1].rjust(4, '0').insert_value}"           + # site_number
                  " #{outline[0].rjust(5, '0').insert_value}"           + # subject_code
                  " #{external_id.insert_value}"                        + # subject_external_id
                  ' NULL,'                                              + # randomization number
                  ' NULL,'                                              + # gender
                  ' NULL,'                                              + # initials
                  "'Screening',"                                        + # enrollment_status
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
                  ' NULL,'                                              + # treatment
                  ' NULL,'                                              + # arm
                  ' NULL,'                                              + # ICF_signing_date ' NULL,'                                              + # ICF withdrawl_date
                  " '#{vendor}'"                                        + # vendor_code
                  ")"
        when 'Randomization'

          case outline[5]
            when 'Nivolumab in Combination with BMS-986016'
              treatment_arm = 'NIVO+BMS986016'
            when 'Nivolumab in Combination with Dasatinib'
              treatment_arm = 'NIVO+DASATINIB'
            when 'Nivolumab Monotherapy'
              treatment_arm = 'Nivolumab'
            else
              @logger.warn "Unknown treatment arm ->#{outline[5]} on line #{values_clause.length+1}"
              treatment_arm = "Unknown treatment arm ->#{outline[5]}<-"
          end

          values_clause <<
              "('CA018-001',"                                           + # study_protocol_id
                  " #{outline[1].rjust(4, '0').insert_value}"           + # site_number
                  " #{outline[0].rjust(5, '0').insert_value}"           + # subject_code
                  " #{external_id.insert_value}"                        + # subject_external_id
                  " #{outline[6].insert_value}"                         + # randomization number
                  ' NULL,'                                              + # gender
                  ' NULL,'                                              + # initials
                  "'Randomized',"                                       + # enrollment_status
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
                  " #{treatment_arm.insert_value}"                      + # treatment
                  " #{outline[7].insert_value}"                         + # arm
                  ' NULL,'                                              + # ICF_signing_date
                  ' NULL,'                                              + # ICF withdrawl_date
                  " '#{vendor}'"                                        + # vendor_code
                  ")"

      end
    end

    @logger.info "#{self.class.name} writer end"
    values_clause
  end
end

class CA018001_EDD
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
    @logger.info "#{self.class.name} processor start"

    @processing_lines = Array.new
    lines = 0
    num_distinct_lines = 0

    @inbound_lines.each do |specline|
      found = false
      lines += 1

      # Ignore if ISO Value is "No specimen received"
      # Ignore if Test Status is "Assigned"
	  
	    if specline[22].strip == "Assigned"
	     next
	    end
	  
	    unless specline[23].nil?
        if specline[23].strip == "No specimen received"
          next
		    end
      end

      @processing_lines.each do |distinct_line|

        # see if we actually need the line in the file ... Accession+TubeNumber already seen, then ignore

        if specline[0]+specline[9] == distinct_line[0]+distinct_line[9]
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
      logger.info "Nothing to insert :("
      exit 0
    end

    values_clause = Array.new

    @processing_lines.each do |outline|

      parent_id        = (outline[11].nil?)           ? "NULL"   : "'#{outline[8].strip}#{outline[11].strip}'"
      is_child         = (parent_id == 'NULL')        ? 'N'      : 'Y'
      specimen_type    = (outline[16][0..2] == 'SCR') ? 'Tissue' : 'Whole Blood'
      receive_datetime = (is_child == 'Y')            ? "NULL"   : "STR_TO_DATE('#{outline[15].strip}',  '%c/%e/%Y %H:%i')"

      values_clause <<
                       " ('CA018-001',"                                           + # study_protocol_id
                       "  '#{outline[2].strip}',"                                 + # site_number
                       "  '#{outline[4].strip}',"                                 + # subject_number
                       "  '#{outline[6].strip}',"                                 + # subject_gender
                       "  STR_TO_DATE('#{outline[5].strip.insert(-3, '19')}',  '%e-%b-%Y'),"    + # subject_DOB
                       "  STR_TO_DATE('#{outline[7].strip}',  '%c/%e/%Y %H:%i'),"    + # specimen_collect_date
                       "  STR_TO_DATE('#{outline[7].strip}',  '%c/%e/%Y %H:%i'),"    + # specimen_collect_time
                       "  #{receive_datetime},"                                   + # specimen_receive_datetime
                       ' NULL,'                                                   + # treatment
                       ' NULL,'                                                   + # arm
                       "  '#{outline[16].strip}',"                                + # visit_name
                       "  '#{outline[8].strip}#{outline[9].strip}',"              + # specimen_barcode
                       '  NULL,'                                                  + # specimen_identifier
                       "  '#{specimen_type}',"                                    + # specimen_type
                       '  NULL,'                                                  + # specimen_name
                       "  #{parent_id},"                                          + # specimen_parent
                       "  '#{is_child}',"                                         + # specimen_ischild
                       '  NULL,'                                                  + # specimen_condition
                       "  'In Inventory',"                                        + # specimen_status
                       '  NULL,'                                                  + # specimen_comment
                       '  NULL,'                                                  + # specimen_shipdate
                       '  NULL,'                                                  + # shipped_location
                       '  NULL,'                                                  + # testing_description
                       "  '#{vendor}'"                                            + # vendor_code
                       " )"
    end

    @logger.info "#{self.class.name} writer end"
    values_clause
  end
end

class CA018001_BMS
  def initialize(logger)
    @logger = logger
    @logger.debug "#{self.class.name} filer initialized"
  end

  def reader(inbound_file)
    @logger.info "#{self.class.name} reader start"
    @inbound_lines = CSV.read(inbound_file, headers: true, skip_blanks: true, skip_lines: '\r', encoding:'windows-1256:utf-8')
    @logger.info "#{self.class.name}reader end"
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

      site_number       =  outline[1].split(' ')[0].strip
      subject_number    =  outline[1].split(' ')[1].strip
      visit_name        = (outline[5].nil?) ? 'NULL' : "'#{outline[5].strip}'"
      specimen_barcode  =  outline[6].strip

      if outline[6][-2..-1] == '01' and visit_name.downcase.include? 'cyto'
        specimen_barcode += 'BMS'
      end

      specimen_parent   = (outline[9].nil?)  ? 'NULL' : "'#{outline[9]}'"
      specimen_ischild  = (outline[9].nil?)  ? "'N'" : "'Y'"
      specimen_shipdate = (outline[12].nil?) ? 'NULL' : "STR_TO_DATE('#{outline[4].strip}',  '%c/%e/%Y %l:%i')"

      values_clause <<
                       " ('#{outline[0]}',"                                       + # study_protocol_id
                       " '#{site_number}',"                                       + # site_number
                       " '#{subject_number}',"                                    + # subject_number
                       ' NULL,'                                                   + # subject_gender
                       ' NULL,'                                                   + # subject_DOB
                       " STR_TO_DATE('#{outline[2].strip}',  '%c/%e/%Y'),"        + # specimen_collect_date
                       " STR_TO_DATE('#{outline[3].strip}',  '%T'),"              + # specimen_collect_time
                       " STR_TO_DATE('#{outline[4].strip}',  '%c/%e/%Y %l:%i'),"  + # specimen_receive_datetime
                       ' NULL,'                                                   + # treatment
                       ' NULL,'                                                   + # arm
                       " #{visit_name},"                                          + # visit_name
                       " '#{specimen_barcode}',"                                  + # specimen_barcode
                       " '#{outline[7].strip}',"                                  + # specimen_identifier
                       " '#{outline[8].strip}',"                                  + # specimen_type
                       ' NULL,'                                                   + # specimen_name
                       " #{specimen_parent},"                                     + # specimen_parent
                       " #{specimen_ischild},"                                    + # specimen_ischild
                       ' NULL,'                                                   + # specimen_condition
                       " '#{outline[11].strip}',"                                 + # specimen_status
                       '  NULL,'                                                  + # specimen_comment
                       " #{specimen_shipdate},"                                   + # specimen_shipdate
                       '  NULL,'                                                  + # shipped_location
                       '  NULL,'                                                  + # testing_description
                       " '#{vendor}'"                                             + # vendor_code
                       ')'
    end

    @logger.info "#{self.class.name} writer end"
    values_clause
  end
end

class CA018001_QINV
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

class CA018001_QASY
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
      :UNSCHED_4	 => 'C1 D1 EOI PK',
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
      :BLOCK     => 'Tissue',
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

    @processing_lines = Array.new
    lines = 0
    current_accession = ''
    specimens = Mysql2::Result.new

    @inbound_lines.each do |assayline|
      lines += 1

      # Ignore if assay code starts with "SMT"
      if assayline[26].strip.start_with?('SMT')
        next
      end

      #check to see if we have a new accession.
      if assayline[5].strip != current_accession
        current_accession = assayline[5]

        begin
          specimens = this_connection.query("SELECT barcode FROM specimens WHERE barcode like '#{current_accession}%'")
        rescue Mysql2::Error => e
          @logger.error "DB Error - get specimen by accession - #{e.message}"
          exit -1
        end
      end

      if specimens.nil? or specimens.count == 0
       next
      end

      # add a adt result for each specimen in the accession
      specimens.each do |this_specimen|
        assayline[5] = "#{this_specimen['barcode']}"
        new_line = Array.new
        assayline.each do |e| new_line << e[1] end
        @processing_lines << new_line
      end
    end

    @logger.info "#{self.class.name} processor end"
    @processing_lines.length
  end

  def writer(vendor)
    @logger.info "#{self.class.name} writer start"

    values_clause = Array.new

    @processing_lines.each do |outline|

      assay_date         = (outline[9].nil?)   ? 'NULL'   : "STR_TO_DATE('#{outline[9].strip}', '%Y-%m-%d')"
      reported_datettime = (outline[12].nil?)  ? 'NULL'   : "STR_TO_DATE('#{outline[12].strip}', '%Y-%m-%d %k:%i')"

      values_clause <<
          " (#{outline[0].insert_value}"                              + # study_protocol_id
          "  #{outline[1].insert_value}"                              + # site_number  #{outline[6].insert_value}
          "  #{outline[2].insert_value}"                              + # subject_number
          "  STR_TO_DATE('#{outline[3].strip}',  '%Y-%m-%d'),"        + # collection_date
          '  NULL,'                                                   + # visit_name
          "  #{outline[5].insert_value}"                              + # lab_barcode
          "  #{outline[6].insert_value}"                              + # analysis_barcode
          "  #{outline[7].insert_value}"                              + # assay_batch_id
          "  #{outline[8].insert_value}"                              + # exclusion_flag
          "  #{assay_date},"                                          + # assay_date
          "  #{outline[10].insert_value}"                             + # result_repeated
          "  #{outline[11].insert_value}"                             + # replicate_number
          "  #{reported_datettime},"                                  + # reported_datetime
          "  #{outline[13].insert_value}"                             + # reported_rr_high
          "  #{outline[14].insert_value}"                             + # reported_rr_low
          "  #{outline[15].insert_value}"                             + # result_categorical
          "  #{outline[16].insert_value}"                             + # result_categorical_code_list
          "  #{outline[17].insert_value}"                             + # result_category
          "  #{outline[18].insert_value}"                             + # assay_comment
          "  #{outline[19].insert_value}"                             + # result_numeric
          "  #{outline[20].insert_value}"                             + # result_numeric_precision
          "  #{outline[21].insert_value}"                             + # result_type
          "  #{outline[22].insert_value}"                             + # result_units
          "  #{outline[23].insert_value}"                             + # assay_run_id
          "  #{outline[24].insert_value}"                             + # vendor_id
          "  #{outline[25].insert_value}"                             + # analyte
          "  #{outline[26].insert_value}"                             + # assay_code
          "  #{outline[27].insert_value}"                             + # assay_description
          "  #{outline[28].insert_value}"                             + # assay_method
          "  #{outline[29].insert_value}"                             + # assay_name
          "  #{outline[30].insert_value}"                             + # assay_protocol_id
          "  #{outline[31].insert_value}"                             + # assay_protocol_version
          "  #{outline[32].insert_value}"                             + # equipment_used
          "  #{outline[33].insert_value}"                             + # lab_assay_protocol_id
          "  #{outline[34].insert_value}"                             + # lab_assay_protocol_version
          "  #{outline[35].insert_value}"                             + # lab_test_name
          "  #{outline[36].insert_value}"                             + # lab_test_number
          "  #{outline[37].insert_value}"                             + # LOINC_code
          "  #{outline[38].insert_value}"                             + # sample_storage_conditions
          "  #{outline[39].insert_value}"                             + # sensitivity
          "  #{outline[40].insert_value}"                             + # assay_status
          "  #{outline[41].insert_value}"                             + # test_type
		      "  '#{vendor}'"                                             + # vendor_code
		  ')'
    end

    @logger.info "#{self.class.name} writer end"
    values_clause
  end                                                                 
end                                                                   
