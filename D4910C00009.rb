require 'mysql2'

class D4910C00009_AssayGrp
  def initialize(logger)
    @logger = logger
    @logger.debug "#{self.class.name} filer initialized"
  end

  def reader(inbound_file)
    @logger.debug "File name is ->#{inbound_file}<-"
    @logger.info "#{self.class.name} reader start"
    @inbound_lines = CSV.read(inbound_file, headers: true, skip_blanks: true, :quote_char => '"', skip_lines: '\r', encoding:'windows-1256:utf-8')
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
        # Check if this is an amdin question test code ADTxxx.  If so set grp to "Administrative Questions"
        if inline[1][0..2] == 'ADT' or inline[1][0..2] == 'AMT'
          inline[4] = 'Administrative Questions'
        end

        # see if we actually need the line in the file ... assay group already seen, then ignore
        if inline[4] == distinct_line[4]
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
          " (#{outline[4].insert_value}"                           + # Assay Group Name
          '  1'                                                   + # Assay Group Version
          ' )'
    end

    @logger.info "#{self.class.name} writer end"
    values_clause
  end
end

class D4910C00009_AssayDef
  def initialize(logger)
    @logger = logger
    @logger.debug "#{self.class.name} filer initialized"

    @master_program_id = 0
    @vendor_id = 0
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

    begin
      master_program = this_connection.query("SELECT mp.master_program_id  FROM master_protocols mp WHERE mp.name = '#{@inbound_lines.first[0].strip}';")
    rescue Mysql2::Error => e
      @logger.error "DB Master Program Select failure - #{e.message}"
      exit -1
    end

    @master_program_id = master_program.first['master_program_id']

    begin
      vendor = this_connection.query("SELECT v.id  FROM vendors v WHERE v.data_feed_code = 'CVD'")
    rescue Mysql2::Error => e
      @logger.error "DB Vendor Select failure - #{e.message}"
      exit -1
    end

    @vendor_id = vendor.first['id']

    @inbound_lines.each do |inline|
      found = false
      lines += 1

      @processing_lines.each do |distinct_line|

        # see if we actually need the line in the file ... assay code already seen, then ignore
        if inline[1] == distinct_line[1]
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

    if @vendor_id == 0
      @logger.info {"Unknown vendor code"}
      exit 0
    end

    if @master_program_id == 0
      @logger.info ("Unknown master program or protocol")
      exit 0
    end

    values_clause = Array.new

    @processing_lines.each do |outline|

      values_clause <<
          " (#{outline[1].insert_value}"                           + # Assay Code
              "  #{outline[2].insert_value}"                       + # Assay Name
              "  'Active',"                                        + # Assay Status
              "  #{@vendor_id},"                                   + # Vendor
              "  #{@master_program_id}"                            + # Master Program
          ' )'
    end

    @logger.info "#{self.class.name} writer end"
    values_clause
  end
end

class D4910C00009_AssayGrpAssayDef
  def initialize(logger)
    @logger = logger
    @logger.debug "#{self.class.name} filer initialized"

    @assayGrps = Array.new
    @assays    = Array.new
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

    begin
      assay_grps = this_connection.query("SELECT ag.id, ag.name  FROM assay_groups ag;")
    rescue Mysql2::Error => e
      @logger.error "DB Assay Group Select failure - #{e.message}"
      exit -1
    end

    assay_grps.each { |r| @assayGrps.push(r) }

    begin
      assays = this_connection.query("SELECT a.id, a.code  FROM assays a;")
    rescue Mysql2::Error => e
      @logger.error "DB Assay Group Select failure - #{e.message}"
      exit -1
    end

    assays.each { |r| @assays.push(r) }

    @inbound_lines.each do |inline|
      found = false
      lines += 1

      @processing_lines.each do |distinct_line|
        # Check if this is an amdin question test code ADTxxx or AMTxxx.  If so set grp to "Administrative Questions"
        if inline[1][0..2] == 'ADT' or inline[1][0..2] == 'AMT'
          inline[4] = 'Administrative Questions'
        end

        # see if we actually need the line in the file ... assay code already seen, then ignore
        if inline[1] == distinct_line[1]
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

    values_clause = Array.new

    @processing_lines.each do |outline|

      values_clause <<
          ' ('                                                              +
          "  #{@assays[@assays.find_index {|i| i['code'] == outline[1].strip}]['id']},"       + # Assay Code id
          "  #{@assayGrps[@assayGrps.find_index {|i| i['name'] == outline[4].strip}]['id']}"  + # Assay Group id
          ' )'
    end

    @logger.info "#{self.class.name} writer end"
    values_clause
  end
end


class D4910C00009_Site

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
        if specline[1] == distinct_line[1]
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
          "(#{outline[0].insert_value}"                               + # study_protocol_id
              " #{outline[1].insert_value}"                           + # site_number
              " #{outline[2].insert_value}"                           + # site_name
              " #{outline[3].insert_value}"                           + # site_address
              " #{outline[4].insert_value}"                           + # site_city
              " #{outline[5].insert_value}"                           + # site_state
              " #{outline[6].insert_value}"                           + # site_country
              " #{outline[7].insert_value}"                           + # site_postal_code
              " #{outline[8].insert_value}"                           + # site_phone
              " #{outline[9].insert_value}"                           + # site_fax
              ' NULL,'                                                + # site_FPFV
              ' NULL,'                                                + # site_LPLV
              ' NULL,'                                                + # planned_enrollment
              " #{outline[13].insert_value}"                          + # site_PI
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

class D4910C00009_Subject

  SUBJECT_GENDER =
      {
          :M =>'Male',
          :F =>'Female',
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
    @logger.info "#{self.class.name} processor"

    @processing_lines = Array.new
    lines = 0
    num_distinct_lines = 0

    @inbound_lines.each do |specline|
      found = false
      lines += 1

      @processing_lines.each do |distinct_line|

        # see if we actually need the line in the file ... Subject Id already seen, then ignore
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

      subject_treatment   = 'Treatment'.insert_value
      subject_arm         = 'Arm'.insert_value
      date_of_birth       = (outline[6].nil?)  ? ' NULL,' : "STR_TO_DATE(#{outline[6].insert_value} '%Y-%m-%d'),"
      gender              = "#{SUBJECT_GENDER[(outline[4].gsub(/[^a-zA-Z0-9]/, '')).to_sym]}".insert_value

      values_clause <<
          "(#{outline[0].insert_value}"                             + # study_protocol_id
              " #{outline[1].insert_value}"                         + # site_number
              " #{outline[2].insert_value}"                         + # subject_code
              ' NULL,'                                              + # subject_external_id
              ' NULL,'                                              + # randomization_number
              " #{gender}"                                          + # gender
              ' NULL,'                                              + # initials
              " 'Randomized',"                                      + # enrollment_status
              " #{date_of_birth}"                                   + # date_of_birth 1-Jan-34
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
              ' NULL,'                                              + # ICF_signing_date
              ' NULL,'                                              + # ICF_withdrawl_date
              " '#{vendor}'"                                        + # vendor_code
              ")"
    end

    @logger.info "#{self.class.name} writer end"
    values_clause
  end
end

class D4910C00009_Inventory
  VISIT_MAP = {
      :VISIT1               => 'Visit 1',
      :PKINTENSIVE          => 'Visit 5',
      :VISIT2               => 'Visit 2 (Baseline)',
      :VISIT3               => 'Visit 3',
      :VISIT5               => 'Visit 5',
      :VISIT8               => 'Visit 8',
      :VISIT10              => 'Visit 10',
      :VISIT11              => 'Visit 11',
      :VISIT16EOT           => 'Visit 16',
      :VISIT17TOC           => 'Visit 17',
      :VISIT18LFU           => 'Visit 18',
      :RETESTUNSCHEDULED    => 'Retest/Unscheduled',
  }.freeze

  SPECIMEN_STATUS = {
      :A     => 'In Storage',
      :P     => 'In Transit',
      :D     => 'Exhausted',
      :R     => 'In Inventory',
  }.freeze

  SPECIMEN_CONDITION = {
      :SCC09  => 'No specimen received',
      :SCC10  => 'Specimen received beyond stability',
      :SCC26  => 'Specimen received at ambient temperature',
      :SCC28  => 'Frozen specimen received',
      :SCC293 => 'Mishandled by Covance',
      :SCC475 => 'Error in specimen ID assignment',
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
    mapspec = CVD_spectype.new(@logger, this_connection)

    @processing_lines = Array.new

    @inbound_lines.each do |specline|
      if specline[11].end_with? '-0'
        next
      end

      specimen_status    = (specline[18].nil?)  ? " 'Exhausted'," : SPECIMEN_STATUS[specline[18].gsub(/[^a-zA-Z0-9]/, '').to_sym].insert_value

      if specimen_status.nil?
        @logger.info '================= QUERY NEED ===================='
        @logger.info "Unknown specimen status ->#{specline[18]}<- specimen id ->#{specline[11]}<-"
        @logger.info "Full record ->#{specline}<-"
      end

      if specline[13].nil? || specline[13] == ''
        @logger.info '================= QUERY NEED ===================='
        @logger.info "Missing specimen type on specimen id ->#{specline[11]}<-"
        @logger.info "Full record ->#{specline}<-"
      else
        spectype = mapspec.get_spectype (specline[13].strip)

        if  spectype.nil? || spectype == ''
          @logger.info '================= QUERY NEED ===================='
          @logger.info "Unknown specimen type ->#{specline[13]}<- on specimen id ->#{specline[11]}<-"
          @logger.info "Full record ->#{specline}<-"
        else
          specline[13] = spectype
        end
      end

      if specline[25].nil? || specline[25] == ''
        @logger.info '================= QUERY NEED ===================='
        @logger.info "RECORD EXCLUDED - visit_name missing ->#{specline[25]}<- specimen id ->#{specline[11]}<-"
        @logger.info "Full record ->#{specline}<-"
        next
      else
        visit_name         = VISIT_MAP[specline[25].gsub(/[^a-zA-Z0-9]/, '').to_sym]

        if visit_name.nil? || visit_name == ''
          @logger.info '================= QUERY NEED ===================='
          @logger.info "RECORD EXCLUDED - Unknown visit_name ->#{specline[25]}<- specimen id ->#{specline[11]}<-"
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

      visit_name         = (outline[25].nil?)             ? ' NULL,'                  : VISIT_MAP[outline[25].gsub(/[^a-zA-Z0-9]/, '').to_sym].insert_value
      visit_date         = (outline[23].nil?)             ? ' NULL,'                  : "STR_TO_DATE(#{outline[23].insert_value} '%Y-%m-%d'),"
      subject_gender     = (outline[9].strip == 'F')      ? 'Female'.insert_value     : 'Male'.insert_value
      date_of_birth      = (outline[6].nil?)              ? ' NULL,'                  : "STR_TO_DATE(#{outline[6].insert_value} '%Y-%m-%d'),"
      subject_enrollment = (outline[9].strip == 'E')      ? 'Randomized'.insert_value : 'Randomized'.insert_value
      collection_date    = (outline[19].nil?)             ? ' NULL,'                  : "STR_TO_DATE(#{outline[19].insert_value} '%Y-%m-%d'),"
      collection_time    = (outline[21].nil?)             ? ' NULL,'                  : "STR_TO_DATE(#{outline[21].insert_value} '%k:%i'),"
      received_date      = (outline[36].nil?)             ? ' NULL,'                  : "STR_TO_DATE(#{outline[36].insert_value} '%Y-%m-%d'),"
      specimen_shipdate  = (outline[37].nil?)             ? ' NULL,'                  : "STR_TO_DATE(#{outline[37].insert_value} '%Y-%m-%d'),"
      specimen_condition = (outline[22].nil?)             ? ' NULL,'                  : SPECIMEN_CONDITION[outline[22].gsub(/[^a-zA-Z0-9]/, '').to_sym].insert_value
      specimen_status    = (outline[18].nil?)             ? " 'Exhausted',"           : SPECIMEN_STATUS[outline[18].gsub(/[^a-zA-Z0-9]/, '').to_sym].insert_value
      designation        = ((!outline[15].nil?) &&
                            (outline[15].start_with? 'SM')) ? "'PK',"                 : ' NULL,'

      values_clause <<
          " (#{outline[0].insert_value}"                                  + # study_protocol_id
              ' 1,'                                                       + # study_protocol_version
              " #{outline[2].insert_value}"                               + # site_number,
              " #{outline[3].insert_value}"                               + # collection_site_country,
              ' NULL,'                                                    + # date_departed_site,
              " #{outline[4].insert_value}"                               + # subject_num,
              " #{subject_gender}"                                        + # subject_gender,
              " #{date_of_birth}"                                         + # subject_date_of_birth,
              ' NULL,'                                                    + # subject_race,
              ' NULL,'                                                    + # subject_ethnicity,
              " #{subject_enrollment}"                                    + # subject_enrollment_status,
              " #{collection_date}"                                       + # specimen_collect_date,
              " #{collection_time}"                                       + # specimen_collect_time,
              " #{received_date}"                                         + # received_datetime,
              " 'Treatment',"                                             + # treatment,
              " 'Arm',"                                                   + # arm,
              " #{visit_name}"                                            + # visit_name,
              " #{visit_date}"                                            + # visit_date,
              ' NULL,'                                                    + # visit_day,
              ' NULL,'                                                    + # visit_week,
              ' NULL,'                                                    + # visit_timepoint,
              " #{outline[12].insert_value}"                              + # specimen_barcode,
              " #{outline[11].insert_value}"                              + # specimen_identifier,
              " #{outline[13].insert_value}"                              + # specimen_type,
              " #{outline[11].insert_value}"                              + # specimen_name,
              ' NULL,'                                                    + # specimen_volume,
              ' NULL,'                                                    + # specimen_volume_units,
              ' NULL,'                                                    + # specimen_mass,
              ' NULL,'                                                    + # specimen_mass_units,
              " #{designation}"                                           + # specimen_designation,
              " #{outline[15].insert_value}"                              + # specimen_designation_detail,
              ' NULL,'                                                    + # specimen_collection_tube_type,
              ' NULL,'                                                    + # specimen_quality,
              ' NULL,'                                                    + # specimen_parent_id,
              ' NULL,'                                                    + # specimen_ischild,
              ' NULL,'                                                    + # specimen_child_type,
              " #{specimen_condition}"                                    + # specimen_condition,
              " #{specimen_status}"                                       + # specimen_status,
              " #{outline[20].insert_value}"                              + # specimen_comment,
              " #{specimen_shipdate}"                                     + # shipped_date,
              " #{outline[38].insert_value}"                              + # shipped_location,
              " #{outline[39].insert_value}"                              + # storage_vendor,
              " #{outline[40].insert_value}"                              + # storage_facility_name,
              " #{outline[41].insert_value}"                              + # storage_location,
              ' NULL,'                                                    + # testing_description,
              " '#{vendor}'"                                              + # vendor_code
              ')'
    end

    @logger.info "#{self.class.name} writer end"
    values_clause
  end
end

class D4910C00009_Assay
  VISIT_MAP = {
      :VISIT1               => 'Visit 1',
      :PKINTENSIVE          => 'Visit 5',
      :VISIT2               => 'Visit 2 (Baseline)',
      :VISIT3               => 'Visit 3',
      :VISIT5               => 'Visit 5',
      :VISIT8               => 'Visit 8',
      :VISIT10              => 'Visit 10',
      :VISIT11              => 'Visit 11',
      :VISIT16EOT           => 'Visit 16',
      :VISIT17TOC           => 'Visit 17',
      :VISIT18LFU           => 'Visit 18',
      :RETESTUNSCHEDULED    => 'Retest/Unscheduled',
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

      if specline[8].end_with? '00'
        next
      end

      if (specline[11].nil? || specline[11] == '') && (specline[12].nil? || specline[12] == '')
        @logger.info '================= QUERY NEED ===================='
        @logger.info "RECORD EXCLUDED - no assay result supplied for specimen id ->#{specline[8]}<-"
        @logger.info "Full record ->#{specline}<-"
        next
      end

      if specline[7].nil? || specline[7] == ''
        @logger.info '================= QUERY NEED ===================='
        @logger.info "RECORD EXCLUDED - visit_name missing ->#{specline[25]}<- specimen id ->#{specline[8]}<-"
        @logger.info "Full record ->#{specline}<-"
        next
      else
        visit_name         = VISIT_MAP[specline[7].gsub(/[^a-zA-Z0-9]/, '').to_sym]

        if visit_name.nil? || visit_name == ''
          @logger.info '================= QUERY NEED ===================='
          @logger.info "RECORD EXCLUDED - Unknown visit_name ->#{specline[7]}<- specimen id ->#{specline[8]}<-"
          @logger.info "Full record ->#{specline}<-"
          next
       end
      end

      if specline[10].nil? || specline[10] == ''
        @logger.info '================= QUERY NEED ===================='
        @logger.info "assay_date missing ->#{specline[25]}<- specimen id ->#{specline[8]}<-"
        @logger.info "Full record ->#{specline}<-"
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

      visit_name     = (outline[7].nil?)    ? 'NULL,' : VISIT_MAP[outline[7].gsub(/\s+/, '').to_sym].insert_value
      assay_date     = (outline[10].nil?)   ? 'NULL'  : "STR_TO_DATE('#{outline[10].strip}', '%Y-%m-%d')"
      result_numeric = (outline[12].nil?)   ? 'NULL,' : " #{outline[12]},"

      values_clause <<
          " (#{outline[0].insert_value}"                              + # study_protocol_id,
              "  #{outline[1].insert_value}"                              + # assay_protocol_id,
              "  #{outline[2].insert_value}"                              + # assay_protocol_version,
              "  #{outline[3].insert_value}"                              + # assay_name,
              "  #{outline[4].insert_value}"                              + # assay_code,
              "  #{outline[5].insert_value}"                              + # subject_number,
              "STR_TO_DATE('#{outline[6].strip}', '%Y-%m-%d'),"           + # collection_date,
              "  #{visit_name}"                                           + # visit_name,
              "  #{outline[8].insert_value}"                              + # lab_barcode,
              "  #{outline[9].insert_value}"                              + # analysis_barcode,
              "  #{assay_date},"                                          + # assay_date,
              "  #{outline[11].insert_value}"                             + # result_categorical,
              "  #{result_numeric}"                                       + # result_numeric,
              "  #{outline[13].insert_value}"                             + # result_type,
              "  #{outline[14].insert_value}"                             + # result_units,
              "  #{outline[16].insert_value}"                             + # assay_batch_id,
              "  #{outline[17].insert_value}"                             + # assay_run_id,
              "  #{outline[18].insert_value}"                             + # result_flag
              "  #{outline[19].insert_value}"                             + # result_comment,
              "  '#{vendor}'"                                             + # vendor_code
              ')'
    end

    @logger.info "#{self.class.name} writer end"
    values_clause
  end
end
