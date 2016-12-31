require 'mysql2'

class MLN4924_P1012_site

  COUNTRY_CAPITAL =
  {
      :Argentina       => 'Buenos Aires',		
      :Australia       => 'Canberra',
      :Austria         => 'Vienna',
      :Belgium         => 'Brussles',
      :Brazil          => 'Brasília',
      :Canada          => 'Ottawa',
      :Chile           => 'Santiago',
      :Colombia        => 'Bogotá',
      :CzechRepublic   => 'Prague',
      :Denmark         => 'Copenhagen',
      :France          => 'Paris',
      :Germany         => 'Berlin',
      :Greece          => 'Athens',
      :Hungary         => 'Budapest',
      :Israel          => 'Jerusalem',
      :Italy           => 'Rome',
      :Japan           => 'Tokyo',
      :Mexico          => 'Mexico City',
      :Netherlands     => 'Amsterdam',
      :Norway          => 'Oslo',
      :Poland          => 'Warsaw',
      :Portugal        => 'Lisbon',
      :RepublicofKorea => 'Seoul',
      :Russia          => 'Moscow',
      :Serbia          => 'Belgrade',
      :Singapore       => 'Singapore',
      :SouthAfrica     => 'Cape Town',
      :Spain           => 'Madrid',
      :Sweden          => 'Stockholm',
      :Switzerland     => 'Bern',
      :Taiwan          => 'Taipei',
      :Thailand        => 'Bangkok',
      :Turkey          => 'Ankara',
      :Ukraine         => 'Kiev',
      :UnitedKingdom   => 'London',
      :UnitedStates    => 'Washington'
  }.freeze

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

      if outline[1] == 'United States of America'
        outline[1] = 'United States'
      end

      country_capital = (outline[1].nil?)  ? ' NULL,' : "#{COUNTRY_CAPITAL[(outline[1].gsub(/[^a-zA-Z0-9]/, '')).to_sym]}".insert_value

      values_clause <<
              "('MLN4924-P1012',"                                     + # study_protocol_id
              " #{outline[2].insert_value}"                           + # site_number
              " #{outline[2].insert_value}"                           + # site_name
              ' NULL,'                                                + # site_address
              " #{country_capital}"                                   + # site_city
              ' NULL,'                                                + # site_state
              " #{outline[1].insert_value}"                           + # site_country
              ' NULL,'                                                + # site_postal_code
              ' NULL,'                                                + # site_phone
              ' NULL,'                                                + # site_fax
              ' NULL,'                                                + # site_FPFV
              ' NULL,'                                                + # site_LPLV
              ' NULL,'                                                + # planned_enrollment
              " #{outline[3].insert_value}"                           + # site_PI
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

class MLN4924_P1012_subject

  CAPITAL_CITY =
      {
          :Argentina       => 'Buenos Aires',
          :Australia       => 'Canberra',
          :Austria         => 'Vienna',
          :Belgium         => 'Brussles',
          :Brazil          => 'Brasília',
          :Canada          => 'Ottawa',
          :Chile           => 'Santiago',
          :Colombia        => 'Bogotá',
          :CzechRepublic   => 'Prague',
          :Denmark         => 'Copenhagen',
          :France          => 'Paris',
          :Germany         => 'Berlin',
          :Greece          => 'Athens',
          :Hungary         => 'Budapest',
          :Israel          => 'Jerusalem',
          :Italy           => 'Rome',
          :Japan           => 'Tokyo',
          :Mexico          => 'Mexico City',
          :Netherlands     => 'Amsterdam',
          :Norway          => 'Oslo',
          :Poland          => 'Warsaw',
          :Portugal        => 'Lisbon',
          :RepublicofKorea => 'Seoul',
          :Russia          => 'Moscow',
          :Serbia          => 'Belgrade',
          :Singapore       => 'Singapore',
          :SouthAfrica     => 'Cape Town',
          :Spain           => 'Madrid',
          :Sweden          => 'Stockholm',
          :Switzerland     => 'Bern',
          :Taiwan          => 'Taipei',
          :Thailand        => 'Bangkok',
          :Turkey          => 'Ankara',
          :Ukraine         => 'Kiev',
          :UnitedKingdom   => 'London',
          :UnitedStates    => 'Washington'
      }.freeze

  SUBJECT_GENDER =
      {
          :S29001001 =>'Male',
          :S63002001 =>'Female',
          :S63002002 =>'Male'
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
        if specline[4] == distinct_line[4]
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

      case outline[2]
        when 'United States of America'
          outline[2] = 'United States'
        when 'South Korea'
          outline[2] = 'Republic of Korea'
      end

      subject_treatment   = 'Pevonedistat'.insert_value
      subject_arm         = 'Single Agent Arm 25 mg/m2'.insert_value
      date_of_birth       = (outline[6].nil?)  ? ' NULL,' : "STR_TO_DATE(#{outline[6].insert(-3, '19').insert_value} '%e-%b-%Y'),"
      country_capital     = (outline[1].nil?)  ? ' NULL,' : "#{CAPITAL_CITY[(outline[1].gsub(/[^a-zA-Z0-9]/, '')).to_sym]}".insert_value
      gender              = "#{SUBJECT_GENDER[('S'+outline[4].gsub(/[^a-zA-Z0-9]/, '')).to_sym]}".insert_value

      values_clause <<
              "('MLN4924-P1012',"                                          + # study_protocol_id
              " #{outline[2].insert_value}"                         + # site_number
              " #{outline[4].insert_value}"                         + # subject_code
              " #{outline[4].insert_value}"                         + # subject_external_id
              ' NULL,'                                              + # randomization_number
              " #{gender}"                                          + # gender
              " #{outline[5].insert_value}"                         + # initials
              " #{outline[5].insert_value}"                         + # enrollment_status
              " #{date_of_birth}"                                   + # date_of_birth 1-Jan-34
              ' NULL,'                                              + # address
              " #{country_capital}"                                 + # city
              ' NULL,'                                              + # state
              ' NULL,'                                              + # region
              " #{outline[1].insert_value}"                         + # country
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

class MLN4924_P1012_Inv
  SPECIMEN_HUB = {
      :Argentina       => 'LCUS',
      :Australia       => 'LCAP',
      :Austria         => 'LCEU',
      :Belgium         => 'LCEU',
      :Brazil          => 'LCUS',
      :Canada          => 'LCUS',
      :Chile           => 'LCUS',
      :Colombia        => 'LCUS',
      :CzechRepublic   => 'LCEU',
      :Denmark         => 'LCEU',
      :France          => 'LCEU',
      :Germany         => 'LCEU',
      :Greece          => 'LCEU',
      :Hungary         => 'LCEU',
      :Israel          => 'LCEU',
      :Italy           => 'LCEU',
      :Japan           => 'LCAP',
      :Mexico          => 'LCUS',
      :Netherlands     => 'LCEU',
      :Norway          => 'LCEU',
      :Poland          => 'LCEU',
      :Portugal        => 'LCEU',
      :RepublicofKorea => 'LCAP',
      :Russia          => 'LCEU',
      :Serbia          => 'LCEU',
      :Singapore       => 'LCAP',
      :SouthAfrica     => 'LCEU',
      :Spain           => 'LCEU',
      :Sweden          => 'LCEU',
      :Switzerland     => 'LCEU',
      :Taiwan          => 'LCAP',
      :Thailand        => 'LCAP',
      :Turkey          => 'LCEU',
      :Ukraine         => 'LCEU',
      :UnitedKingdom   => 'LCEU',
      :UnitedStates    => 'LCUS'
  }.freeze

  VISIT_MAP =
  {
    :Screening    => 'Screening',
  }.freeze

  SPECIMEN_TYPE = {
  }.freeze

  SPECIMEN_DESIGNATION = {
  }.freeze

  SPECIMEN_DESIGNATION_DETAIL = {
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

    @processing_lines = Array.new
    lines = 0

    @inbound_lines.each do |specline|                        # Ignore lines with ...
      if specline[9].nil?                  ||                # Empty tube number
         specline[9].strip  == ''          ||                # Missing tube number
         specline[20].strip == 'Cancelled' ||                # Cancelled testing status (tube never arrived)
         specline[20].strip == 'Assigned'                    # Assigned testing status (tube has not arrived yet)
        next
      end

      found = false
      @processing_lines.each do |distinct_line|

        # see if we actually need the line in the file
        if (specline[0] == distinct_line[0])  &&     # Accession Number
           (specline[9] == distinct_line[9])         # Tube Number
          found = true

          if !specline[21].nil? && specline[21][0..4] == 'Check'
            distinct_line[21] = specline[21]
          end

          break
        end
      end

      if !found
        @processing_lines << specline
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

      treatment           = 'Ixazomib'
      arm                 = 'Ixazomib or Placebo'
      date_of_birth       = (outline[6].nil?)  ? ' NULL,' : "STR_TO_DATE(#{outline[6].insert(-3, '19').insert_value} '%e-%b-%Y'),"
      visit               = VISIT_MAP[(outline[16].gsub(/[^a-zA-Z0-9]/, '')).to_sym]
      specimen_testname   = outline[19].nil? ? '' : outline[19].gsub(/[.]/,'0')
      specimen_type       = SPECIMEN_TYPE[('S'+(((outline[18].nil? ? '' : outline[18]) + specimen_testname).gsub(/[^a-zA-Z0-9]/, ''))).to_sym]
      specimen_designation= SPECIMEN_DESIGNATION[('S'+(((outline[18].nil? ? '' : outline[18]) + specimen_testname).gsub(/[^a-zA-Z0-9]/, ''))).to_sym]
      specimen_ddetals    = SPECIMEN_DESIGNATION_DETAIL[('S'+(((outline[18].nil? ? '' : outline[18]) + specimen_testname).gsub(/[^a-zA-Z0-9]/, ''))).to_sym]
      receive_date        = (outline[15].nil?)  ? ' NULL,' : " STR_TO_DATE(#{outline[15].insert_value} '%c/%e/%Y %T'),"
      specimen_barcode    = outline[0] + '-' + outline[9].rjust(2,'0')
      specimen_hub        = SPECIMEN_HUB[(outline[3].gsub(/[^a-zA-Z0-9]/, '')).to_sym]
      specimen_status     = 'Exhausted'
      specimen_location   = specimen_hub
      specimen_is_child   = 'No'
      specimen_parent     = 'NULL,'

# Figure our lineage

      if !outline[11].nil?
        specimen_is_child = 'Yes'
        specimen_parent   = (outline[0] + '-' + outline[11].rjust(2,'0')).insert_value
      end

# set specimen status and vendor code base upon visit and tube mapping.

      case visit
        when 'Cycle 10 Day 1',
             'Cycle 2 Day 1',
             'Cycle 3 Day 1',
             'Cycle 4 Day 1',
             'Cycle 5 Day 1',
             'Cycle 6 Day 1',
             'Cycle 8 Day 1',
             'Cycle 9 Day 1'
          case outline[9]
            when '1', '2', '4', '5', '6', '7', '15', '17', '18', '26', '27', '47'                    #default
            when '27', '26'
              if !outline[21].nil? && outline[21][0..8] == 'Check Out'
                specimen_location = 'SYNC'
              end
              specimen_status   = 'In Storage'
            when '6'
              if !outline[21].nil? && outline[21][0..7] == 'Check In'
                specimen_status   = 'In Storage'
              end
            else
              specimen_location = "UNKNOWN TUBE #{outline[10]}"
              @logger.error "0 Tube ->#{outline[9]}<- not known for visit ->#{visit}<-."
          end
        when 'Cycle 11 Day 1',
             'Cycle 12 Day 1',
             'Cycle 14 Day 1',
             'Cycle 15 Day 1',
             'Progressive Disease (PD)'
          case outline[9]
            when '1', '2', '4', '5', '6', '7', '15', '17', '18', '47'                               #default
            when '6'
              if !outline[21].nil? && outline[21][0..7] == 'Check In'
                specimen_status   = 'In Storage'
              end
            else
              specimen_location = "UNKNOWN TUBE #{outline[10]}"
              @logger.error "0 Tube ->#{outline[9]}<- not known for visit ->#{visit}<-."
          end
        when 'Cycle 1 Day 1'
          case outline[9]
            when '1', '2', '4', '5', '7', '8', '15', '17', '18', '47'                                     #default
            when '26', '27', '28', '29', '30'
               specimen_location = 'LCUS'
               if !outline[18].nil? && !outline[20].nil? && outline[20][0..8] == "Check Out"
                 specimen_location = 'QPS'
               end
               specimen_status   = 'In Storage'
            when '6'
              if !outline[21].nil? && outline[21][0..7] == 'Check In'
                specimen_status   = 'In Storage'
              end
            else
              specimen_location = "UNKNOWN TUBE #{outline[10]}"
              @logger.error "0 Tube ->#{outline[9]}<- not known for visit ->#{visit}<-."
          end
        when 'Cycle 13 Day 1'
          case outline[9]
            when '1', '2', '4', '5', '6', '7', '11', '12', '13', '15', '17', '18', '24', '47'       #default
            when '25'
              specimen_location = 'LCUS'
              if !outline[18].nil? && !outline[20].nil? && outline[20][0..8] == "Check Out"
                specimen_location = 'ADPT'
              end
              specimen_status   = 'In Storage'
            when '6'
              if !outline[21].nil? && outline[21][0..7] == 'Check In'
                specimen_status   = 'In Storage'
              end
            else
              specimen_location = "UNKNOWN TUBE #{outline[10]}"
              @logger.error "0 Tube ->#{outline[9]}<- not known for visit ->#{visit}<-."
            end
        when 'Cycle 1 Day 15',
             'Cycle 1 Day 8',
             'Cycle 2 Day 8',
             'Cycle 5 Day 8'
          case outline[9]
            when '1', '2'                                                                           #default
            when '26', '27'
              specimen_location = 'LCUS'
              if !outline[18].nil? && !outline[20].nil? && outline[20][0..8] == "Check Out"
                specimen_location = 'QPS'
              end
              specimen_status   = 'In Storage'
            else
              specimen_location = "UNKNOWN TUBE #{outline[10]}"
              @logger.error "0 Tube ->#{outline[9]}<- not known for visit ->#{visit}<-."
          end
        when 'Cycle 7 Day 1',
             'Progression-Free Survival 2 (PFS2)'
          case outline[9]
            when '1', '2', '4', '5', '7', '11', '12', '13', '15', '17', '18', '47'                  #default
            when '26', '27'
              specimen_location = 'LCUS'
              if !outline[18].nil? && !outline[20].nil? && outline[20][0..8] == "Check Out"
                specimen_location = 'QPS'
              end
              specimen_status   = 'In Storage'
            when '6'
              if !outline[21].nil? && outline[21][0..7] == 'Check In'
                specimen_status   = 'In Storage'
              end
            else
              specimen_location = "UNKNOWN TUBE #{outline[10]}"
              @logger.error "0 Tube ->#{outline[9]}<- not known for visit ->#{visit}<-."
          end
        when 'Complete Response'
          case outline[9]
            when '1'                                                                              #default
            when '2'
              specimen_location = 'LCUS'
              if !outline[18].nil? && !outline[20].nil? && outline[20][0..8] == "Check Out"
                specimen_location = 'ADPT'
              end
              specimen_status   = 'In Storage'
            else
              specimen_location = "UNKNOWN TUBE #{outline[10]}"
              @logger.error "0 Tube ->#{outline[9]}<- not known for visit ->#{visit}<-."
          end
        when 'End of Treatment'
          case outline[9]
            when '1', '2', '4', '5', '7', '11', '12', '13', '15', '17', '18', '24', '31', '47'                                     #default
            when '34'
              specimen_location = 'LCUS'
              if !outline[18].nil? && !outline[20].nil? && outline[20][0..7] == "Check In"
                specimen_status = 'In Storage'
              end
            when '35'
              specimen_location = 'LCUS'
              if !outline[18].nil? && !outline[20].nil? && outline[20][0..8] == "Check Out"
                specimen_location = 'ADPT'
              end
              specimen_status  = 'In Storage'
            when '6'
              if !outline[21].nil? && outline[21][0..7] == 'Check In'
                specimen_status   = 'In Storage'
              end
            else
              specimen_location = "UNKNOWN TUBE #{outline[10]}"
              @logger.error "0 Tube ->#{outline[9]}<- not known for visit ->#{visit}<-."
          end
        when 'Pre-screening'
          case outline[9]
            when '1'
            when '3', '4', '5', '6', '7', '8', '9', '10'                   #default
              if !outline[21].nil? && outline[21][0..8] == 'Check Out'
                specimen_location = 'ADPT'
              end
              specimen_status   = 'In Storage'
            when '2'
              if !outline[21].nil? && outline[21][0..7] == 'Check In'
                specimen_status   = 'In Storage'
              end
            else
              specimen_location = "UNKNOWN TUBE #{outline[10]}"
              @logger.error "0 Tube ->#{outline[9]}<- not known for visit ->#{visit}<-."
          end
        when 'Screening'
          case outline[9]
            when '1', '2', '4', '5', '7', '8', '9', '11', '12', '13', '15', '17', '18', '24', '47'    #default
            when '22', '23'
              if !outline[21].nil? && outline[21][0..8] == 'Check Out'
                specimen_location = 'BRIT'
              else
                specimen_location = 'LCUS'
              end
              specimen_status   = 'In Storage'
            when '6'
              if !outline[21].nil? && outline[21][0..7] == 'Check In'
                specimen_status   = 'In Storage'
              end
            when '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40',
                  '41', '42', '43', '44', '45', '46'
              #default
              if !outline[21].nil? && outline[21][0..8] == 'Check Out'
                specimen_location = 'ADPT'
              else
                specimen_location = 'LCRT'
              end
              specimen_status   = 'In Storage'
            when '21'
              if !outline[21].nil? && outline[21][0..7] == 'Check In'
                specimen_status   = 'In Storage'
              end
            else
              specimen_location = "UNKNOWN TUBE #{outline[10]}"
              @logger.error "0 Tube ->#{outline[9]}<- not known for visit ->#{visit}<-."
          end
        when 'Unscheduled'
          case outline[9]
            when '1', '4', '5', '7', '9', '11', '12', '13', '15', '17', '18', '24', '47', '97'
            when '2'
              if specimen_type == 'Enriched Plasma Cells'
                specimen_status = 'In Storage'
              end
            when '2'
              if !outline[21].nil? && outline[21][0..7] == 'Check In'
                specimen_status   = 'In Storage'
              end
            when '6'
              if !outline[21].nil? && outline[21][0..7] == 'Check In'
                specimen_status   = 'In Storage'
              end
            when '21'
              specimen_location = 'LCUS'
              if !outline[21].nil? && outline[21][0..7] == 'Check In'
                specimen_status   = 'In Storage'
              end
            when '22', '23'
              specimen_status   = 'In Storage'
              if outline[21].nil? && outline[21][0..8] == 'Check Out'
                specimen_location = 'BRIT'
              else
                specimen_location = 'LCUS'
              end
            when '26', '27'
              specimen_status   = 'In Storage'
              if outline[21].nil? && outline[21][0..8] == 'Check Out'
                specimen_location = 'QPS'
              end
            else
              specimen_location = "UNKNOWN TUBE #{outline[10]}"
              @logger.error "0 Tube ->#{outline[9]}<- not known for visit ->#{visit}<-."
          end
        else
          specimen_location = "UNKNOWN VISIT #{visit}"
          @logger.error "Visit ->#{visit}<- not known."
      end

      values_clause <<
              "('C16021',"                                               + # study_protocol_id
              " #{outline[2].rjust(5, '0').insert_value}"                + # site_number
              " #{outline[4].insert_value}"                              + # subject_code
              " #{outline[7].insert_value}"                              + # subject_gender
              " #{date_of_birth}"                                        + # subject_DOB
              " STR_TO_DATE(#{outline[8].insert_value} '%c/%e/%Y %H:%i'),"  + # specimen_collect_date
              " STR_TO_DATE(#{outline[8].insert_value} '%c/%e/%Y %H:%i'),"  + # specimen_collect_time
              " #{receive_date}"                                         + # specimen_receive_datetime
              " #{treatment.insert_value}"                               + # treatment
              " #{arm.insert_value}"                                     + # arm
              " #{visit.insert_value}"                                   + # visit_name
              " #{specimen_barcode.insert_value}"                        + # specimen_barcode
              " #{specimen_barcode.insert_value}"                        + # specimen_identifier
              " #{specimen_type.insert_value}"                           + # specimen_type
              " #{specimen_barcode.insert_value}"                        + # specimen_name
              " #{specimen_designation.insert_value}"                    + # specimen_designation,
              " #{specimen_ddetals.insert_value}"                        + # specimen_designation_details
              " #{specimen_parent}"                                      + # specimen_parent
              " #{specimen_is_child.insert_value}"                       + # specimen_ischild
              ' NULL,'                                                   + # specimen_condition
              " #{specimen_status.insert_value}"                         + # specimen_status
              " #{outline[23].insert_value}"                             + # specimen_comment
              ' NULL,'                                                   + # shipped_date
              " #{specimen_location.insert_value}"                       + # shipped_location
              ' NULL,'                                                   + # testing_description
              " '#{vendor}'"                                             + # vendor_code
              ")"
    end

    @logger.info "#{self.class.name} writer end"
    values_clause
  end
end
