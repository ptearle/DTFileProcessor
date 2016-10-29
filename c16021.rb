require 'mysql2'

class C16021_site

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
      :Serbia           => 'Belgrade',
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
              "('C16021',"                                            + # study_protocol_id
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

class C16021_subject

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
          :S01103001 =>'Female',
          :S01103002 => 'Male',
          :S01105001 => 'Male',
          :S02101001 => 'Male',
          :S02102001 => 'Female',
          :S02103001 => 'Female',
          :S02103002 => 'Male',
          :S02103003 => 'Male',
          :S02103004 => 'Female',
          :S02103005 => 'Female',
          :S02103006 => 'Male',
          :S02103007 => 'Female',
          :S02103008 => 'Female',
          :S02103009 => 'Female',
          :S02103010 => 'Female',
          :S03102001 => 'Female',
          :S05115001 => 'Female',
          :S05124001 => 'Male',
          :S07101001 => 'Female',
          :S07101002 => 'Male',
          :S07103001 => 'Female',
          :S07103002 => 'Male',
          :S07103003 => 'Male',
          :S08102001 => 'Female',
          :S08102002 => 'Female',
          :S08102003 => 'Male',
          :S08102004 => 'Female',
          :S08103001 => 'Female',
          :S08103002 => 'Female',
          :S08103003 => 'Male',
          :S10103001 => 'Female',
          :S12101001 => 'Male',
          :S12101002 => 'Male',
          :S12101003 => 'Male',
          :S12101004 => 'Male',
          :S12101005 => 'Female',
          :S12101006 => 'Female',
          :S12101007 => 'Male',
          :S12102001 => 'Female',
          :S12102002 => 'Female',
          :S12102003 => 'Male',
          :S12102004 => 'Male',
          :S12103001 => 'Male',
          :S12103002 => 'Female',
          :S12103003 => 'Female',
          :S12103004 => 'Female',
          :S12103005 => 'Male',
          :S12103006 => 'Male',
          :S12104001 => 'Male',
          :S12104002 => 'Male',
          :S12104003 => 'Female',
          :S12104004 => 'Male',
          :S12105001 => 'Female',
          :S12105002 => 'Female',
          :S12106001 => 'Male',
          :S12106002 => 'Male',
          :S12106003 => 'Female',
          :S12106004 => 'Female',
          :S12902013 => 'Female',
          :S13101001 => 'Male',
          :S13102001 => 'Male',
          :S13102002 => 'Male',
          :S13102003 => 'Male',
          :S13103001 => 'Male',
          :S13104001 => 'Female',
          :S13104002 => 'Female',
          :S19104001 => 'Male',
          :S19109001 => 'Male',
          :S20102001 => 'Female',
          :S20102002 => 'Female',
          :S20103001 => 'Male',
          :S20103002 => 'Female',
          :S20103003 => 'Female',
          :S20103004 => 'Male',
          :S20103005 => 'Male',
          :S20103006 => 'Male',
          :S20103007 => 'Male',
          :S20103008 => 'Male',
          :S20103009 => 'Male',
          :S20103010 => 'Male',
          :S20103011 => 'Female',
          :S20103012 => 'Female',
          :S20103013 => 'Female',
          :S20103014 => 'Male',
          :S20103015 => 'Female',
          :S20103016 => 'Male',
          :S20103017 => 'Male',
          :S20103018 => 'Male',
          :S20103019 => 'Female',
          :S20104001 => 'Female',
          :S20104002 => 'Female',
          :S20104003 => 'Male',
          :S20104004 => 'Male',
          :S20104005 => 'Male',
          :S22102001 => 'Male',
          :S22102002 => 'Male',
          :S22102003 => 'Male',
          :S22103001 => 'Female',
          :S27101001 => 'Female',
          :S27102001 => 'Male',
          :S27102002 => 'Male',
          :S27102003 => 'Male',
          :S27102004 => 'Female',
          :S27104001 => 'Female',
          :S27105001 => 'Female',
          :S27107001 => 'Female',
          :S27108001 => 'Female',
          :S27110001 => 'Male',
          :S27110002 => 'Female',
          :S28101001 => 'Male',
          :S28101003 => 'Female',
          :S28101004 => 'Male',
          :S28102001 => 'Male',
          :S28103001 => 'Male',
          :S28103002 => 'Male',
          :S28103003 => 'Female',
          :S28103004 => 'Male',
          :S28107001 => 'Male',
          :S28109001 => 'Male',
          :S28109002 => 'Female',
          :S28110001 => 'Male',
          :S28110002 => 'Male',
          :S28110003 => 'Male',
          :S28110004 => 'Male',
          :S28110005 => 'Male',
          :S28111001 => 'Male',
          :S28112001 => 'Male',
          :S28112002 => 'Male',
          :S28112003 => 'Female',
          :S28112004 => 'Male',
          :S28112005 => 'Female',
          :S28112006 => 'Male',
          :S28112007 => 'Female',
          :S28112008 => 'Male',
          :S28113001 => 'Male',
          :S29101001 => 'Female',
          :S29101002 => 'Male',
          :S29101003 => 'Female',
          :S29102001 => 'Male',
          :S29102002 => 'Female',
          :S29102003 => 'Female',
          :S29103001 => 'Male',
          :S29104001 => 'Male',
          :S29105001 => 'Male',
          :S29106001 => 'Female',
          :S35101001 => 'Female',
          :S35101002 => 'Female',
          :S42105001 => 'Male',
          :S42105P01 => 'Female',
          :S43101001 => 'Female',
          :S43101002 => 'Female',
          :S43102001 => 'Male',
          :S43103001 => 'Male',
          :S43104001 => 'Female',
          :S43105001 => 'Female',
          :S43105002 => 'Male',
          :S43105003 => 'Male',
          :S43105004 => 'Male',
          :S43106001 => 'Male',
          :S46102001 => 'Male',
          :S47101001 => 'Female',
          :S47101002 => 'Male',
          :S47101P06 => 'Female',
          :S47102001 => 'Male',
          :S47102002 => 'Female',
          :S47102003 => 'Female',
          :S47102004 => 'Female',
          :S47102005 => 'Female',
          :S47104001 => 'Female',
          :S48101001 => 'Female',
          :S48101002 => 'Female',
          :S48101003 => 'Male',
          :S48101004 => 'Female',
          :S48101005 => 'Male',
          :S48101006 => 'Female',
          :S48102001 => 'Male',
          :S50101001 => 'Female',
          :S50101002 => 'Male',
          :S50103001 => 'Male',
          :S51101001 => 'Female',
          :S51102001 => 'Male',
          :S51102002 => 'Male',
          :S51103001 => 'Female',
          :S51104001 => 'Female',
          :S51104002 => 'Male',
          :S51104003 => 'Male',
          :S51104004 => 'Male',
          :S51105001 => 'Male',
          :S51105002 => 'Male',
          :S51105003 => 'Female',
          :S51107001 => 'Male',
          :S51107002 => 'Male',
          :S51107003 => 'Male',
          :S51108001 => 'Female',
          :S51108002 => 'Female',
          :S51108003 => 'Female',
          :S51109001 => 'Female',
          :S51109002 => 'Female',
          :S51111001 => 'Female',
          :S51111002 => 'Male',
          :S51111003 => 'Male',
          :S52101001 => 'Male',
          :S52101002 => 'Male',
          :S52102001 => 'Male',
          :S52104001 => 'Female',
          :S52104002 => 'Female',
          :S54101001 => 'Male',
          :S54102001 => 'Female',
          :S54102002 => 'Male',
          :S54102003 => 'Male',
          :S54103001 => 'Male',
          :S54103002 => 'Male',
          :S55101001 => 'Male',
          :S55101002 => 'Male',
          :S55101003 => 'Female',
          :S55101004 => 'Male',
          :S57101001 => 'Female',
          :S57102001 => 'Female',
          :S57104001 => 'Female',
          :S57106001 => 'Male',
          :S57108001 => 'Female',
          :S57109001 => 'Female',
          :S57110001 => 'Male',
          :S57110002 => 'Male',
          :S57111001 => 'Female',
          :S57116001 => 'Male',
          :S57118001 => 'Male',
          :S57118002 => 'Female',
          :S57118003 => 'Male',
          :S57118004 => 'Male',
          :S57118005 => 'Male',
          :S57118006 => 'Male',
          :S57119002 => 'Male',
          :S58101P01 => 'Male',
          :S58102001 => 'Male',
          :S58104001 => 'Female',
          :S58105001 => 'Male',
          :S58105002 => 'Male',
          :S58115001 => 'Male',
          :S58117001 => 'Male',
          :S58120001 => 'Female',
          :S58123001 => 'Male',
          :S58123002 => 'Male',
          :S63101001 => 'Male',
          :S63101002 => 'Female',
          :S63101003 => 'Female',
          :S63104001 => 'Male',
          :S63106001 => 'Female',
          :S63107001 => 'Female',
          :S63107002 => 'Female',
          :S63107003 => 'Male',
          :S63107004 => 'Male',
          :S63108001 => 'Female',
          :S63108002 => 'Female',
          :S63108003 => 'Female',
          :S63109001 => 'Male',
          :S63110001 => 'Female',
          :S63110002 => 'Male',
          :S63117001 => 'Male',
          :S64101001 => 'Female',
          :S64102001 => 'Female',
          :S64102002 => 'Female',
          :S64103001 => 'Female',
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

      if outline[1] == 'United States of America'
        outline[1] = 'United States'
      end

      subject_treatment   = 'Ixazomib'.insert_value
      subject_arm         = 'Ixazomib or Placebo'.insert_value
      date_of_birth       = (outline[6].nil?)  ? ' NULL,' : "STR_TO_DATE(#{outline[6].insert(-3, '19').insert_value} '%e-%b-%Y'),"
      country_capital     = (outline[1].nil?)  ? ' NULL,' : "#{CAPITAL_CITY[(outline[1].gsub(/[^a-zA-Z0-9]/, '')).to_sym]}".insert_value
      gender              = "#{SUBJECT_GENDER[('S'+outline[4].gsub(/[^a-zA-Z0-9]/, '')).to_sym]}".insert_value

      values_clause <<
              "('C16021',"                                          + # study_protocol_id
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

class C16021_Inv
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

  SPECIMEN_TYPE = {
      :S24hrUPEPIFEAlbuminUrine                         => 'Urine',
      :S24hrUPEPIFEAlpha1GlobulinUrine                  => 'Urine',
      :S24hrUPEPIFEAlpha2GlobulinUrine                  => 'Urine',
      :S24hrUPEPIFEBetaGlobulinUrine                    => 'Urine',
      :S24hrUPEPIFEGammaGlobulinUrine                   => 'Urine',
      :S24hrUPEPIFEImmunofixationResultUrine            => 'Urine',
      :S24hrUPEPIFEMSpike                               => 'Urine',
      :S24hrUPEPIFEMSpike24hr                           => 'Urine',
      :S24hrUPEPIFETotalPrUrine24hr                     => 'Urine',
      :S24hrUPEPIFETotalPrUrineRandom                   => 'Urine',
      :SIgDIFE                                          => 'Serum',
      :SIgEIFE                                          => 'Serum',
      :SIgEIgDIFEStorage                                => 'Serum',
      :SRubeolaAntibodiesIgG                            => 'Serum',
      :SVaricellaZosterVAbIgG                           => 'Serum',
      :SIgA                                             => 'Serum',
      :SIgG                                             => 'Serum',
      :SIgM                                             => 'Serum',
      :SBlockSectioningofSlidesSectioned                => 'Bone Marrow Aspirate',
      :SBlockSectioningSectioningComment                => 'Bone Marrow Aspirate',
      :SBlockSectioningSlideSectioningDate              => 'Bone Marrow Aspirate',
      :SMRDBMASlideCaseBlock                            => 'Bone Marrow Aspirate',
      :SBMACD138PostIsoCD19Lymphs                       => 'Bone Marrow Aspirate',
      :SBMACD138PostIsoCD19PCs                          => 'Bone Marrow Aspirate',
      :SBMACD138PostIsoLymphsViable                     => 'Bone Marrow Aspirate',
      :SBMACD138PostIsoPCsViable                        => 'Bone Marrow Aspirate',
      :SBMACD138PostIsoTNCViable                        => 'Bone Marrow Aspirate',
      :SBMACD138PostIsoViableTotal                      => 'Bone Marrow Aspirate',
      :SBMACD138PostIso138RGNCountViable                => 'Bone Marrow Aspirate',
      :SBMACD138PostIso38RGNCountViable                 => 'Bone Marrow Aspirate',
      :SBMACD138PostIsoPCRGNCountViable                 => 'Bone Marrow Aspirate',
      :SBMACD138PostIsoTNCRGNCountViable                => 'Bone Marrow Aspirate',
      :SBMACD138PostIsoTotalCount                       => 'Bone Marrow Aspirate',
      :SBMACD138PostIsoViableRGNCountTot                => 'Bone Marrow Aspirate',
      :SBMACD138PreIsoCD19Lymphs                        => 'Bone Marrow Aspirate',
      :SBMACD138PreIsoCD19PCs                           => 'Bone Marrow Aspirate',
      :SBMACD138PreIsoLymphsViable                      => 'Bone Marrow Aspirate',
      :SBMACD138PreIsoPCsViable                         => 'Bone Marrow Aspirate',
      :SBMACD138PreIsoTNCViable                         => 'Bone Marrow Aspirate',
      :SBMACD138PreIsoViableTotal                       => 'Bone Marrow Aspirate',
      :SBMACD138PreIso138RGNCountViable                 => 'Bone Marrow Aspirate',
      :SBMACD138PreIso38RGNCountViable                  => 'Bone Marrow Aspirate',
      :SBMACD138PreIsoPCRGNCountViable                  => 'Bone Marrow Aspirate',
      :SBMACD138PreIsoTNCRGNCountViable                 => 'Bone Marrow Aspirate',
      :SBMACD138PreIsoTotalCount                        => 'Bone Marrow Aspirate',
      :SBMACD138PreIsoViableRGNCountTot                 => 'Bone Marrow Aspirate',
      :SBMATotalCellCountAspriateVolumeUponReceipt      => 'Bone Marrow Aspirate',
      :SBMATotalCellCountBMATotalcellcount              => 'Bone Marrow Aspirate',
      :SBMATotalCellCountBMAViablecellcount             => 'Bone Marrow Aspirate',
      :SBMATotalCellCountNmbrcellssavedforFISH          => 'Bone Marrow Aspirate',
      :SBMACD138Comment                                 => 'Bone Marrow Aspirate',
      :SBMATTCComment                                   => 'Bone Marrow Aspirate',
      :SBMACD1380PostIsoCD19Lymphs0                     => 'Bone Marrow Aspirate',
      :SBMACD1380PostIsoCD19PCs0                        => 'Bone Marrow Aspirate',
      :SBMACD1380PostIsoLymphsViable                    => 'Bone Marrow Aspirate',
      :SBMACD1380PostIsoPCsViable0                      => 'Bone Marrow Aspirate',
      :SBMACD1380PostIsoTNCViable0                      => 'Bone Marrow Aspirate',
      :SBMACD1380PostIsoViableTotal0                    => 'Bone Marrow Aspirate',
      :SBMACD1380PostIso38RGNCountVia                   => 'Bone Marrow Aspirate',
      :SBMACD1380PostIsoPCRGNCountVia                   => 'Bone Marrow Aspirate',
      :SBMACD1380PostIsoTNCRGNCountVia                  => 'Bone Marrow Aspirate',
      :SBMACD1380PostIsoTotalCount0                     => 'Bone Marrow Aspirate',
      :SBMACD1380PostIsoViaRGNCounttot                  => 'Bone Marrow Aspirate',
      :SBMACD1380PostIso138RGNCountVia                  => 'Bone Marrow Aspirate',
      :SBMACD1380PreIsoCD19Lymphs0                      => 'Bone Marrow Aspirate',
      :SBMACD1380PreIsoCD19PCs0                         => 'Bone Marrow Aspirate',
      :SBMACD1380PreIsoLymphsViable0                    => 'Bone Marrow Aspirate',
      :SBMACD1380PreIsoPCsViable0                       => 'Bone Marrow Aspirate',
      :SBMACD1380PreIsoTNCViable0                       => 'Bone Marrow Aspirate',
      :SBMACD1380PreIsoViableTotal0                     => 'Bone Marrow Aspirate',
      :SBMACD1380PreIso138RGNCountVia                   => 'Bone Marrow Aspirate',
      :SBMACD1380PreIso38RGNCountVia                    => 'Bone Marrow Aspirate',
      :SBMACD1380PreIsoPCRGNCountVia                    => 'Bone Marrow Aspirate',
      :SBMACD1380PreIsoTNCRGNCountVia                   => 'Bone Marrow Aspirate',
      :SBMACD1380PreIsoTotalCount0                      => 'Bone Marrow Aspirate',
      :SBMACD1380PreIsoViaRGNCounttot                   => 'Bone Marrow Aspirate',
      :SChemistryAlbumin                                => 'Serum',
      :SChemistryAlkalinePhosphatase                    => 'Serum',
      :SChemistryALTSGPT                                => 'Serum',
      :SChemistryASTSGOT                                => 'Serum',
      :SChemistryBicarbonate                            => 'Serum',
      :SChemistryBloodUreaNitrogen                      => 'Serum',
      :SChemistryCalcium                                => 'Serum',
      :SChemistryChloride                               => 'Serum',
      :SChemistryCreatinine                             => 'Serum',
      :SChemistryGammaGlutamylTransferase               => 'Serum',
      :SChemistryGlucoseSerum                           => 'Serum',
      :SChemistryLDH                                    => 'Serum',
      :SChemistryMagnesium                              => 'Serum',
      :SChemistryPhosphorous                            => 'Serum',
      :SChemistryPotassium                              => 'Serum',
      :SChemistrySodium                                 => 'Serum',
      :SChemistryTotalBili                              => 'Serum',
      :SChemistryUricAcid                               => 'Serum',
      :SCockcroftGaulteGFR                              => 'Serum',
      :SCorrectedCalcium                                => 'Serum',
      :SFISHSlideStorage1                               => 'Bone Marrow Aspirate',
      :SFISHSlideStorage2                               => 'Bone Marrow Aspirate',
      :SFISHSlideStorage3                               => 'Bone Marrow Aspirate',
      :SFISHSlideStorage4                               => 'Bone Marrow Aspirate',
      :SFISHSlideStorage5                               => 'Bone Marrow Aspirate',
      :SFISHSlideStorage6                               => 'Bone Marrow Aspirate',
      :SFISHSlideStorage7                               => 'Bone Marrow Aspirate',
      :SFISHSlideStorage8                               => 'Bone Marrow Aspirate',
      :SAutoDiffBasophilsAbsolute                       => 'Whole Blood',
      :SAutoDiffBasophilsPercent                        => 'Whole Blood',
      :SAutoDiffEosinophilsAbsolute                     => 'Whole Blood',
      :SAutoDiffEosinophilsPercent                      => 'Whole Blood',
      :SAutoDiffLymphocytesAbsolute                     => 'Whole Blood',
      :SAutoDiffLymphocytesPercent                      => 'Whole Blood',
      :SAutoDiffMonocytesAbsolute                       => 'Whole Blood',
      :SAutoDiffMonocytesPercent                        => 'Whole Blood',
      :SAutoDiffNeutrophilsAbsolute                     => 'Whole Blood',
      :SAutoDiffNeutrophilsPercent                      => 'Whole Blood',
      :SDiffMorphologyCommentsDifferentialComments      => 'Whole Blood',
      :SDiffMorphologyCommentsPlateletAssessment        => 'Whole Blood',
      :SDiffMorphologyCommentsRBCMorphology             => 'Whole Blood',
      :SDiffMorphologyCommentsWBCMorphology             => 'Whole Blood',
      :SHematologyHematocrit                            => 'Whole Blood',
      :SHematologyHemoglobin                            => 'Whole Blood',
      :SHematologyPlateletCount                         => 'Whole Blood',
      :SHematologyWhiteBloodCells                       => 'Whole Blood',
      :SManualDiffBandsAbsolute                         => 'Whole Blood',
      :SManualDiffBandsPercent                          => 'Whole Blood',
      :SManualDiffBasophilsManualPercent                => 'Whole Blood',
      :SManualDiffBasophilsManualAbsolute               => 'Whole Blood',
      :SManualDiffBlastsAbsolute                        => 'Whole Blood',
      :SManualDiffBlastsPercent                         => 'Whole Blood',
      :SManualDiffEosinophilsManualPercent              => 'Whole Blood',
      :SManualDiffEosinophilsManualAbsolute             => 'Whole Blood',
      :SManualDiffLymphocytesAtypicalAbsolute           => 'Whole Blood',
      :SManualDiffLymphocytesAtypicalPercent            => 'Whole Blood',
      :SManualDiffLymphocytesManualPercent              => 'Whole Blood',
      :SManualDiffLymphocytesManualAbsolute             => 'Whole Blood',
      :SManualDiffMetamyelocytesAbsolute                => 'Whole Blood',
      :SManualDiffMetamyelocytesPercent                 => 'Whole Blood',
      :SManualDiffMonocytesManualPercent                => 'Whole Blood',
      :SManualDiffMonocytesManualAbsolute               => 'Whole Blood',
      :SManualDiffMyelocytesAbsolute                    => 'Whole Blood',
      :SManualDiffMyelocytesPercent                     => 'Whole Blood',
      :SManualDiffNeutrophilsManualPercent              => 'Whole Blood',
      :SManualDiffNeutrophilsManualAbsolute             => 'Whole Blood',
      :SManualDiffNucleatedRBCManualAbsolute            => 'Whole Blood',
      :SManualDiffNucleatedRBCManualPercent             => 'Whole Blood',
      :SManualDiffPlasmaCellsAbsolute                   => 'Whole Blood',
      :SManualDiffPlasmaCellsPercent                    => 'Whole Blood',
      :SManualDiffPromyelocytesAbsolute                 => 'Whole Blood',
      :SManualDiffPromyelocytesPercent                  => 'Whole Blood',
      :SCBCComments                                     => 'Whole Blood',
      :SCBCtriggerReflexifC                             => 'Whole Blood',
      :SCBCtriggerReflexifS                             => 'Whole Blood',
      :SHematologySlidesReceived                        => 'Whole Blood',
      :SIgD                                             => 'Serum',
      :SIgE                                             => 'Serum',
      :SLymphocytephenotypingGranulocytes               => 'Whole Blood',
      :SLymphocytephenotypingLymphocytes                => 'Whole Blood',
      :SLymphocytephenotypingMonocytes                  => 'Whole Blood',
      :SLymphocytephenotypingCD19                       => 'Whole Blood',
      :SLymphocytephenotypingCD3CD16CD56                => 'Whole Blood',
      :SLymphocytephenotypingCD3Tube1                   => 'Whole Blood',
      :SLymphocytephenotypingCD3CD4                     => 'Whole Blood',
      :SLymphocytephenotypingCD3CD8                     => 'Whole Blood',
      :SLymphocytephenotypingAbsGranulocytes            => 'Whole Blood',
      :SLymphocytephenotypingAbsMonocytes               => 'Whole Blood',
      :SLymphocytephenotypingAbsoluteCD3Tube1           => 'Whole Blood',
      :SLymphocytephenotypingAbsoluteLymphocytesTube1   => 'Whole Blood',
      :SLymphocytephenotypingCD1656Absolute             => 'Whole Blood',
      :SLymphocytephenotypingCD19Absolute               => 'Whole Blood',
      :SLymphocytephenotypingCD4Absolute                => 'Whole Blood',
      :SLymphocytephenotypingCD8Absolute                => 'Whole Blood',
      :SLymphocytephenotypingTruCOUNTBeadCountTube2     => 'Whole Blood',
      :SLymphocytephenotypingTruCOUNTBeadsTube1         => 'Whole Blood',
      :SLymphocytePhenotComment                         => 'Whole Blood',
      :SMMCytogeneticsFISHAbn0CellsAmp1q21              => 'Enriched Plasma Cells',
      :SMMCytogeneticsFISHAbn0CellsDel17                => 'Enriched Plasma Cells',
      :SMMCytogeneticsFISHAbn0Cellst1416                => 'Enriched Plasma Cells',
      :SMMCytogeneticsFISHAbn0Cellst414                 => 'Enriched Plasma Cells',
      :SMMCytogeneticsFISHCellsCountedAmp1q21           => 'Enriched Plasma Cells',
      :SMMCytogeneticsFISHCellscountedDel17             => 'Enriched Plasma Cells',
      :SMMCytogeneticsFISHCellscountedt1416             => 'Enriched Plasma Cells',
      :SMMCytogeneticsFISHCellscountedt414              => 'Enriched Plasma Cells',
      :SMMCytogeneticsFISHCommentAmp1q21                => 'Enriched Plasma Cells',
      :SMMCytogeneticsFISHCommentDel17                  => 'Enriched Plasma Cells',
      :SMMCytogeneticsFISHCommentt1416                  => 'Enriched Plasma Cells',
      :SMMCytogeneticsFISHCommentt414                   => 'Enriched Plasma Cells',
      :SMMCytogeneticsFISHResultAmp1q21                 => 'Enriched Plasma Cells',
      :SMMCytogeneticsFISHResultDel17                   => 'Enriched Plasma Cells',
      :SMMCytogeneticsFISHResultt1416                   => 'Enriched Plasma Cells',
      :SMMCytogeneticsFISHResultt414                    => 'Enriched Plasma Cells',
      :SEnrichedPCFISHStorage                           => 'Enriched Plasma Cells',
      :SMRDBMASlide5                                    => 'Bone Marrow Aspirate',
      :SMRDBMASlide1                                    => 'Bone Marrow Aspirate',
      :SMRDBMASlide10                                   => 'Bone Marrow Aspirate',
      :SMRDBMASlide11                                   => 'Bone Marrow Aspirate',
      :SMRDBMASlide12                                   => 'Bone Marrow Aspirate',
      :SMRDBMASlide13                                   => 'Bone Marrow Aspirate',
      :SMRDBMASlide14                                   => 'Bone Marrow Aspirate',
      :SMRDBMASlide15                                   => 'Bone Marrow Aspirate',
      :SMRDBMASlide16                                   => 'Bone Marrow Aspirate',
      :SMRDBMASlide17                                   => 'Bone Marrow Aspirate',
      :SMRDBMASlide18                                   => 'Bone Marrow Aspirate',
      :SMRDBMASlide19                                   => 'Bone Marrow Aspirate',
      :SMRDBMASlide2                                    => 'Bone Marrow Aspirate',
      :SMRDBMASlide20                                   => 'Bone Marrow Aspirate',
      :SMRDBMASlide3                                    => 'Bone Marrow Aspirate',
      :SMRDBMASlide4                                    => 'Bone Marrow Aspirate',
      :SMRDBMASlide6                                    => 'Bone Marrow Aspirate',
      :SMRDBMASlide7                                    => 'Bone Marrow Aspirate',
      :SMRDBMASlide8                                    => 'Bone Marrow Aspirate',
      :SMRDBMASlide9                                    => 'Bone Marrow Aspirate',
      :SMRDFlowResidual                                 => 'Bone Marrow Aspirate',
      :SMRDFlowBoneMarrowSpecimenvolume                 => 'Bone Marrow Aspirate',
      :SMRDFlowBoneMarrowTotalCellCount                 => 'Bone Marrow Aspirate',
      :SMRDFlowBoneMarrowWhitebloodcellcount            => 'Bone Marrow Aspirate',
      :S7AADNegative                                    => 'Bone Marrow Aspirate',
      :SMRDFlowBMAComment                               => 'Bone Marrow Aspirate',
      :SNmbrcellssavedforDNARNA                         => 'Bone Marrow Aspirate',
      :SPKStorage1                                      => 'Plasma',
      :SPKStorage110                                    => 'Plasma',
      :SPKStorage140                                    => 'Plasma',
      :SPKStorage2                                      => 'Plasma',
      :SPKStorage210                                    => 'Plasma',
      :SPKStorage240                                    => 'Plasma',
      :SSerumFreeLightChainFreeKappaLt0Chains           => 'Serum',
      :SSerumFreeLightChainFreeLambdaLt0Chains          => 'Serum',
      :SSerumFreeLightChainKappaLambdaRatioSerum        => 'Serum',
      :SSerumHCG                                        => 'Serum',
      :SSerumHCG0                                       => 'Serum',
      :SSPEPIFEAGRatio                                  => 'Serum',
      :SSPEPIFEAlbumin0                                 => 'Serum',
      :SSPEPIFEAlpha1Globulin                           => 'Serum',
      :SSPEPIFEAlpha2Globulin                           => 'Serum',
      :SSPEPIFEBetaGlobulin                             => 'Serum',
      :SSPEPIFEGammaGlobulin                            => 'Serum',
      :SSPEPIFEGlobulinTotal                            => 'Serum',
      :SSPEPIFEImmunofixationResultSerum                => 'Serum',
      :SSPEPIFEMSpike                                   => 'Serum',
      :SSPEPIFETotalProtein                             => 'Serum',
      :STetanusAntitoxoidIgG                            => 'Serum',
      :SUrinalysisMacroscopicpH                         => 'Urine',
      :SUrinalysisMacroscopicSpecificgravity            => 'Urine',
      :SUrinalysisMacroscopicUABilirubin                => 'Urine',
      :SUrinalysisMacroscopicUABlood                    => 'Urine',
      :SUrinalysisMacroscopicUAClarityAppearance        => 'Urine',
      :SUrinalysisMacroscopicUAColor                    => 'Urine',
      :SUrinalysisMacroscopicUAGlucose                  => 'Urine',
      :SUrinalysisMacroscopicUAKetones                  => 'Urine',
      :SUrinalysisMacroscopicUALeukocyteEsterase        => 'Urine',
      :SUrinalysisMacroscopicUANitrite                  => 'Urine',
      :SUrinalysisMacroscopicUAUrobilinogen             => 'Urine',
      :SUrinalysisMacroscopicUrineProtein               => 'Urine',
      :SUrineMicroscopicAmorphousUratesorPhosphates     => 'Urine',
      :SUrineMicroscopicBacteria                        => 'Urine',
      :SUrineMicroscopicCalciumOxalate                  => 'Urine',
      :SUrineMicroscopicCellularCast                    => 'Urine',
      :SUrineMicroscopicCystine                         => 'Urine',
      :SUrineMicroscopicFattyCast                       => 'Urine',
      :SUrineMicroscopicGranularCast                    => 'Urine',
      :SUrineMicroscopicHyalineCast                     => 'Urine',
      :SUrineMicroscopicMucus                           => 'Urine',
      :SUrineMicroscopicRBCCast                         => 'Urine',
      :SUrineMicroscopicRBCUrine                        => 'Urine',
      :SUrineMicroscopicRenalTubularEpithelialCells     => 'Urine',
      :SUrineMicroscopicSperm                           => 'Urine',
      :SUrineMicroscopicSquamousEpithelialCells         => 'Urine',
      :SUrineMicroscopicTransitionalEpithelialCells     => 'Urine',
      :SUrineMicroscopicTriplePhosphate                 => 'Urine',
      :SUrineMicroscopicTyrosine                        => 'Urine',
      :SUrineMicroscopicUAComments                      => 'Urine',
      :SUrineMicroscopicUricAcidCrystal                 => 'Urine',
      :SUrineMicroscopicWaxyCast                        => 'Urine',
      :SUrineMicroscopicWBCCast                         => 'Urine',
      :SUrineMicroscopicWBCUrine                        => 'Urine',
      :SUrineMicroscopicYeast                           => 'Urine',
      :SUrinalysisMacroscopicTriggerMicroscopicifABN    => 'Urine',
      :SBMACD138DNAExtractDNAConcentration0             => 'Whole Blood',
      :SBMACD138DNAExtractDNAExtractionDate0            => 'Whole Blood',
      :SBMACD138DNAExtractDNAYield0                     => 'Whole Blood',
      :SBMACD138DNAExtractPurity0                       => 'Whole Blood',
      :SWBDNAExtractionDNAConcentration                 => 'Whole Blood',
      :SWBDNAExtractionDNAExtractionDate                => 'Whole Blood',
      :SWBDNAExtractionDNAYield                         => 'Whole Blood',
      :SWBDNAExtractionPurity                           => 'Whole Blood',
      :SOfNanoDNAAliquots                               => 'Whole Blood',
      :SOfWBDNAAliquots                                 => 'Whole Blood',
      :SBMACD138PreExtraction                           => 'Whole Blood',
      :SFinalVolume                                     => 'Whole Blood',
      :SFinalVolume0                                    => 'Whole Blood',
      :SPSMB1                                           => 'Whole Blood',
      :SPSMB10                                          => 'Whole Blood',
      :SPSMB100                                         => 'Whole Blood',
      :SWBDNAPreExtraction                              => 'Whole Blood',
      :SWBDNAStorage1                                   => 'DNA',
      :SWBDNAStorage2                                   => 'DNA'
  }.freeze

  SPECIMEN_DESIGNATION = {
      :SIgDIFE                                         => 'Antibodies IgD IgE and IFE',
      :SIgEIFE                                         => 'Antibodies IgD IgE and IFE',
      :SIgEIgDIFEStorage                               => 'Antibodies IgD IgE and IFE',
      :SRubeolaAntibodiesIgG                           => 'Antibodies IgG  ',
      :SVaricellaZosterVAbIgG                          => 'Antibodies IgG  ',
      :SIgA                                            => 'Antobodies Ig AGM',
      :SIgG                                            => 'Antobodies Ig AGM',
      :SIgM                                            => 'Antobodies Ig AGM',
      :SBlockSectioningofSlidesSectioned               => 'BMA Slide',
      :SBlockSectioningSectioningComment               => 'BMA Slide',
      :SBlockSectioningSlideSectioningDate             => 'BMA Slide',
      :SMRDBMASlideCaseBlock                           => 'BMA Slide',
      :SFISHSlideStorage1                              => 'FISH Slide Storage',
      :SFISHSlideStorage2                              => 'FISH Slide Storage',
      :SFISHSlideStorage3                              => 'FISH Slide Storage',
      :SFISHSlideStorage4                              => 'FISH Slide Storage',
      :SFISHSlideStorage5                              => 'FISH Slide Storage',
      :SFISHSlideStorage6                              => 'FISH Slide Storage',
      :SFISHSlideStorage7                              => 'FISH Slide Storage',
      :SFISHSlideStorage8                              => 'FISH Slide Storage',
      :SAutoDiffBasophilsAbsolute                      => 'Hematology',
      :SAutoDiffBasophilsPercent                       => 'Hematology',
      :SAutoDiffEosinophilsAbsolute                    => 'Hematology',
      :SAutoDiffEosinophilsPercent                     => 'Hematology',
      :SAutoDiffLymphocytesAbsolute                    => 'Hematology',
      :SAutoDiffLymphocytesPercent                     => 'Hematology',
      :SAutoDiffMonocytesAbsolute                      => 'Hematology',
      :SAutoDiffMonocytesPercent                       => 'Hematology',
      :SAutoDiffNeutrophilsAbsolute                    => 'Hematology',
      :SAutoDiffNeutrophilsPercent                     => 'Hematology',
      :SDiffMorphologyCommentsDifferentialComments     => 'Hematology',
      :SDiffMorphologyCommentsPlateletAssessment       => 'Hematology',
      :SDiffMorphologyCommentsRBCMorphology            => 'Hematology',
      :SDiffMorphologyCommentsWBCMorphology            => 'Hematology',
      :SHematologyHematocrit                           => 'Hematology',
      :SHematologyHemoglobin                           => 'Hematology',
      :SHematologyPlateletCount                        => 'Hematology',
      :SHematologyWhiteBloodCells                      => 'Hematology',
      :SManualDiffBandsAbsolute                        => 'Hematology',
      :SManualDiffBandsPercent                         => 'Hematology',
      :SManualDiffBasophilsManualPercent               => 'Hematology',
      :SManualDiffBasophilsManualAbsolute              => 'Hematology',
      :SManualDiffBlastsAbsolute                       => 'Hematology',
      :SManualDiffBlastsPercent                        => 'Hematology',
      :SManualDiffEosinophilsManualPercent             => 'Hematology',
      :SManualDiffEosinophilsManualAbsolute            => 'Hematology',
      :SManualDiffLymphocytesAtypicalAbsolute          => 'Hematology',
      :SManualDiffLymphocytesAtypicalPercent           => 'Hematology',
      :SManualDiffLymphocytesManualPercent             => 'Hematology',
      :SManualDiffLymphocytesManualAbsolute            => 'Hematology',
      :SManualDiffMetamyelocytesAbsolute               => 'Hematology',
      :SManualDiffMetamyelocytesPercent                => 'Hematology',
      :SManualDiffMonocytesManualPercent               => 'Hematology',
      :SManualDiffMonocytesManualAbsolute              => 'Hematology',
      :SManualDiffMyelocytesAbsolute                   => 'Hematology',
      :SManualDiffMyelocytesPercent                    => 'Hematology',
      :SManualDiffNeutrophilsManualPercent             => 'Hematology',
      :SManualDiffNeutrophilsManualAbsolute            => 'Hematology',
      :SManualDiffNucleatedRBCManualAbsolute           => 'Hematology',
      :SManualDiffNucleatedRBCManualPercent            => 'Hematology',
      :SManualDiffPlasmaCellsAbsolute                  => 'Hematology',
      :SManualDiffPlasmaCellsPercent                   => 'Hematology',
      :SManualDiffPromyelocytesAbsolute                => 'Hematology',
      :SManualDiffPromyelocytesPercent                 => 'Hematology',
      :SCBCComments                                    => 'Hematology',
      :SCBCtriggerReflexifC                            => 'Hematology',
      :SCBCtriggerReflexifS                            => 'Hematology',
      :SHematologySlidesReceived                       => 'Hematology',
      :SIgD                                            => 'IgD',
      :SIgE                                            => 'IgE',
      :SLymphocytephenotypingGranulocytes              => 'Lymphocyte phenotyping',
      :SLymphocytephenotypingLymphocytes               => 'Lymphocyte phenotyping',
      :SLymphocytephenotypingMonocytes                 => 'Lymphocyte phenotyping',
      :SLymphocytephenotypingCD19                      => 'Lymphocyte phenotyping',
      :SLymphocytephenotypingCD3CD16CD56               => 'Lymphocyte phenotyping',
      :SLymphocytephenotypingCD3Tube1                  => 'Lymphocyte phenotyping',
      :SLymphocytephenotypingCD3CD4                    => 'Lymphocyte phenotyping',
      :SLymphocytephenotypingCD3CD8                    => 'Lymphocyte phenotyping',
      :SLymphocytephenotypingAbsGranulocytes           => 'Lymphocyte phenotyping',
      :SLymphocytephenotypingAbsMonocytes              => 'Lymphocyte phenotyping',
      :SLymphocytephenotypingAbsoluteCD3Tube1          => 'Lymphocyte phenotyping',
      :SLymphocytephenotypingAbsoluteLymphocytesTube1  => 'Lymphocyte phenotyping',
      :SLymphocytephenotypingCD1656Absolute            => 'Lymphocyte phenotyping',
      :SLymphocytephenotypingCD19Absolute              => 'Lymphocyte phenotyping',
      :SLymphocytephenotypingCD4Absolute               => 'Lymphocyte phenotyping',
      :SLymphocytephenotypingCD8Absolute               => 'Lymphocyte phenotyping',
      :SLymphocytephenotypingTruCOUNTBeadCountTube2    => 'Lymphocyte phenotyping',
      :SLymphocytephenotypingTruCOUNTBeadsTube1        => 'Lymphocyte phenotyping',
      :SLymphocytePhenotComment                        => 'Lymphocyte phenotyping',
      :SMMCytogeneticsFISHAbn0CellsAmp1q21             => 'MM Cytogenetics FISH',
      :SMMCytogeneticsFISHAbn0CellsDel17               => 'MM Cytogenetics FISH',
      :SMMCytogeneticsFISHAbn0Cellst1416               => 'MM Cytogenetics FISH',
      :SMMCytogeneticsFISHAbn0Cellst414                => 'MM Cytogenetics FISH',
      :SMMCytogeneticsFISHCellsCountedAmp1q21          => 'MM Cytogenetics FISH',
      :SMMCytogeneticsFISHCellscountedDel17            => 'MM Cytogenetics FISH',
      :SMMCytogeneticsFISHCellscountedt1416            => 'MM Cytogenetics FISH',
      :SMMCytogeneticsFISHCellscountedt414             => 'MM Cytogenetics FISH',
      :SMMCytogeneticsFISHCommentAmp1q21               => 'MM Cytogenetics FISH',
      :SMMCytogeneticsFISHCommentDel17                 => 'MM Cytogenetics FISH',
      :SMMCytogeneticsFISHCommentt1416                 => 'MM Cytogenetics FISH',
      :SMMCytogeneticsFISHCommentt414                  => 'MM Cytogenetics FISH',
      :SMMCytogeneticsFISHResultAmp1q21                => 'MM Cytogenetics FISH',
      :SMMCytogeneticsFISHResultDel17                  => 'MM Cytogenetics FISH',
      :SMMCytogeneticsFISHResultt1416                  => 'MM Cytogenetics FISH',
      :SMMCytogeneticsFISHResultt414                   => 'MM Cytogenetics FISH',
      :SEnrichedPCFISHStorage                          => 'MM Cytogenetics FISH',
      :SMRDBMASlide5                                   => 'MRD BMA Slide',
      :SMRDBMASlide1                                   => 'MRD BMA Slide',
      :SMRDBMASlide10                                  => 'MRD BMA Slide',
      :SMRDBMASlide11                                  => 'MRD BMA Slide',
      :SMRDBMASlide12                                  => 'MRD BMA Slide',
      :SMRDBMASlide13                                  => 'MRD BMA Slide',
      :SMRDBMASlide14                                  => 'MRD BMA Slide',
      :SMRDBMASlide15                                  => 'MRD BMA Slide',
      :SMRDBMASlide16                                  => 'MRD BMA Slide',
      :SMRDBMASlide17                                  => 'MRD BMA Slide',
      :SMRDBMASlide18                                  => 'MRD BMA Slide',
      :SMRDBMASlide19                                  => 'MRD BMA Slide',
      :SMRDBMASlide2                                   => 'MRD BMA Slide',
      :SMRDBMASlide20                                  => 'MRD BMA Slide',
      :SMRDBMASlide3                                   => 'MRD BMA Slide',
      :SMRDBMASlide4                                   => 'MRD BMA Slide',
      :SMRDBMASlide6                                   => 'MRD BMA Slide',
      :SMRDBMASlide7                                   => 'MRD BMA Slide',
      :SMRDBMASlide8                                   => 'MRD BMA Slide',
      :SMRDBMASlide9                                   => 'MRD BMA Slide',
      :SMRDFlowResidual                                => 'MRD Flow - Residual',
      :SMRDFlowBoneMarrowSpecimenvolume                => 'MRD Flow Bone Marrow',
      :SMRDFlowBoneMarrowTotalCellCount                => 'MRD Flow Bone Marrow',
      :SMRDFlowBoneMarrowWhitebloodcellcount           => 'MRD Flow Bone Marrow',
      :S7AADNegative                                   => 'MRD Flow Bone Marrow',
      :SMRDFlowBMAComment                              => 'MRD Flow Bone Marrow',
      :SNmbrcellssavedforDNARNA                        => 'MRD Flow Bone Marrow',
      :SPKStorage1                                     => 'PK',
      :SPKStorage110                                   => 'PK',
      :SPKStorage140                                   => 'PK',
      :SPKStorage2                                     => 'PK',
      :SPKStorage210                                   => 'PK',
      :SPKStorage240                                   => 'PK',
      :SSerumFreeLightChainFreeKappaLt0Chains          => 'Serum Free Light Chain',
      :SSerumFreeLightChainFreeLambdaLt0Chains         => 'Serum Free Light Cin',
      :SSerumFreeLightChainKappaLambdaRatioSerum       => 'Serum Free Light Cin',
      :SSerumHCG                                       => 'Serum HCG',
      :SSerumHCG0                                      => 'Serum HCG',
      :SSPEPIFEAGRatio                                 => 'SPEP + IFE',
      :SSPEPIFEAlbumin0                                => 'SPEP + IFE',
      :SSPEPIFEAlpha1Globulin                          => 'SPEP + IFE',
      :SSPEPIFEAlpha2Globulin                          => 'SPEP + IFE',
      :SSPEPIFEBetaGlobulin                            => 'SPEP + IFE',
      :SSPEPIFEGammaGlobulin                           => 'SPEP + IFE',
      :SSPEPIFEGlobulinTotal                           => 'SPEP + IFE',
      :SSPEPIFEImmunofixationResultSerum               => 'SPEP + IFE',
      :SSPEPIFEMSpike                                  => 'SPEP + IFE',
      :SSPEPIFETotalProtein                            => 'SPEP + IFE',
      :STetanusAntitoxoidIgG                           => 'Tetanus Antitoxoid IgG',
      :SBMACD138DNAExtractDNAConcentration0            => 'WB DNA Extraction',
      :SBMACD138DNAExtractDNAExtractionDate0           => 'WB DNA Extraction',
      :SBMACD138DNAExtractDNAYield0                    => 'WB DNA Extraction',
      :SBMACD138DNAExtractPurity0                      => 'WB DNA Extraction',
      :SWBDNAExtractionDNAConcentration                => 'WB DNA Extraction',
      :SWBDNAExtractionDNAExtractionDate               => 'WB DNA Extraction',
      :SWBDNAExtractionDNAYield                        => 'WB DNA Extraction',
      :SWBDNAExtractionPurity                          => 'WB DNA Extraction',
      :SOfNanoDNAAliquots                              => 'WB DNA Extraction',
      :SOfWBDNAAliquots                                => 'WB DNA Extraction',
      :SBMACD138PreExtraction                          => 'WB DNA Extraction',
      :SFinalVolume                                    => 'WB DNA Extraction',
      :SFinalVolume0                                   => 'WB DNA Extraction',
      :SPSMB1                                          => 'WB DNA Extraction',
      :SPSMB10                                         => 'WB DNA Extraction',
      :SPSMB100                                        => 'WB DNA Extraction',
      :SWBDNAPreExtraction                             => 'WB DNA Extraction',
      :SWBDNAStorage1                                  => 'WB DNA Storage',
      :SWBDNAStorage2                                  => 'WB DNA Storage'
  }.freeze

  SPECIMEN_DESIGNATION_DETAIL = {
      :SFISHSlideStorage1         => 'FISH Slide Storage 1',
      :SFISHSlideStorage2         => 'FISH Slide Storage 2',
      :SFISHSlideStorage3         => 'FISH Slide Storage 3',
      :SFISHSlideStorage4         => 'FISH Slide Storage 4',
      :SFISHSlideStorage5         => 'FISH Slide Storage 5',
      :SFISHSlideStorage6         => 'FISH Slide Storage 6',
      :SFISHSlideStorage7         => 'FISH Slide Storage 7',
      :SFISHSlideStorage8         => 'FISH Slide Storage 8',
      :SHematologySlidesReceived  => 'Hematology Slide',
      :SMRDBMASlide5              => 'MRD BMA Slide5',
      :SMRDBMASlide1              => 'MRD BMA Slide 1',
      :SMRDBMASlide10             => 'MRD BMA Slide 10',
      :SMRDBMASlide11             => 'MRD BMA Slide 11',
      :SMRDBMASlide12             => 'MRD BMA Slide 12',
      :SMRDBMASlide13             => 'MRD BMA Slide 13',
      :SMRDBMASlide14             => 'MRD BMA Slide 14',
      :SMRDBMASlide15             => 'MRD BMA Slide 15',
      :SMRDBMASlide16             => 'MRD BMA Slide 16',
      :SMRDBMASlide17             => 'MRD BMA Slide 17',
      :SMRDBMASlide18             => 'MRD BMA Slide 18',
      :SMRDBMASlide19             => 'MRD BMA Slide 19',
      :SMRDBMASlide2              => 'MRD BMA Slide 2',
      :SMRDBMASlide20             => 'MRD BMA Slide 20',
      :SMRDBMASlide3              => 'MRD BMA Slide 3',
      :SMRDBMASlide4              => 'MRD BMA Slide 4',
      :SMRDBMASlide6              => 'MRD BMA Slide 6',
      :SMRDBMASlide7              => 'MRD BMA Slide 7',
      :SMRDBMASlide8              => 'MRD BMA Slide 8',
      :SMRDBMASlide9              => 'MRD BMA Slide 9',
      :SPKStorage1                => 'PK 1',
      :SPKStorage110              => 'PK 1',
      :SPKStorage140              => 'PK 2',
      :SPKStorage2                => 'PK 2',
      :SPKStorage210              => 'PK 2',
      :SWBDNAStorage1             => 'WB DNA Storage 1',
      :SWBDNAStorage2             => 'WB DNA Storage 2'
  }.freeze

   VISIT_MAP = {
        :C10D1        => 'Cycle 10 Day 1',
        :C11D1        => 'Cycle 11 Day 1',
        :C12D1        => 'Cycle 12 Day 1',
        :C13D1        => 'Cycle 13 Day 1',
        :C14D1        => 'Cycle 14 Day 1',
        :C15D1        => 'Cycle 15 Day 1',
        :C1D1         => 'Cycle 1 Day 1',
        :C1D15        => 'Cycle 15 Day 15',
        :C1D8         => 'Cycle 1 Day 8',
        :C2D1         => 'Cycle 2 Day 1',
        :C2D8         => 'Cycle 2 Day 8',
        :C3D1         => 'Cycle 3 Day 1',
        :C4D1         => 'Cycle 4 Day 1',
        :C5D1         => 'Cycle 5 Day 1',
        :C5D8         => 'Cycle 5 Day 1',
        :C6D1         => 'Cycle 6 Day 1',
        :C7D1         => 'Cycle 7 Day 1',
        :C8D1         => 'Cycle 8 Day 1',
        :C9D1         => 'Cycle 9 Day 1',
        :CR           => 'Complete Response',
        :EOT          => 'End of Treatment',
        :FollowupPD   => 'Progressive Disease (PD)',
        :FollowupPFS  => 'Progression-Free Survival 2 (PFS2)',
        :PreScreening => 'Pre-screening',
        :Screening    => 'Screening',
        :Unscheduled  => 'Unscheduled'
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
