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
      :Belgium      => 'LCEU',
      :Canada       => 'LCUS',
      :France       => 'LCEU',
      :Japan        => 'LCAP',
      :NewZealand   => 'LCAP',
      :Russia       => 'LCEU',
      :SouthKorea   => 'LCAP',
      :UnitedStates => 'LCUS',
  }.freeze

  PARENT_OFFSET = {
      :MMCytogeneticsFISHAbnCellsAmp1q21      => -1,
      :MMCytogeneticsFISHAbnCellsDel17        => -1,
      :MMCytogeneticsFISHAbnCellst1416        => -1,
      :MMCytogeneticsFISHAbnCellst414         => -1,
      :MMCytogeneticsFISHCellsCountedAmp1q21  => -1,
      :MMCytogeneticsFISHCellsCountedDel17    => -1,
      :MMCytogeneticsFISHCellsCountedt141     => -1,
      :MMCytogeneticsFISHCellsCountedt414     => -1,
      :MMCytogeneticsFISHCommentAmp1q21       => -1,
      :MMCytogeneticsFISHCommentDel17         => -1,
      :MMCytogeneticsFISHCommentt1416         => -1,
      :MMCytogeneticsFISHCommentt414          => -1,
      :MMCytogeneticsFISHResultAmp1q21        => -1,
      :MMCytogeneticsFISHResultDel17          => -1,
      :MMCytogeneticsFISHResultt1416          => -1,
      :MMCytogeneticsFISHResultt414           => -1,
      :PCDNARNAExtractionDNAconcentration     => -3,
      :PCDNARNAExtractionDNAqualityA260280    => -3,
      :PCDNARNAExtractionDNAtotalyield        => -3,
      :PCDNARNAExtractionExtractionDate       => -3,
      :PCDNARNAExtractionRNAconcentration     => -3,
      :PCDNARNAExtractionRNARINScore          => -3,
      :PCDNARNAExtractionRNAtotalyield        => -3,
      :BMDNAStorage1                          => -1,
      :BMDNAStorage2                          => -2,
      :BMRNAStorage1                          => -3,
      :BMRNAStorage2                          => -4,
      :DNAAliquotVolume                       => -3,
      :EnrichedPCFISHStorage                  => -1,
      :FISHSlideStorage                       => -1,
      :PCDNARNAStorage                        => -3,
      :WBDNAStorage1                          => -1,
      :WBDNAStorage2                          => -2,
  }.freeze

  SPECIMEN_TYPE = {
      :BMACD138PlasmaCellEnPostIsoCD19Lymphs          => 	  'Bone Marrow Aspirate',
      :BMACD138PlasmaCellEnPostIsoCD19PCs  	          =>    'Bone Marrow Aspirate',
      :BMACD138PlasmaCellEnPostIsoLymphsViable  	    =>  	'Bone Marrow Aspirate',
      :BMACD138PlasmaCellEnPostIsoPCsViable  	        =>  	'Bone Marrow Aspirate',
      :BMACD138PlasmaCellEnPostIsoTNCViable  	        =>  	'Bone Marrow Aspirate',
      :BMACD138PlasmaCellEnPostIsoViableTotal   	    =>  	'Bone Marrow Aspirate',
      :BMACD138PlasmaCellEnPostIso138RGNCountViable  	=>  	'Bone Marrow Aspirate',
      :BMACD138PlasmaCellEnPostIso38RGNCountViable  	=>  	'Bone Marrow Aspirate',
      :BMACD138PlasmaCellEnPostIsoPCRGNCountViable  	=>  	'Bone Marrow Aspirate',
      :BMACD138PlasmaCellEnPostIsoTNCRGNCountViable  	=>  	'Bone Marrow Aspirate',
      :BMACD138PlasmaCellEnPostIsoTotalCount  	      =>  	'Bone Marrow Aspirate',
      :BMACD138PlasmaCellEnPostIsoViableRGNCountTot  	=>  	'Bone Marrow Aspirate',
      :BMACD138PlasmaCellEnPreIsoCD19Lymphs  	        =>  	'Bone Marrow Aspirate',
      :BMACD138PlasmaCellEnPreIsoCD19PCs  	          =>  	'Bone Marrow Aspirate',
      :BMACD138PlasmaCellEnPreIsoLymphsViable       	=>  	'Bone Marrow Aspirate',
      :BMACD138PlasmaCellEnPreIsoPCsViable  	        =>  	'Bone Marrow Aspirate',
      :BMACD138PlasmaCellEnPreIsoTNCViable  	        =>  	'Bone Marrow Aspirate',
      :BMACD138PlasmaCellEnPreIsoViableTotal  	      =>  	'Bone Marrow Aspirate',
      :BMACD138PlasmaCellEnPreIso138RGNCountViable  	=>  	'Bone Marrow Aspirate',
      :BMACD138PlasmaCellEnPreIso38RGNCountViable 	  =>  	'Bone Marrow Aspirate',
      :BMACD138PlasmaCellEnPreIsoPCRGNCountViable  	  =>  	'Bone Marrow Aspirate',
      :BMACD138PlasmaCellEnPreIsoTNCRGNCountViable  	=>  	'Bone Marrow Aspirate',
      :BMACD138PlasmaCellEnPreIsoTotalCount  	        =>  	'Bone Marrow Aspirate',
      :BMACD138PlasmaCellEnPreIsoViableRGNCountTot  	=>  	'Bone Marrow Aspirate',
      :DiffMorphologyCommentsDifferentialComments  	  =>  	'Whole Blood',
      :DiffMorphologyCommentsPlateletAssessment  	    =>  	'Whole Blood',
      :HematologyPlateletCount  	                    =>  	'Whole Blood',
      :ManualDiffPlasmaCellsAbsolute  	              =>  	'Whole Blood',
      :ManualDiffPlasmaCellsPercent  	                =>  	'Whole Blood',
      :MLNMMMRD2AssayBMSpecimenvolume  	              =>  	'Enriched Plasma Cells',
      :MLNMMMRD2AssayBMTotalCellCount  	              =>  	'Enriched Plasma Cells',
      :MMCytogeneticsFISHAbnCellsAmp1q21  	          =>  	'Enriched Plasma Cells',
      :MMCytogeneticsFISHAbnCellsDel17  	            =>  	'Enriched Plasma Cells',
      :MMCytogeneticsFISHAbnCellst1416  	            =>  	'Enriched Plasma Cells',
      :MMCytogeneticsFISHAbnCellst414  	              =>  	'Enriched Plasma Cells',
      :MMCytogeneticsFISHCellsCountedAmp1q21  	      =>  	'Enriched Plasma Cells',
      :MMCytogeneticsFISHCellsCountedDel17  	        =>  	'Enriched Plasma Cells',
      :MMCytogeneticsFISHCellsCountedt1416  	        =>  	'Enriched Plasma Cells',
      :MMCytogeneticsFISHCellsCountedt414  	          =>  	'Enriched Plasma Cells',
      :MMCytogeneticsFISHCommentAmp1q21  	            =>  	'Enriched Plasma Cells',
      :MMCytogeneticsFISHCommentDel17             	  =>  	'Enriched Plasma Cells',
      :MMCytogeneticsFISHCommentt1416  	              =>  	'Enriched Plasma Cells',
      :MMCytogeneticsFISHCommentt414  	              =>  	'Enriched Plasma Cells',
      :MMCytogeneticsFISHResultAmp1q21  	            =>  	'Enriched Plasma Cells',
      :MMCytogeneticsFISHResultDel17  	              =>  	'Enriched Plasma Cells',
      :MMCytogeneticsFISHResultt1416  	              =>  	'Enriched Plasma Cells',
      :MMCytogeneticsFISHResultt414  	                =>  	'Enriched Plasma Cells',
      :PCDNARNAExtractionDNAconcentration  	          =>  	'Enriched Plasma Cells',
      :PCDNARNAExtractionDNAqualityA260280  	        =>  	'Enriched Plasma Cells',
      :PCDNARNAExtractionDNAtotalyield  	            =>  	'Enriched Plasma Cells',
      :PCDNARNAExtractionExtractionDate  	            =>  	'Enriched Plasma Cells',
      :PCDNARNAExtractionRNAconcentration  	          =>  	'Enriched Plasma Cells',
      :PCDNARNAExtractionRNARINScore 	                =>  	'Enriched Plasma Cells',
      :PCDNARNAExtractionRNAtotalyield  	            =>  	'Enriched Plasma Cells',
      :TotalViableCellCountAspriateVolumeUponReceipt  =>  	'Bone Marrow Aspirate',
      :TotalViableCellCountNmbrcellssavedforDNARNA    =>  	'Bone Marrow Aspirate',
      :TotalViableCellCountNmbrcellssavedforFISH  	  =>  	'Bone Marrow Aspirate',
      :TotalViableCellCountTotalcellcounthemocyto  	  =>  	'Bone Marrow Aspirate',
      :TotalViableCellCountViablecellcounthemocyto    =>  	'Bone Marrow Aspirate',
      :WBDNAExtractionDNAconcentration  	            =>  	'Whole Blood',
      :WBDNAExtractionDNAExtractionDate  	            =>  	'Whole Blood',
      :WBDNAExtractionDNAYield  	                    =>  	'Whole Blood',
      :OfDNAAliquots  	                              =>  	'Whole Blood',
      :OfNanDNAAliquots  		                          =>  	'Whole Blood',
      :OfRNAAliquots  			                          =>  	'Whole Blood',
      :BMDNAStorage1  			                          =>  	'DNA',
      :BMDNAStorage2  			                          =>  	'DNA',
      :BMRNAStorage1  			                          =>  	'RNA',
      :BMRNAStorage2  			                          =>  	'RNA',
      :BoneBiomarkerStorage1  	                      =>  	'Serum',
      :BoneBiomarkerStorage2  	                      =>  	'Serum',
      :BoneBiomarkerStorage3  	                      =>  	'Serum',
      :BoneBiomarkerStorage4  	                      =>  	'Serum',
      :DNAAliquotVolume  	                            =>  	'Whole Blood',
      :EnrichedPCFISHStorage  	                      =>  	'Enriched Plasma Cells',
      :FISHSlideStorage  	                            =>  	'Enriched Plasma Cells',
      :GermlineDNAStorage  	                          =>  	'Whole Blood',
      :MRDPanel  	                                    =>  	'Enriched Plasma Cells',
      :PCDNARNAStorage  	                            =>  	'Whole Blood',
      :PlasmaProteasomeStor1  	                      =>  	'Plasma',
      :PlasmaProteasomeStor2  	                      =>  	'Plasma',
      :WBDNAStorage1  	                              =>  	'DNA',
      :WBDNAStorage2  	                              =>  	'DNA',
  }.freeze

   VISIT_MAP = {
      :C10D1        => 'Cycle 10 Day 1',
      :C11D1        => 'Cycle 11 Day 1',
      :C12D1        => 'Cycle 12 Day 1',
      :C13D1        => 'Cycle 13 Day 1',
      :C14D1        => 'Cycle 14 Day 1',
      :C15D1        => 'Cycle 15 Day 1',
      :C16D1        => 'Cycle 16 Day 1',
      :C17D1        => 'Cycle 17 Day 1',
      :C18D1        => 'Cycle 18 Day 1',
      :C19D1        => 'Cycle 19 Day 1',
      :C1D1         => 'Cycle 1 Day 1',
      :C1D14        => 'Cycle 1 Day 14',
      :C1D21        => 'Cycle 1 Day 21',
      :C1D7         => 'Cycle 1 Day 7',
      :C20D1        => 'Cycle 20 Day 1',
      :C21D1        => 'Cycle 21 Day 1',
      :C22D1        => 'Cycle 22 Day 1',
      :C23D1        => 'Cycle 23 Day 1',
      :C24D1        => 'Cycle 24 Day 1',
      :C25D1        => 'Cycle 25 Day 1',
      :C26D1        => 'Cycle 26 Day 1',
      :C27D1        => 'Cycle 27 Day 1',
      :C28D1        => 'Cycle 28 Day 1',
      :C29D1        => 'Cycle 29 Day 1',
      :C2D1         => 'Cycle 2 Day 1',
      :C2D14        => 'Cycle 2 Day 14',
      :C2D21        => 'Cycle 2 Day 21',
      :C2D7         => 'Cycle 2 Day 7',
      :C30D1        => 'Cycle 30 Day 1',
      :C31D1        => 'Cycle 31 Day 1',
      :C32D1        => 'Cycle 32 Day 1',
      :C33D1        => 'Cycle 33 Day 1',
      :C34D1        => 'Cycle 34 Day 1',
      :C35D1        => 'Cycle 35 Day 1',
      :C36D1        => 'Cycle 36 Day 1',
      :C37D1        => 'Cycle 37 Day 1',
      :C3D1         => 'Cycle 3 Day 1',
      :C3D14        => 'Cycle 3 Day 14',
      :C4D1         => 'Cycle 4 Day 1',
      :C5D1         => 'Cycle 5 Day 1',
      :C6D1         => 'Cycle 6 Day 1',
      :C7D1         => 'Cycle 7 Day 1',
      :C8D1         => 'Cycle 8 Day 1',
      :C9D1         => 'Cycle 9 Day 1',
      :EOT          => 'End of Treatment',
      :FollowUpPFS  => 'Progressive-free Survival PFS',
      :MRDKit       => 'MRD Panel',
      :RelapseBMA   => 'Relapse BMA',
      :Screening    => 'Screening',
      :ScreeningBMA => 'Screening BMA',
      :Unscheduled  => 'Unscheduled',
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
                 WHERE    dsv.study_protocol_id = 'C16014'
                 GROUP BY dsv.site_number,
                          dsv.subject_code;"

    @my_subjects = this_connection.query(my_select)

    @processing_lines = Array.new
    lines = 0

    @inbound_lines.each do |specline|
      if specline[10].nil?                 ||
         specline[10] == ''                ||
         specline[19].strip == 'Cancelled' ||
         specline[19].strip == 'Assigned'  ||
         specline[18].strip == 'Temp Tale Storage'
        next
      end

      found = false
      @processing_lines.each do |distinct_line|

        # see if we actually need the line in the file
        if (specline[0]  == distinct_line[0])  &&     # Accession Number
           (specline[10] == distinct_line[10])        # Tube Number
          found = true

          if !specline[20].nil? && specline[20][0..4] == 'Check'
            distinct_line[20] = specline[20]
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

      treatment           = 'MLN9708'
      arm                 = 'MLN9708 or Placebo'
      date_of_birth       = (outline[5].nil?)  ? ' NULL,' : "STR_TO_DATE(#{outline[5].insert(-3, '19').insert_value} '%e-%b-%Y'),"
      visit               = VISIT_MAP[(outline[15].gsub(/[^a-zA-Z0-9]/, '')).to_sym]
      specimen_type       = SPECIMEN_TYPE[(((outline[17].nil? ? '' : outline[17]) + (outline[18].nil? ? '' : outline[18])).gsub(/[^a-zA-Z0-9]/, '')).to_sym]
      receive_date        = (outline[14].nil?)  ? ' NULL,' : " STR_TO_DATE(#{outline[14].insert_value} '%c/%e/%Y %T'),"
      specimen_barcode    = outline[0] + '-' + outline[10].rjust(2,'0')
      specimen_hub        = SPECIMEN_HUB[(outline[3].gsub(/[^a-zA-Z0-9]/, '')).to_sym]
      specimen_status     = "Exhausted"
      specimen_location   = specimen_hub
      specimen_is_child   = 'No'
      specimen_parent     = 'NULL'

# Figure our lineage

      parent_offset       = PARENT_OFFSET[(((outline[17].nil? ? '' : outline[17]) + (outline[18].nil? ? '' : outline[18])).gsub(/[^a-zA-Z0-9]/, '')).to_sym]

      if !parent_offset.nil?
        specimen_is_child = 'Yes'
        parent_tube       = outline[10].to_i + parent_offset
        specimen_parent   = outline[0] + '-' + parent_tube.to_s.rjust(2, '0')
     end

# set specimen status and vendor code base upon visit and tube mapping.

      case visit
        when 'Screening'
          case outline[10]
            when '1'
            when '11', '12', '13', '14'
              if !outline[20].nil? && outline[20][0..8] == "Check Out"
                specimen_location = 'SYNC'
              end
              specimen_status   = 'In Storage'
            when '16', '17'
              if !outline[20].nil? && outline[20][0..8] == 'Check Out'
                specimen_location = 'ITEK'
              end
              specimen_status   = 'In Storage'
            when '26'
              specimen_location = 'LCUS'
              specimen_status   = 'Exhausted'
            when '27', '28'
              if !outline[20].nil? && outline[20][0..8] == 'Check Out'
                specimen_location = 'BRIN'
                specimen_status   = 'In Storage'
              end

              if !outline[20].nil? && outline[20][0..7] == 'Check In'
                specimen_location = 'LCUS'
                specimen_status   = 'In Storage'
              end
            else
              specimen_location = "UNKNOWN TUBE #{outline[10]}"
              @logger.error "0 Tube ->#{outline[10]}<- not known for visit ->#{visit}<-."
          end
        when 'Screening BMA',
             'Relapse BMA'
          case outline[10]
            when '1'
            when '2', '4'
              specimen_location = 'LCUS'
            when '3'
              specimen_location = 'LCRT'
              specimen_status   = 'In Storage'
            when '5', '6', '7', '8'
              if !outline[20].nil? && outline[20][0..7] == 'Check In'
                specimen_location = 'LCUS'
                specimen_status   = 'In Storage'
              end
              if !outline[20].nil? && outline[20][0..7] == 'Check Out'
                specimen_location = 'BRIN'
                specimen_status   = 'In Storage'
              end
            else
              specimen_location = "UNKNOWN TUBE #{outline[10]}"
              @logger.error "1 Tube ->#{outline[10]}<- not known for visit ->#{visit}<-."
          end
        when 'Cycle 1 Day 7',
             'Cycle 1 Day 14',
             'Cycle 1 Day 21',
             'Cycle 2 Day 7',
             'Cycle 2 Day 14',
             'Cycle 2 Day 21',
             'Cycle 3 Day 14',
             'Cycle 9 Day 1',
             'Cycle 10 Day 1',
             'Cycle 13 Day 1',
             'Cycle 14 Day 1',
             'Cycle 15 Day 1',
             'Cycle 16 Day 1',
             'Cycle 17 Day 1',
             'Cycle 19 Day 1',
             'Cycle 20 Day 1',
             'Cycle 21 Day 1',
             'Cycle 22 Day 1',
             'Cycle 25 Day 1',
             'Cycle 26 Day 1',
             'Cycle 27 Day 1',
             'Cycle 28 Day 1',
             'Cycle 29 Day 1',
             'Cycle 30 Day 1',
             'Cycle 31 Day 1',
             'Cycle 32 Day 1',
             'Cycle 33 Day 1',
             'Cycle 34 Day 1',
             'Cycle 35 Day 1',
             'Cycle 36 Day 1',
             'Cycle 37 Day 1'
           case outline[10]
             when '1', '7'
             else
               specimen_location = "UNKNOWN TUBE #{outline[10]}"
               @logger.error "2 Tube ->#{outline[10]}<- not known for visit ->#{visit}<-."
           end
        when 'Cycle 1 Day 1',
             'Cycle 2 Day 1',
             'Cycle 3 Day 1',
             'Cycle 4 Day 1',
             'Cycle 5 Day 1',
             'Cycle 6 Day 1',
             'Cycle 7 Day 1',
             'Cycle 8 Day 1',
             'Cycle 11 Day 1',
             'Cycle 12 Day 1',
             'Cycle 18 Day 1',
             'Cycle 23 Day 1',
             'Cycle 24 Day 1',
             'End of Treatment',
             'Progressive-free Survival PFS'
          case outline[10]
            when '1'
            when '7', '8', '9', '10', '11', '12', '13', '14'
              if !outline[20].nil? && outline[20][0..8] == "Check Out"
                specimen_location = 'SYNC'
              end
              specimen_status   = 'In Storage'
            else
              specimen_location = "UNKNOWN TUBE #{outline[10]}"
              @logger.error "3 Tube ->#{outline[10]}<- not known for visit ->#{visit}<-."
          end
        when 'MRD Panel'
           case outline[10]
             when '3'
             else
               puts 'AND Here ... bummer'
               specimen_location = "UNKNOWN TUBE #{outline[10]}"
               @logger.error "4 Tube ->#{outline[10]}<- not known for visit ->#{visit}<-."
           end
        when 'Unscheduled'
          case outline[10]
            when '1', '18', '19', '46'
            when '2', '4', '21'
              specimen_location = 'LCUS'
            when '5', '6', '7', '8'
              specimen_status = 'In Storage'
              if (outline[20].strip)[-4..-1] == 'RNA,'
                specimen_location = 'BRIN'
              else
                if !outline[20].nil? && outline[20][0..7] == 'Check In'
                  specimen_location = 'LCUS'
                end
                if !outline[20].nil? && outline[20][0..7] == 'Check Out'
                  specimen_location = 'SYNC'
                end
              end
            when '9', '10', '11', '12', '13', '14'
              specimen_status = 'In Storage'
              if !outline[20].nil? && outline[20][0..7] == 'Check Out'
                specimen_location = 'SYNC'
              end
            when '16', '17'
              specimen_status = 'In Storage'
              if !outline[20].nil? && outline[20][0..7] == 'Check Out'
                specimen_location = 'ITEK'
              end
            when '26'
              specimen_location = 'LCUS'
              specimen_status   = 'Exhausted'
            when '27', '28'
              if !outline[20].nil? && outline[20][0..8] == 'Check Out'
                specimen_location = 'BRIN'
                specimen_status   = 'In Storage'
              end

              if !outline[20].nil? && outline[20][0..7] == 'Check In'
                specimen_location = 'LCUS'
                specimen_status   = 'In Storage'
              end
            else
              specimen_location = "UNKNOWN TUBE #{outline[10]}"
              @logger.error "5 Tube ->#{outline[10]}<- not known for visit ->#{visit}<-."
          end
        else
          specimen_location = "UNKNOWN VISIT #{visit}"
          @logger.error "Visit ->#{visit}<- not known."
      end

      values_clause <<
              "('C16021',"                                               + # study_protocol_id
              " #{outline[2].rjust(5, '0').insert_value}"                + # site_number
              " #{outline[4].insert_value}"                              + # subject_code
              " #{outline[6].insert_value}"                              + # subject_gender
              " #{date_of_birth}"                                        + # subject_DOB
              " STR_TO_DATE(#{outline[7].insert_value} '%c/%e/%Y %T'),"  + # specimen_collect_date
              " STR_TO_DATE(#{outline[7].insert_value} '%c/%e/%Y %T'),"  + # specimen_collect_time
              " #{receive_date}"                                         + # specimen_receive_datetime
              " #{treatment.insert_value}"                               + # treatment
              " #{arm.insert_value}"                                     + # arm
              " #{visit.insert_value}"                                   + # visit_name
              " #{specimen_barcode.insert_value}"                        + # specimen_barcode
              " #{specimen_barcode.insert_value}"                        + # specimen_identifier
              " #{specimen_type.insert_value}"                           + # specimen_type
              " #{specimen_barcode.insert_value}"                        + # specimen_name
              ' NULL,'                                                   + # specimen_designation,
              ' NULL,'                                                   + # specimen_designation_detaill
              " #{specimen_parent.insert_value}"                         + # specimen_parent
              " #{specimen_is_child.insert_value}"                       + # specimen_ischild
              ' NULL,'                                                   + # specimen_condition
              " #{specimen_status.insert_value}"                         + # specimen_status
              " #{outline[26].insert_value}"                             + # specimen_comment
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
