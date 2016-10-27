require 'mysql2'

class C16019_site

  COUNTRY_CAPITAL =
  {
    :Argentina       => 'Buenos Aires',
    :Australia       => 'Canberra',
    :Austria         => 'Vienna',
    :Belgium         => 'Brussles',
    :Brazil          => 'Brasília',
    :CzechRepublic   => 'Prague',
    :Denmark         => 'Copenhagen',
    :France          => 'Paris',
    :Germany         => 'Berlin',
    :Greece          => 'Athens',
    :Hungary         => 'Budapest',
    :Israel          => 'Jerusalem',
    :Italy           => 'Rome',
    :Japan           => 'Tokyo',
    :Netherlands     => 'Amsterdam',
    :Norway          => 'Oslo',
    :Poland          => 'Warsaw',
    :Portugal        => 'Lisbon',
    :RepublicofKorea => 'Seoul',
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
              "('C16019',"                                            + # study_protocol_id
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

class C16019_subject

  def initialize(logger)
    @logger = logger
    @logger.debug "#{self.class.name} filer Initialized"
  end

  def reader(inbound_file)
    @logger.debug "File name is ->#{inbound_file}<-"
    @logger.info "#{self.class.name} reader start"
    @inbound_lines  = CSV.read(inbound_file, headers: true, skip_blanks: true, skip_lines: '\r', encoding:'windows-1256:utf-8')
    inbound_file2 = inbound_file.dup
    @inbound_lines2 = CSV.read(inbound_file2.insert(12, '2'), headers: true, skip_blanks: true, skip_lines: '\r', encoding:'windows-1256:utf-8')
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
    @logger.info 'R668AD1021_site writer start'

    if @processing_lines.count == 0
      logger.info ("Nothing to insert :(")
      exit 0
    end

    values_clause = Array.new
    @myrow = Array.new

    @processing_lines.each do |outline|

      @myrow = @inbound_lines2.select {|this_line| this_line[6] == outline[4]}
      subject_treatment   = 'Ixazomib (MLN9708)'.insert_value
      subject_arm         = 'Ixazomib or Placebo'.insert_value
      date_of_birth       = (outline[6].nil?)  ? ' NULL,' : "STR_TO_DATE(#{outline[6].insert(-3, '19').insert_value} '%e-%b-%Y'),"

      if @myrow.count == 1
        gender            = @myrow[0][33].insert_value
        initials          = @myrow[0][31].insert_value
        race              = @myrow[0][46].insert_value
        ethnicity         = @myrow[0][43].insert_value
        icf_signing_date  = "STR_TO_DATE('#{@myrow[0][49]}', '%d %b %Y'),"
      else
        gender            = 'NULL,'
        initials          = 'NULL,'
        race              = 'NULL,'
        ethnicity         = 'NULL,'
        icf_signing_date  = 'NULL,'

      end

      values_clause <<
              "('C16019',"                                          + # study_protocol_id
              " #{outline[2].insert_value}"                         + # site_number
              " #{outline[4].insert_value}"                         + # subject_code
              " #{outline[4].insert_value}"                         + # subject_external_id
              ' NULL,'                                              + # randomization_number
              " #{gender}"                                          + # gender
              " #{initials}"                                        + # initials
              " #{outline[16].insert_value}"                        + # enrollment_status
              " #{date_of_birth}"                                   + # date_of_birth 1-Jan-34
              ' NULL,'                                              + # address
              ' NULL,'                                              + # city
              ' NULL,'                                              + # state
              ' NULL,'                                              + # region
              ' NULL,'                                              + # country
              ' NULL,'                                              + # postcode
              " #{race}"                                            + # primary_race
              ' NULL,'                                              + # secondary_race
              " #{ethnicity}"                                       + # ethnicity
              " #{subject_treatment}"                               + # treatment
              " #{subject_arm}"                                     + # arm
              " #{icf_signing_date}"                                + # ICF_signing_date
              ' NULL,'                                              + # ICF_withdrawl_date
              " '#{vendor}'"                                        + # vendor_code
              ")"
    end

    @logger.info "#{self.class.name} writer end"
    values_clause
  end
end

class C16019_Inv
  SPECIMEN_HUB = {
      :Argentina       => 'LCUS',
      :Australia       => 'LCAP',
      :Austria         => 'LCEu',
      :Belgium         => 'LCEu',
      :Brazil          => 'LCUS',
      :CzechRepublic   => 'LCEU',
      :Denmark         => 'LCEU',
      :France          => 'LCEU',
      :Germany         => 'LCEU',
      :Greece          => 'LCEU',
      :Hungary         => 'LCEU',
      :Israel          => 'LCEU',
      :Italy           => 'LCEU',
      :Japan           => 'LCAP',
      :Netherlands     => 'LCEU',
      :Norway          => 'LCEU',
      :Poland          => 'LCEU',
      :Portugal        => 'LCEU',
      :RepublicofKorea => 'LCAP',
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
      :UnitedStates    => 'LCUS',
  }.freeze

  SPECIMEN_TYPE = {
      :BlockSectioningofSlidesSectioned            => 'Tissue',
      :BlockSectioningSlideSectioningDate          => 'Tissue',
      :BMACD138PostIsoCD19Lymphs                   => 'Bone Marrow Aspirate',
      :BMACD138PostIsoCD19PCs            	         => 'Bone Marrow Aspirate',
      :BMACD138PostIsoLymphsViable                 => 'Bone Marrow Aspirate',
      :BMACD138PostIsoPCsViable  	                 => 'Bone Marrow Aspirate',
      :BMACD138PostIsoTNCViable         	         => 'Bone Marrow Aspirate',
      :BMACD138PostIsoViableTotal                  => 'Bone Marrow Aspirate',
      :BMACD138PostIso138RGNCountViable            => 'Bone Marrow Aspirate',
      :BMACD138PostIso38RGNCountViable             => 'Bone Marrow Aspirate',
      :BMACD138PostIsoPCRGNCountViable             => 'Bone Marrow Aspirate',
      :BMACD138PostIsoTNCRGNCountViable            => 'Bone Marrow Aspirate',
      :BMACD138PostIsoTotalCount                   => 'Bone Marrow Aspirate',
      :BMACD138PostIsoViableRGNCountTot            => 'Bone Marrow Aspirate',
      :BMACD138PreIsoCD19Lymphs                    => 'Bone Marrow Aspirate',
      :BMACD138PreIsoCD19PCs  	                   => 'Bone Marrow Aspirate',
      :BMACD138PreIsoLymphsViable      	           => 'Bone Marrow Aspirate',
      :BMACD138PreIsoPCsViable  	                 => 'Bone Marrow Aspirate',
      :BMACD138PreIsoTNCViable  	                 => 'Bone Marrow Aspirate',
      :BMACD138PreIsoViableTotal       	           => 'Bone Marrow Aspirate',
      :BMACD138PreIso138RGNCountViable  	         => 'Bone Marrow Aspirate',
      :BMACD138PreIso38RGNCountViable  	           => 'Bone Marrow Aspirate',
      :BMACD138PreIsoPCRGNCountViable  	           => 'Bone Marrow Aspirate',
      :BMACD138PreIsoTNCRGNCounTViable  	         => 'Bone Marrow Aspirate',
      :BMACD138PreIsoTotalCount  	                 => 'Bone Marrow Aspirate',
      :BMACD138PreIsoViableRGNCounTTot  	         => 'Bone Marrow Aspirate',
      :BMATotalCellCountcellsresidualpellet        => 'Bone Marrow Aspirate',
      :BMATotalCellCountAspirateVolumeUponReceipt  => 'Bone Marrow Aspirate',
      :BMATotalCellCountNmbrcellssavedforFISH  	   => 'Bone Marrow Aspirate',
      :BMATotalCellCountTotalCellCount             => 'Bone Marrow Aspirate',
      :BMATotalCellCountTotalcellcountFISH  	     => 'Bone Marrow Aspirate',
      :BMATotalCellCountViableCellCount  	         => 'Bone Marrow Aspirate',
      :MMCytogeneticsFISHCellsCountedAmp1q21  	   => 'Enriched Plasma Cells',
      :MMCytogeneticsFISHCellscountedDel17  	     => 'Enriched Plasma Cells',
      :MMCytogeneticsFISHCellscountedt1416  	     => 'Enriched Plasma Cells',
      :MMCytogeneticsFISHCellscountedt414  	       => 'Enriched Plasma Cells',
      :MMCytogeneticsFISHCommentAmp1q21  	         => 'Enriched Plasma Cells',
      :MMCytogeneticsFISHCommentDel17  	           => 'Enriched Plasma Cells',
      :MMCytogeneticsFISHCommentt1416  	           => 'Enriched Plasma Cells',
      :MMCytogeneticsFISHCommentt414  	           => 'Enriched Plasma Cells',
      :MMCytogeneticsFISHResultAmp1q21  	         => 'Enriched Plasma Cells',
      :MMCytogeneticsFISHResultDel17  	           => 'Enriched Plasma Cells',
      :MMCytogeneticsFISHResultt1416  	           => 'Enriched Plasma Cells',
      :MMCytogeneticsFISHResultt414  	             => 'Enriched Plasma Cells',
      :MRDFlowBoneMarrowSpecimenvolume  	         => 'Bone Marrow Aspirate',
      :MRDFlowBoneMarrowTotalCellCount             => 'Bone Marrow Aspirate',
      :MRDFlowBoneMarrowTotalCellCountFlow  	     => 'Bone Marrow Aspirate',
      :MRDFlowBoneMarrowWhitebloodcellcount  	     => 'Bone Marrow Aspirate',
      :MRDPeripheralBloodNumberfrozencellsaliquot  => 'Bone Marrow Aspirate',
      :MRDPeripheralBloodTotalbloodvolumecollected => 'Bone Marrow Aspirate',
      :MRDPeripheralBloodTotalCellcount            => 'Bone Marrow Aspirate',
      :WBDNAExtractionDNAConcentration  	         => 'Whole Blood',
      :WBDNAExtractionDNAExtractionDate  	         => 'Whole Blood',
      :WBDNAExtractionDNAPurity                    => 'Whole Blood',
      :WBDNAExtractionDNAYield                     => 'Whole Blood',
      :OfWBDNAAliquots                             => 'Whole Blood',
      :cellsMRDresidbldfrz  	                     => 'Bone Marrow Aspirate',
      :CirculatingProtStorage1                     => 'Plasma',
      :CirculatingProtStorage2  	                 => 'Plasma',
      :CirculatingProtStorage3  	                 => 'Plasma',
      :CirculatingProtStorage4  	                 => 'Plasma',
      :CirculatingProtStorage5  	                 => 'Plasma',
      :FinalVolume  	                             => 'Whole Blood',
      :FlowCommentMRDFlow1                         => 'Bone Marrow Aspirate',
      :BMASlideCaseBlock                           => 'Bone Marrow Aspirate',
      :BMASlideCaseBlock2                          => 'Bone Marrow Aspirate',
      :FISHSlideStorage1  	                       => 'Bone Marrow Aspirate',
      :FISHSlideStorage10                          => 'Bone Marrow Aspirate',
      :FISHSlideStorage2  	                       => 'Bone Marrow Aspirate',
      :FISHSlideStorage3  	                       => 'Bone Marrow Aspirate',
      :FISHSlideStorage4  	                       => 'Bone Marrow Aspirate',
      :FISHSlideStorage5  	                       => 'Bone Marrow Aspirate',
      :FISHSlideStorage6  	                       => 'Bone Marrow Aspirate',
      :FISHSlideStorage7  	                       => 'Bone Marrow Aspirate',
      :FISHSlideStorage8  	                       => 'Bone Marrow Aspirate',
      :FISHSlideStorage9                           => 'Bone Marrow Aspirate',
      :MRDBMASlide1  	                             => 'Bone Marrow Aspirate',
      :MRDBMASlide10  	                           => 'Bone Marrow Aspirate',
      :MRDBMASlide11  	                           => 'Bone Marrow Aspirate',
      :MRDBMASlide12  	                           => 'Bone Marrow Aspirate',
      :MRDBMASlide13  	                           => 'Bone Marrow Aspirate',
      :MRDBMASlide14  	                           => 'Bone Marrow Aspirate',
      :MRDBMASlide15  	                           => 'Bone Marrow Aspirate',
      :MRDBMASlide16  	                           => 'Bone Marrow Aspirate',
      :MRDBMASlide17  	                           => 'Bone Marrow Aspirate',
      :MRDBMASlide18  	                           => 'Bone Marrow Aspirate',
      :MRDBMASlide19  	                           => 'Bone Marrow Aspirate',
      :MRDBMASlide2  	                             => 'Bone Marrow Aspirate',
      :MRDBMASlide20  	                           => 'Bone Marrow Aspirate',
      :MRDBMASlide21  	                           => 'Bone Marrow Aspirate',
      :MRDBMASlide22  	                           => 'Bone Marrow Aspirate',
      :MRDBMASlide23  	                           => 'Bone Marrow Aspirate',
      :MRDBMASlide24  	                           => 'Bone Marrow Aspirate',
      :MRDBMASlide25  	                           => 'Bone Marrow Aspirate',
      :MRDBMASlide26  	                           => 'Bone Marrow Aspirate',
      :MRDBMASlide27  	                           => 'Bone Marrow Aspirate',
      :MRDBMASlide28  	                           => 'Bone Marrow Aspirate',
      :MRDBMASlide29  	                           => 'Bone Marrow Aspirate',
      :MRDBMASlide3  	                             => 'Bone Marrow Aspirate',
      :MRDBMASlide30  	                           => 'Bone Marrow Aspirate',
      :MRDBMASlide4  	                             => 'Bone Marrow Aspirate',
      :MRDBMASlide5  	                             => 'Bone Marrow Aspirate',
      :MRDBMASlide6  	                             => 'Bone Marrow Aspirate',
      :MRDBMASlide7  	                             => 'Bone Marrow Aspirate',
      :MRDBMASlide8  	                             => 'Bone Marrow Aspirate',
      :MRDBMASlide9  	                             => 'Bone Marrow Aspirate',
      :MRDBMASlideCaseBlock  	                     => 'Bone Marrow Aspirate',
      :MRDBMABankedBiopsyDt  	                     => 'Bone Marrow Aspirate',
      :MRDFlowBMAComment  	                       => 'Bone Marrow Aspirate',
      :MRDFlowFreshBMACollDt  	                   => 'Bone Marrow Aspirate',
      :MRDFlowFreshBMACollTm  	                   => 'Bone Marrow Aspirate',
      :MRDPeripBloodCollDate  	                   => 'Bone Marrow Aspirate',
      :MRDPeripBloodCollTime  	                   => 'Bone Marrow Aspirate',
      :MRDPeripBloodStorage1  	                   => 'Bone Marrow Aspirate',
      :MRDPeripBloodStorage2  	                   => 'Bone Marrow Aspirate',
      :Numcellsbloodaliquot1  	                   => 'Bone Marrow Aspirate',
      :Numcellsbloodaliquot2  	                   => 'Bone Marrow Aspirate',
      :ResidualMRDFlow  	                         => 'Bone Marrow Aspirate',
      :TotalCellcountPBlood  	                     => 'Bone Marrow Aspirate',
      :TotlbloodvolcollectPBlood  	               => 'Bone Marrow Aspirate',
      :WBDNAExtractionStor  	                     => 'Whole Blood',
      :WBDNAExtractionStorage  	                   => 'Whole Blood',
      :WBDNAStorage1  	                           => 'DNA',
      :WBDNAStorage2  	                           => 'DNA',
  }.freeze

 SPECIMEN_DESIGNATION = {
      :BlockSectioningofSlidesSectioned            => 'Block',
      :BlockSectioningSlideSectioningDate          => 'Block',
      :MMCytogeneticsFISHCellsCountedAmp1q21       => 'MM Cytogenetics FISH',
      :MMCytogeneticsFISHCellscountedDel17         => 'MM Cytogenetics FISH',
      :MMCytogeneticsFISHCellscountedt1416         => 'MM Cytogenetics FISH',
      :MMCytogeneticsFISHCellscountedt414          => 'MM Cytogenetics FISH',
      :MMCytogeneticsFISHCommentAmp1q21            => 'MM Cytogenetics FISH',
      :MMCytogeneticsFISHCommentDel17              => 'MM Cytogenetics FISH',
      :MMCytogeneticsFISHCommentt1416              => 'MM Cytogenetics FISH',
      :MMCytogeneticsFISHCommentt414               => 'MM Cytogenetics FISH',
      :MMCytogeneticsFISHResultAmp1q21             => 'MM Cytogenetics FISH',
      :MMCytogeneticsFISHResultDel17               => 'MM Cytogenetics FISH',
      :MMCytogeneticsFISHResultt1416               => 'MM Cytogenetics FISH',
      :MMCytogeneticsFISHResultt414                => 'MM Cytogenetics FISH',
      :MRDFlowBoneMarrowSpecimenvolume             => 'MRD Flow',
      :MRDFlowBoneMarrowTotalCellcount             => 'MRD Flow',
      :MRDFlowBoneMarrowTotalCellCountFlow         => 'MRD Flow',
      :MRDFlowBoneMarrowWhitebloodcellcount        => 'MRD Flow',
      :MRDPeripheralBloodNumberfrozencellsaliquot  => 'MRD Flow',
      :MRDPeripheralBloodTotalbloodvolumecollected => 'MRD Flow',
      :MRDPeripheralBloodTotalCellcount            => 'MRD Flow',
      :WBDNAExtractionDNAConcentration             => 'Genomic DNA',
      :WBDNAExtractionDNAExtractionDate            => 'Genomic DNA',
      :WBDNAExtractionDNAPurity                    => 'Genomic DNA',
      :WBDNAExtractionDNAYield                     => 'Genomic DNA',
      :OfWBDNAAliquots                             => 'Genomic DNA',
      :cellsMRDresidbldfrz                         => 'MRD Flow',
      :CirculatingProtStorage1                     => 'Circulating Prot Plasma',
      :CirculatingProtStorage2                     => 'Circulating Prot Plasma',
      :CirculatingProtStorage3                     => 'Circulating Prot Plasma',
      :CirculatingProtStorage4                     => 'Circulating Prot Plasma',
      :CirculatingProtStorage5                     => 'Circulating Prot Plasma',
      :FinalVolume                                 => 'Genomic DNA',
      :FISHSlideStorage1                           => 'FISH Slide Storage',
      :FISHSlideStorage10                          => 'FISH Slide Storage',
      :FISHSlideStorage2                           => 'FISH Slide Storage',
      :FISHSlideStorage3                           => 'FISH Slide Storage',
      :FISHSlideStorage4                           => 'FISH Slide Storage',
      :FISHSlideStorage5                           => 'FISH Slide Storage',
      :FISHSlideStorage6                           => 'FISH Slide Storage',
      :FISHSlideStorage7                           => 'FISH Slide Storage',
      :FISHSlideStorage8                           => 'FISH Slide Storage',
      :FISHSlideStorage9                           => 'FISH Slide Storage',
      :MRDBMASlide1                                => 'FISH Slide Storage',
      :MRDBMASlide10                               => 'FISH Slide Storage',
      :MRDBMASlide11                               => 'FISH Slide Storage',
      :MRDBMASlide12                               => 'FISH Slide Storage',
      :MRDBMASlide13                               => 'FISH Slide Storage',
      :MRDBMASlide14                               => 'FISH Slide Storage',
      :MRDBMASlide15                               => 'FISH Slide Storage',
      :MRDBMASlide16                               => 'FISH Slide Storage',
      :MRDBMASlide17                               => 'FISH Slide Storage',
      :MRDBMASlide18                               => 'FISH Slide Storage',
      :MRDBMASlide19                               => 'FISH Slide Storage',
      :MRDBMASlide2                                => 'FISH Slide Storage',
      :MRDBMASlide20                               => 'FISH Slide Storage',
      :MRDBMASlide21                               => 'FISH Slide Storage',
      :MRDBMASlide22                               => 'FISH Slide Storage',
      :MRDBMASlide23                               => 'FISH Slide Storage',
      :MRDBMASlide24                               => 'FISH Slide Storage',
      :MRDBMASlide25                               => 'FISH Slide Storage',
      :MRDBMASlide26                               => 'FISH Slide Storage',
      :MRDBMASlide27                               => 'FISH Slide Storage',
      :MRDBMASlide28                               => 'FISH Slide Storage',
      :MRDBMASlide29                               => 'FISH Slide Storage',
      :MRDBMASlide3                                => 'FISH Slide Storage',
      :MRDBMASlide30                               => 'FISH Slide Storage',
      :MRDBMASlide4  	                             => 'FISH Slide Storage',
      :MRDBMASlide5                                => 'FISH Slide Storage',
      :MRDBMASlide6                                => 'FISH Slide Storage',
      :MRDBMASlide7                                => 'FISH Slide Storage',
      :MRDBMASlide8                                => 'FISH Slide Storage',
      :MRDBMASlide9                                => 'FISH Slide Storage',
      :MRDBMASlideCaseBlock                        => 'MRD BMA Slide Case/Block',
      :MRDBMABankedBiopsyDt                        => 'MRD BMA(Banked) Biopsy Dt',
      :MRDFlowBMAComment                           => 'MRD Flow',
      :MRDFlowFreshBMACollDt                       => 'MRD Flow',
      :MRDFlowFreshBMACollTm                       => 'MRD Flow',
      :MRDPeripBloodCollDate                       => 'MRD Flow',
      :MRDPeripBloodCollTime                       => 'MRD Flow',
      :MRDPeripBloodStorage1                       => 'MRD Perip Blood Storage',
      :MRDPeripBloodStorage2                       => 'MRD Perip Blood Storage',
      :Numcellsbloodaliquot1                       => 'MRD Flow',
      :Numcellsbloodaliquot2                       => 'MRD Flow',
      :ResidualMRDFlow                             => 'MRD Flow',
      :TotalCellcountPBlood                        => 'MRD Flow',
      :TotlbloodvolcollectPBlood                   => 'MRD Flow',
      :WBDNAExtractionStor                         => 'Genomic DNA',
      :WBDNAExtractionStorage                      => 'Genomic DNA',
      :WBDNAStorage1                               => 'WB DNA Storage',
      :WBDNAStorage2                               => 'WB DNA Storage',
 }.freeze
      
 SPECIMEN_DESIGNATION_DETAIL = {
     :CirculatingProtStorage1 =>	'Circulating Prot Storage1',
     :CirculatingProtStorage2 =>	'Circulating Prot Storage2',
     :CirculatingProtStorage3 =>	'Circulating Prot Storage3',
     :CirculatingProtStorage4 =>	'Circulating Prot Storage4',
     :CirculatingProtStorage5 =>	'Circulating Prot Storage5',
     :FISHSlideStorage1       =>	'FISH Slide Storage 1',
     :FISHSlideStorage10  	  =>	'FISH Slide Storage 10',
     :FISHSlideStorage2  	    =>	'FISH Slide Storage 2',
     :FISHSlideStorage3  	    =>	'FISH Slide Storage 3',
     :FISHSlideStorage4  	    =>	'FISH Slide Storage 4',
     :FISHSlideStorage5  	    =>	'FISH Slide Storage 5',
     :FISHSlideStorage6  	    =>	'FISH Slide Storage 6',
     :FISHSlideStorage7  	    =>	'FISH Slide Storage 7',
     :FISHSlideStorage8  	    =>	'FISH Slide Storage 8',
     :FISHSlideStorage9  	    =>	'FISH Slide Storage 9',
     :MRDBMASlide1            =>	'MRD BMA Slide 1',
     :MRDBMASlide10           =>	'MRD BMA Slide 10',
     :MRDBMASlide11           =>	'MRD BMA Slide 11',
     :MRDBMASlide12           =>	'MRD BMA Slide 12',
     :MRDBMASlide13           =>	'MRD BMA Slide 13',
     :MRDBMASlide14           =>	'MRD BMA Slide 14',
     :MRDBMASlide15           =>	'MRD BMA Slide 15',
     :MRDBMASlide16           =>	'MRD BMA Slide 16',
     :MRDBMASlide17           =>	'MRD BMA Slide 17',
     :MRDBMASlide18           =>	'MRD BMA Slide 18',
     :MRDBMASlide19           =>	'MRD BMA Slide 19',
     :MRDBMASlide2  	        =>	'MRD BMA Slide 2',
     :MRDBMASlide20           =>	'MRD BMA Slide 20',
     :MRDBMASlide21           =>	'MRD BMA Slide 21',
     :MRDBMASlide22           =>	'MRD BMA Slide 22',
     :MRDBMASlide23           =>	'MRD BMA Slide 23',
     :MRDBMASlide24           =>	'MRD BMA Slide 24',
     :MRDBMASlide25           =>	'MRD BMA Slide 25',
     :MRDBMASlide26           =>	'MRD BMA Slide 26',
     :MRDBMASlide27           =>	'MRD BMA Slide 27',
     :MRDBMASlide28           =>	'MRD BMA Slide 28',
     :MRDBMASlide29           =>	'MRD BMA Slide 29',
     :MRDBMASlide3  	        =>	'MRD BMA Slide 3',
     :MRDBMASlide30           =>	'MRD BMA Slide 30',
     :MRDBMASlide4  	        =>	'MRD BMA Slide 4',
     :MRDBMASlide5  	        =>	'MRD BMA Slide 5',
     :MRDBMASlide6  	        =>	'MRD BMA Slide 6',
     :MRDBMASlide7  	        =>	'MRD BMA Slide 7',
     :MRDBMASlide8  	        =>	'MRD BMA Slide 8',
     :MRDBMASlide9  	        =>	'MRD BMA Slide 9',
     :MRDPeripBloodStorage1   =>	'MRD Perip Blood Storage1',
     :MRDPeripBloodStorage2   =>	'MRD Perip Blood Storage2',
     :WBDNAStorage1           =>	'WB DNA Storage 1',
     :WBDNAStorage2           =>	'WB DNA Storage 2'
  }.freeze

 VISIT_MAP = {
	  :BrazilCR         => 'Brazil - Complete Response',
	  :C12D1            => 'Cycle 12 Day 1',
	  :C13D1            => 'Cycle 13 Day 1',
	  :C14D1            => 'Cycle 14 Day 1',
	  :C24D1            => 'Cycle 24 Day 1',
	  :C6D1             => 'Cycle 6 Day 1',
	  :CR               => 'Complete Response',
	  :PreScreening     => 'Pre-Screening',
	  :Screening        => 'Screening',
	  :Unscheduled      => 'Unscheduled',
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

      if specline[0].nil?                  ||
         specline[0]        == ''          ||
         specline[7].nil?                  ||
         specline[7]        == ''          ||
         specline[17].strip == 'Cancelled' ||
         specline[17].strip == 'Assigned'  ||
         specline[2][0..3]  == 'DEMO'
        next
      end

      found = false
      @processing_lines.each do |distinct_line|

        # see if we actually need the line in the file
        if (specline[0]  == distinct_line[0])  &&     # Accession Number
           (specline[7] == distinct_line[7])        # Tube Number
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

      treatment             = 'Ixazomib (MLN9708)'
      arm                   = 'Ixazomib or Placebo'
      visit                 = VISIT_MAP[(outline[14].gsub(/[^a-zA-Z0-9]/, '')).to_sym]
      specimen_type         = SPECIMEN_TYPE[(((outline[15].nil? ? '' : outline[15]) + (outline[16].nil? ? '' : outline[16])).gsub(/[^a-zA-Z0-9]/, '')).to_sym]
      specimen_designation  = SPECIMEN_DESIGNATION[(((outline[15].nil? ? '' : outline[15]) + (outline[16].nil? ? '' : outline[16])).gsub(/[^a-zA-Z0-9]/, '')).to_sym]
      specimen_ddetail      = SPECIMEN_DESIGNATION_DETAIL[(((outline[15].nil? ? '' : outline[15]) + (outline[16].nil? ? '' : outline[16])).gsub(/[^a-zA-Z0-9]/, '')).to_sym]
      receive_date          = (outline[13].nil?)  ? ' NULL,' : " STR_TO_DATE(#{outline[13].insert_value} '%c/%e/%Y %H:%i'),"
      specimen_barcode      = outline[1] + '-' + outline[7].rjust(2,'0')
      specimen_hub          = SPECIMEN_HUB[(outline[3].gsub(/[^a-zA-Z0-9]/, '')).to_sym]
      specimen_status       = "Exhausted"
      specimen_location     = specimen_hub
      specimen_is_child     = 'No'
      specimen_parent       = 'NULL'

      if specimen_type.nil?
        puts "->#{specimen_barcode}<-"
        puts "->#{outline[15]}<- -><-#{outline[16]}"
        puts "->#{(((outline[15].nil? ? '' : outline[15]) + (outline[16].nil? ? '' : outline[16])).gsub(/[^a-zA-Z0-9]/, ''))}<-"
      end

# Figure our lineage

      if !outline[9].nil?
        specimen_is_child = 'Yes'
        specimen_parent   = outline[1] + '-' + outline[9].rjust(2,'0')
      end

# set specimen status and vendor code base upon visit and tube mapping.

      case visit
        when 'Pre-Screening',
             'Cycle 13 Day 1',
             'Brazil - Complete Response'         # use default
        when 'Complete Response'
          case outline[7]
            when '1', '8'                         # use default
            when '2', '3', '6'
              specimen_location = 'LCUS'
              if !outline[18].nil? && !outline[20].nil? && outline[20][0..8] == "Check Out"
                specimen_location = 'ADPT'
              end
              specimen_status   = 'In Storage'
            else
              specimen_location = "UNKNOWN TUBE #{outline[10]}"
              @logger.error "0 Tube ->#{outline[10]}<- not known for visit ->#{visit}<-."
          end
        when 'Cycle 12 Day 1'
          case outline[7]
            when '13', '20'                         # use default
            when '14', '15', '18'
              specimen_location = 'LCUS'
              if !outline[18].nil? && !outline[20].nil? && outline[20][0..8] == "Check Out"
                specimen_location = 'ADPT'
              end
              specimen_status   = 'In Storage'
            else
              specimen_location = "UNKNOWN TUBE #{outline[10]}"
              @logger.error "0 Tube ->#{outline[10]}<- not known for visit ->#{visit}<-."
          end
        when 'Cycle 14 Day 1'
          case outline[7]
            when '98', '20'                         # use default
            when '18'
              specimen_location = 'LCUS'
              if !outline[18].nil? && !outline[20].nil? && outline[20][0..8] == "Check Out"
                specimen_location = 'ADPT'
              end
              specimen_status   = 'In Storage'
            else
              specimen_location = "UNKNOWN TUBE #{outline[10]}"
              @logger.error "0 Tube ->#{outline[10]}<- not known for visit ->#{visit}<-."
          end
        when 'Cycle 24 Day 1',
             'Cycle 6 Day 1'
          case outline[7]
            when '13', '20'                         # use default
            when '18', '14', '15'
              specimen_location = 'LCUS'
              if !outline[18].nil? && !outline[20].nil? && outline[20][0..8] == "Check Out"
                specimen_location = 'ADPT'
              end
              specimen_status   = 'In Storage'
            else
              specimen_location = "UNKNOWN TUBE #{outline[10]}"
              @logger.error "0 Tube ->#{outline[10]}<- not known for visit ->#{visit}<-."
          end
        when 'Screening'
          case outline[7]
            when '27', '32', '54', '55'           # use default
            when '23'
              specimen_location = 'LCUS'
            when '18', '19', '20', '21', '22'
              specimen_location = 'LCUS'
              if !outline[18].nil? && !outline[20].nil? && outline[20][0..8] == "Check Out"
                specimen_location = 'ITEK'
              end
              specimen_status   = 'In Storage'
            when '24', '25', '28', '29', '31', '33', '34', '35', '36', '37', '38', '39',
                 '40', '41', '42', '43', '44', '45', '46', '47', '48', '49', '50', '51',
                 '52', '53', '56', '57', '58', '59', '60', '61', '62', '63', '64', '65'
              specimen_location = 'LCUS'
              if !outline[18].nil? && !outline[20].nil? && outline[20][0..8] == "Check Out"
                specimen_location = 'ADPT'
              end
              specimen_status   = 'In Storage'
            else
              specimen_location = "UNKNOWN TUBE #{outline[10]}"
              @logger.error "0 Tube ->#{outline[10]}<- not known for visit ->#{visit}<-."
          end
        when 'Unscheduled'
          case outline[7]
            when '13', '20', '27', '32'           # use default
              if outline[7] == '20' && specimen_type == 'Plasma'
                if !outline[20].nil? && outline[20][0..8] == "Check Out"
                  specimen_location = 'ITEK'
                else
                  specimen_location = 'LCUS'
                end
                specimen_status   = 'In Storage'
              end
            when '23', '26'
              specimen_location = 'LCUS'
            when '19', '20', '21', '22', '23'
              specimen_location = 'LCUS'
              if !outline[18].nil? && !outline[20].nil? && outline[20][0..8] == "Check Out"
                specimen_location = 'ITEK'
              end
              specimen_status   = 'In Storage'
            when '18', '24', '25', '28', '29', '31', '33', '34', '35', '36', '37', '38', '39',
                '40', '41', '42', '43', '44', '45', '46', '47', '48', '49', '50', '51',
                '52', '53', '56', '57', '58', '59', '60', '61', '62', '63', '64', '65'
              specimen_location = 'LCUS'
              if !outline[18].nil? && !outline[20].nil? && outline[20][0..8] == "Check Out"
                if specimen_type == 'Plasma'
                  specimen_location = 'ITEK'
                else
                  specimen_location = 'ADPT'
                end
              end
              specimen_status   = 'In Storage'
            else
              specimen_location = "UNKNOWN TUBE #{outline[10]}"
              @logger.error "0 Tube ->#{outline[10]}<- not known for visit ->#{visit}<-."
           end
        else
          specimen_location = "UNKNOWN VISIT #{visit}"
          @logger.error "Visit ->#{visit}<- not known."
      end

      values_clause <<
              "('C16014',"                                               + # study_protocol_id
              " #{outline[2].rjust(5, '0').insert_value}"                + # site_number
              " #{outline[0].insert_value}"                              + # subject_code
              ' NULL,'                                                   + # subject_gender
              ' NULL,'                                                   + # subject_DOB
              " STR_TO_DATE(#{outline[6].insert_value} '%c/%e/%Y %H:%i'),"  + # specimen_collect_date
              " STR_TO_DATE(#{outline[6].insert_value} '%c/%e/%Y %H:%i'),"  + # specimen_collect_time
              " #{receive_date}"                                         + # specimen_receive_datetime
              " #{treatment.insert_value}"                               + # treatment
              " #{arm.insert_value}"                                     + # arm
              " #{visit.insert_value}"                                   + # visit_name
              " #{specimen_barcode.insert_value}"                        + # specimen_barcode
              " #{specimen_barcode.insert_value}"                        + # specimen_identifier
              " #{specimen_type.insert_value}"                           + # specimen_type
              " #{specimen_barcode.insert_value}"                        + # specimen_name
              " #{specimen_designation.insert_value}"                    + # specimen_designation,
              " #{specimen_ddetail.insert_value}"                        + # specimen_designation_detaill
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
