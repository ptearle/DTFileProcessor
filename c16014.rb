require 'mysql2'

class C16014_siteRETRO

  COUNTRY_CAPITAL =
      {
          :Belgium      => 'Brussels',
          :Canada       => 'Ottawa',
          :France       => 'Paris',
          :Japan        => 'Tokyo',
          :NewZealand   => 'Wellington',
          :Russia       => 'Moscow',
          :SouthKorea   => 'Seoul',
          :UnitedStates => 'Washington'
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

      country_capital = (outline[3].nil?)  ? ' NULL,' : "#{COUNTRY_CAPITAL[(outline[3].gsub(/[^a-zA-Z0-9]/, '')).to_sym]}".insert_value

      values_clause <<
          "('C16014',"                                            + # study_protocol_id
              " #{outline[2].insert_value}"                           + # site_number
              " #{outline[2].insert_value}"                           + # site_name
              ' NULL,'                                                + # site_address
              " #{country_capital}"                                   + # site_city
              ' NULL,'                                                + # site_state
              " #{outline[3].insert_value}"                           + # site_country
              ' NULL,'                                                + # site_postal_code
              ' NULL,'                                                + # site_phone
              ' NULL,'                                                + # site_fax
              ' NULL,'                                                + # site_FPFV
              ' NULL,'                                                + # site_LPLV
              ' NULL,'                                                + # planned_enrollment
              " #{outline[1].insert_value}"                           + # site_PI
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
class C16014_site

  COUNTRY_CAPITAL =
      {
          :Belgium           => 'Brussels',
          :Canada            => 'Ottawa',
          :France            => 'Paris',
          :Japan             => 'Tokyo',
          :NewZealand        => 'Wellington',
          :Russia            => 'Moscow',
          :KoreaRepublicof   => 'Seoul',
          :UnitedStates      => 'Washington',
          :RussianFederation => 'Moscow'
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
      lines += 1

      if specline['ns2:Status'] == 'Disqualified' or
         specline['ns2:Status'] == 'Withdrawn' or
         specline['ns2:Status'] == 'Void'
        next
      end

      @processing_lines << specline
      num_distinct_lines += 1
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

      country_capital = (outline['ns2:Study_Country_Name'].nil?)  ? ' NULL,' : "#{COUNTRY_CAPITAL[(outline['ns2:Study_Country_Name'].gsub(/[^a-zA-Z0-9]/, '')).to_sym]}".insert_value

      case outline['ns2:Status']
        when 'Active - No Longer Recruiting',
             'Active - Recruiting',
             'Active - Not Yet Recruiting'
          site_status = "'Active',"
        when 'Close-Out Ready',
             'Closed'
          site_status = "'Closed',"
        else
          site_status = 'NULL,'
      end

      values_clause <<
          "(#{outline['ns2:Protocol_Number'].insert_value}"               + # study_protocol_id
              " #{outline['ns2:Site_Number'].insert_value}"               + # site_number
              " #{outline['ns2:Site_Number'].insert_value}"               + # site_name
              ' NULL,'                                                + # site_address
              " #{country_capital}"                                   + # site_city
              ' NULL,'                                                + # site_state
              " #{outline['ns2:Study_Country_Name'].insert_value}"        + # site_country
              ' NULL,'                                                + # site_postal_code
              ' NULL,'                                                + # site_phone
              ' NULL,'                                                + # site_fax
              ' NULL,'                                                + # site_FPFV
              ' NULL,'                                                + # site_LPLV
              ' NULL,'                                                + # planned_enrollment
              " #{(outline['ns2:PI_Last_Name']+', '+outline['ns2:PI_First_Name']).insert_value}" + # site_PI
              ' NULL,'                                                + # site_PI_email
              ' NULL,'                                                + # site_coordinator
              ' NULL,'                                                + # site_coordinator_email
              " #{site_status}"                                       + # site_status
              " '#{vendor}'"                                          + # vendor_code
              ' )'
    end

    @logger.info "#{self.class.name} writer end"
    values_clause
  end
end

class C16014_subjectRETRO

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
    @logger.info 'R668AD1021_site writer start'

    if @processing_lines.count == 0
      logger.info ("Nothing to insert :(")
      exit 0
    end

    values_clause = Array.new

    @processing_lines.each do |outline|

      subject_treatment   = 'MLN9708'.insert_value
      subject_arm         = 'MLN9708 or Placebo'.insert_value
      date_of_birth       = (outline[5].nil?)  ? ' NULL,' : "STR_TO_DATE(#{outline[5].insert(-3, '19').insert_value} '%e-%b-%Y'),"

      values_clause <<
          "('C16014',"                                          + # study_protocol_id
              " #{outline[2].insert_value}"                         + # site_number
              " #{outline[4].insert_value}"                         + # subject_code
              " #{outline[4].insert_value}"                         + # subject_external_id
              ' NULL,'                                              + # randomization_number
              " #{outline[6].insert_value}"                         + # gender
              ' NULL,'                                              + # initials
              ' NULL,'                                              + # enrollment_status
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

class C16014_subject

  SUBJECT_STATUS =
  {
      :Completed          => 'Randomized',
      :EarlyTerminated    => 'Discontinued',
      :Enrolled           => 'Screened',
      :Randomized         => 'Randomized',
      :ScreenFailure      => 'Screen Failed',
      :TxDiscontinued     => 'Discontinued',
      :EnrollmentFailure  => 'Discontinued',
      :Screened           => 'Screened',
      :Rescreened         => 'Screened'
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

    @inbound_lines.each do |specline|

      subject_status = (specline['ns2:Primary_Status'].nil?)

      if specline['ns2:Primary_Status'].nil? or SUBJECT_STATUS[(specline['ns2:Primary_Status'].gsub(/[^a-zA-Z0-9]/, '')).to_sym].nil?
        @logger.info '================= QUERY NEED ===================='
        @logger.info "Unknown subject status ->#{specline['ns2:Primary_Status']}<- on subject ->#{specline['ns2:Subject_Number']}<-"
        @logger.info "Full record ->#{specline}<-"
        next
      end

      @processing_lines << specline
    end

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

      subject_treatment   = ' NULL,'
      subject_arm         = ' NULL,'
      date_of_birth       = ' NULL,'
      enrollment_status   = SUBJECT_STATUS[(outline['ns2:Primary_Status'].gsub(/[^a-zA-Z0-9]/, '')).to_sym].insert_value


      values_clause <<
             "( #{outline['ns2:Protocol_Number'].insert_value}"     + # study_protocol_id
              " #{outline['ns2:Site_Number'].insert_value}"         + # site_number
              " #{outline['ns2:Subject_Number'].insert_value}"      + # subject_code
              " #{outline['ns2:Subject_Number'].insert_value}"      + # subject_external_id
              ' NULL,'                                              + # randomization_number
              ' NULL,'                                              + # gender
              ' NULL,'                                              + # initials
              " #{enrollment_status}"                               + # enrollment_status
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

class C16014_Inv
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
              if !outline[20].nil? && outline[20][0..8] == 'Check Out'
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
              "('C16014',"                                               + # study_protocol_id
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
              ' NULL,'                                                   + # specimen_designation_detail
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
