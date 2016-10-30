require 'mysql2'

class C34001_site

  COUNTRY_CAPITAL =
  {
    :Italy           => 'Rome',
    :Spain           => 'Madrid',
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

      if outline[4] == 'USA'
        outline[4] = 'United States'
      end

      site_number     = outline[2]
      country         = outline[4]
      puts "->#{outline[4]}<- ->#{country}<-"
      country_capital = "#{COUNTRY_CAPITAL[(country.gsub(/[^a-zA-Z0-9]/, '')).to_sym]}"
      investigator    = outline[3]

      values_clause <<
              "(#{outline[1].insert_value}"                           + # study_protocol_id
              " #{site_number.insert_value}"                          + # site_number
              " #{site_number.insert_value}"                          + # site_name
              ' NULL,'                                                + # site_address
              " #{country_capital.insert_value}"                      + # site_city
              ' NULL,'                                                + # site_state
              " #{country.insert_value}"                              + # site_country
              ' NULL,'                                                + # site_postal_code
              ' NULL,'                                                + # site_phone
              ' NULL,'                                                + # site_fax
              ' NULL,'                                                + # site_FPFV
              ' NULL,'                                                + # site_LPLV
              ' NULL,'                                                + # planned_enrollment
              " #{investigator.insert_value}"                         + # site_PI
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

class C34001_subject

  def initialize(logger)
    @logger = logger
    @logger.debug "#{self.class.name} filer Initialized"
  end

  def reader(inbound_file)
    @logger.debug "File name is ->#{inbound_file}<-"
    @logger.info "#{self.class.name} reader start"
    @inbound_lines  = CSV.read(inbound_file, headers: true, skip_blanks: true, skip_lines: '\r', encoding:'windows-1256:utf-8')
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

      # Skip entries with no subject id
      if specline[6].nil? || specline[6] == ''
        next
      end

      @processing_lines.each do |distinct_line|

        # see if we actually need the line in the file ... Site Id already seen, then ignore
        if specline[6] == distinct_line[6]
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
      @logger.info ("Nothing to insert :(")
      exit 0
    end

    values_clause = Array.new

    @processing_lines.each do |outline|

      site_number         = outline[2]
      subject_code        = outline[6]
      subject_gender      = outline[10]
      enrollment_status   = 'Randomized'
      subject_treatment   = 'TAK-659'
      subject_arm         = 'Dose Escalation (Part A)'
      subject_dob         = (outline[11].nil?)  ? ' NULL,' : "STR_TO_DATE(#{outline[11].insert_value} '%e-%b-%Y'),"
      puts "->#{outline[11]}<- ->#{subject_dob}<-"

      values_clause <<
              "(#{outline[1].insert_value}"                         + # study_protocol_id
              " #{site_number.insert_value}"                        + # site_number
              " #{subject_code.insert_value}"                       + # subject_code
              " #{subject_code.insert_value}"                       + # subject_external_id
              ' NULL,'                                              + # randomization_number
              " #{subject_gender.insert_value}"                     + # gender
              ' NULL,'                                              + # initials
              " #{enrollment_status.insert_value}"                  + # enrollment_status
              " #{subject_dob}"                                     + # date_of_birth 1-Jan-34
              ' NULL,'                                              + # address
              ' NULL,'                                              + # city
              ' NULL,'                                              + # state
              ' NULL,'                                              + # region
              ' NULL,'                                              + # country
              ' NULL,'                                              + # postcode
              ' NULL,'                                              + # primary_race
              ' NULL,'                                              + # secondary_race
              ' NULL,'                                              + # ethnicity
              " #{subject_treatment.insert_value}"                  + # treatment
              " #{subject_arm.insert_value}"                        + # arm
              ' NULL,'                                              + # ICF_signing_date
              ' NULL,'                                              + # ICF_withdrawl_date
              " '#{vendor}'"                                        + # vendor_code
              ")"
    end

    @logger.info "#{self.class.name} writer end"
    values_clause
  end
end

class C34001_Inv

 VISIT_MAP = {
    :BIOPDNA => 'BIOPDNA',
    :C1D1    => 'Cycle 1 Day 1',
    :C1D12   => 'Cycle 1 Day 1',
    :C1D15   => 'Cycle 1 Day 15',
    :C1D1516 => 'Cycle 1 Day 16',
    :C1D8    => 'Cycle 1 Day 8',
    :RIDay1  => 'RIDay1',
    :RIDay2  => 'RIDay2',
    :RIDay3  => 'RIDay3',
    :RIDay4  => 'RIDay4',
    :RIDay5  => 'RIDay5',
    :RIDay8  => 'RIDay8',
    :SCREEN  => 'Screening'
  }.freeze

  SPECIMEN_TYPE =
  {
    :BiopsyTissue       => 'Tissue',
    :Document           => 'Document',
    :ExtractedDNA       => 'DNA',
    :Paraffinblock      => 'Tissue',
    :ParaffinSlide      => 'Tissue',
    :PlasmaEDTAK2       => 'Palsma',
    :SerumwGel          => 'Serum',
    :Slide              => 'Whole Blood',
    :UrineRandom        => 'Urine',
    :WBEDTAK2           => 'Whole Blood',
    :WBNaHepNP3Buffered => 'Whole Blood'
  }.freeze

  SPECIMEN_DESIGNATION =
  {
    :PTINR                     => 'PT / INR',
    :TBAntigenTubeRedTop       => 'TB Antigen Tube',
    :TBMitogenControlPurpleTop => 'TB Mitogen',
    :TBNILControlTubeGreyTop   => 'TB NIL',
    :CReactiveProtein          => 'C Reactive Protein',
    :Chemistry                 => 'Chemistry',
    :Creatinine                => 'Creatinine',
    :FSH                       => 'FSH',
    :HepatitisHIV              => 'Hepatitis / HIV',
    :Immunogenicity1           => 'Immunogenicity',
    :Immunogenicity2           => 'Immunogenicity',
    :Immunogenicity3           => 'Immunogenicity',
    :Immunogenicity4           => 'Immunogenicity',
    :SerumHCG                  => 'Serum HCG',
    :CalprotectinFecal         => 'Calprotectin (Fecal)',
    :ColonBiopsyDesSig1        => 'Colon Biopsy Des/Sig',
    :ColonBiopsyTrans1         => 'Colon Biopsy Trans',
    :ColonBiopsyAscend1        => 'Colon Biopsy Ascend',
    :ColonBiopsyAscend2        => 'Colon Biopsy Ascend',
    :ColonBiopsyDesSig2        => 'Colon Biopsy Des/Sig',
    :ColonBiopsyIleum1         => 'Colon Biopsy Ileum',
    :ColonBiopsyIleum2         => 'Colon Biopsy Ileum',
    :ColonBiopsyRectum1        => 'Colon Biopsy Rectum',
    :ColonBiopsyRectum2        => 'Colon Biopsy Rectum',
    :ColonBiopsyTrans2         => 'Colon Biopsy Trans',
    :Urinalysis                => 'Urinalysis',
    :CBCwAutoDiff              => 'CBC w/Auto Diff',
    :PGxDNA1                   => 'PGx DNA',
    :PGxDNA2                   => 'PGx DNA',
    :PGxRNA1                   => 'PGx RNA',
    :PGxRNA2                   => 'PGx RNA'
  }.freeze

 SPECIMEN_DESIGNATION_DETAIL =
 {
     :PTINR                     => 'PT / INR',
     :TBAntigenTubeRedTop       => 'TB Antigen Tube (Red Top)',
     :TBMitogenControlPurpleTop => 'TB Mitogen Control (Purple Top)',
     :TBNILControlTubeGreyTop   => 'TB NIL Control Tube (Grey Top)',
     :CReactiveProtein          => 'C Reactive Protein',
     :Chemistry                 => 'Chemistry',
     :Creatinine                => 'Creatinine',
     :FSH                       => 'FSH',
     :HepatitisHIV              => 'Hepatitis / HIV',
     :Immunogenicity1           => 'Immunogenicity 1',
     :Immunogenicity2           => 'Immunogenicity 2',
     :Immunogenicity3           => 'Immunogenicity 3',
     :Immunogenicity4           => 'Immunogenicity 4',
     :SerumHCG                  => 'Serum HCG',
     :CalprotectinFecal         => 'Calprotectin (Fecal)',
     :ColonBiopsyDesSig1        => 'Colon Biopsy Des/Sig 1',
     :ColonBiopsyTrans1         => 'Colon Biopsy Trans 1',
     :ColonBiopsyAscend1        => 'Colon Biopsy Ascend 1',
     :ColonBiopsyAscend2        => 'Colon Biopsy Ascend 2',
     :ColonBiopsyDesSig2        => 'Colon Biopsy Des/Sig 2',
     :ColonBiopsyIleum1         => 'Colon Biopsy Ileum 1',
     :ColonBiopsyIleum2         => 'Colon Biopsy Ileum 2',
     :ColonBiopsyRectum1        => 'Colon Biopsy Rectum 1',
     :ColonBiopsyRectum2        => 'Colon Biopsy Rectum 2',
     :ColonBiopsyTrans2         => 'Colon Biopsy Trans 2',
     :Urinalysis                => 'Urinalysis',
     :CBCwAutoDiff              => 'CBC w/Auto Diff',
     :PGxDNA1                   => 'PGx DNA 1',
     :PGxDNA2                   => 'PGx DNA 2',
     :PGxRNA1                   => 'PGx RNA 1',
     :PGxRNA2                   => 'PGx RNA 2'
 }.freeze

 SPECIMEN_SHIPPED =
 {
     :ChristieNHSFoundationTrust => 'CNFT',
     :Clarient                   => 'CLAR',
     :CovanceGenomicsLab         => 'LCGL',
     :CovanceGenomicsLaboratory  => 'LCGL',
     :GenoptixInc                => 'GENO',
     :ICONCentralLaboratories    => 'ICON',
     :JohnsHopkinsUniversity     => 'JHU',
     :LabCorpClinicalTrials      => 'LCRP',
     :TandemLabsRTP              => 'TANR',
     :TheBroadInstitute          => 'BRIN'
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
    @processing_lines = @inbound_lines
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

      treatment             = 'TAK-659'
      arm                   = 'Dose Escalation (Part A)'
      visit                 = VISIT_MAP[(outline[12].gsub(/[^a-zA-Z0-9]/, '')).to_sym]
      specimen_type         = SPECIMEN_TYPE[((outline[22].nil? ? '' : outline[22]).gsub(/[^a-zA-Z0-9]/, '')).to_sym]
      specimen_designation  = SPECIMEN_DESIGNATION[((outline[13].nil? ? '' : outline[13]).gsub(/[^a-zA-Z0-9]/, '')).to_sym]
      specimen_ddetail      = SPECIMEN_DESIGNATION_DETAIL[((outline[13].nil? ? '' : outline[13]).gsub(/[^a-zA-Z0-9]/, '')).to_sym]
      specimen_barcode      = outline[18]
      specimen_status       = "In Storage"
      specimen_location     = 'PPDF'
      specimen_is_child     = 'No'
      specimen_shipdate     = (outline[33].nil?)  ? ' NULL,' : "STR_TO_DATE(#{outline[33].insert_value} '%e-%b-%Y'),"

      if outline[28].nil?
        specimen_location   = SPECIMEN_SHIPPED[((outline[35].nil? ? '' : outline[35]).gsub(/[^a-zA-Z0-9]/, '')).to_sym]
      else
        specimen_location   = (outline[28] == 'Dublin') ? 'PPDD' : 'PPDF'
      end

      values_clause <<
          "(#{outline[1].insert_value}"                              + # study_protocol_id
          " #{outline[2].insert_value}"                              + # site_number
          " #{outline[6].insert_value}"                              + # subject_code
          ' NULL,'                                                   + # subject_gender
          ' NULL,'                                                   + # subject_DOB
          " STR_TO_DATE(#{outline[19].insert_value} '%e-%b-%Y'),"    + # specimen_collect_date
          " STR_TO_DATE(#{outline[20].insert_value} '%H:%i'),"       + # specimen_collect_time
          " STR_TO_DATE(#{outline[23].insert_value} '%e-%b-%Y'),"    + # specimen_receive_datetime
          " #{treatment.insert_value}"                               + # treatment
          " #{arm.insert_value}"                                     + # arm
          " #{visit.insert_value}"                                   + # visit_name
          " #{specimen_barcode.insert_value}"                        + # specimen_barcode
          " #{specimen_barcode.insert_value}"                        + # specimen_identifier
          " #{specimen_type.insert_value}"                           + # specimen_type
          " #{outline[17].insert_value}"                             + # specimen_name
          " #{specimen_designation.insert_value}"                    + # specimen_designation,
          " #{specimen_ddetail.insert_value}"                        + # specimen_designation_detaill
          ' NULL,'                                                   + # specimen_parent
          " #{specimen_is_child.insert_value}"                       + # specimen_ischild
          ' NULL,'                                                   + # specimen_condition
          " #{specimen_status.insert_value}"                         + # specimen_status
          " #{outline[25].insert_value}"                             + # specimen_comment
          " #{specimen_shipdate}"                                    + # shipped_date
          " #{specimen_location.insert_value}"                       + # shipped_location
          ' NULL,'                                                   + # testing_description
          " '#{vendor}'"                                             + # vendor_code
          ")"
    end

    @logger.info "#{self.class.name} writer end"
    values_clause
  end
end
