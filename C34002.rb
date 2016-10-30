require 'mysql2'

class C34002_site

  COUNTRY_CAPITAL =
  {
    :Belgium         => 'Brussles',
    :Canada          => 'Ottawa',
    :CzechRepublic   => 'Prague',
    :France          => 'Paris',
    :Hungary         => 'Budapest',
    :Italy           => 'Rome',
    :Poland          => 'Warsaw',
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
        if specline[10][-5..-1] == distinct_line[10][-5..-1]
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

    values_clause   = Array.new
    country         = 'United States'
    country_capital = "#{COUNTRY_CAPITAL[(country.gsub(/[^a-zA-Z0-9]/, '')).to_sym]}"

    @processing_lines.each do |outline|

      site_number     = outline[10][-5..-1]
      investigator    = outline[9][0..-6]

      values_clause <<
              "('C34002',"                                            + # study_protocol_id
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

class C34002_subject

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

      site_number         = outline[10][-5..-1]
      subject_code        = outline[6]
      subject_initials    = ''
      enrollment_status   = 'Randomized'
      subject_treatment   = 'TAK-659'
      subject_arm         = 'Phase 1b'
      icf_signing_date    = (outline[50].nil?)  ? ' NULL,' : "STR_TO_DATE(#{outline[50].insert_value} '%e %b %Y'),"

      values_clause <<
              "('C34002',"                                          + # study_protocol_id
              " #{site_number.insert_value}"                        + # site_number
              " #{subject_code.insert_value}"                       + # subject_code
              " #{subject_code.insert_value}"                       + # subject_external_id
              ' NULL,'                                              + # randomization_number
              ' NULL,'                                              + # gender
              ' NULL,'                                              + # initials
              " #{enrollment_status.insert_value}"                  + # enrollment_status
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
              " #{subject_treatment.insert_value}"                  + # treatment
              " #{subject_arm.insert_value}"                        + # arm
              " #{icf_signing_date}"                                + # ICF_signing_date
              ' NULL,'                                              + # ICF_withdrawl_date
              " '#{vendor}'"                                        + # vendor_code
              ")"
    end

    @logger.info "#{self.class.name} writer end"
    values_clause
  end
end

class C34002_Inv

 VISIT_MAP = {
    :C1D22C1D28     => 'Cycle 1 Day 22',
    :Cycle1Day1     => 'Cycle 1 Day 1',
    :Cycle1Day15    => 'Cycle 1 Day 15',
    :Cycle1Day16    => 'Cycle 1 Day 16',
    :Cycle1Day2     => 'Cycle 1 Day 2',
    :Cycle2Day1     => 'Cycle 2 Day 1',
    :Cycle2Day15    => 'Cycle 2 Day 15',
    :Cycle3Day1     => 'Cycle 3 Day 1',
    :Cycle4Day1     => 'Cycle 4 Day 1',
    :Cycle5Day1     => 'Cycle 5 Day 1',
    :Cycle6Day1     => 'Cycle 6 Day 1',
    :Cycle7Day1     => 'Cycle 7 Day 1',
    :Screening      => 'Screening',
    :ScreeningBM    => 'Screening BM',
    :Unscheduled1   => 'Unscheduled',
    :Unscheduled5   => 'Unscheduled',
    :UnscheduledBM1 => 'Unscheduled BM'
  }.freeze

  SPECIMEN_TYPE =
  {
      :TumorTissueBMAFresh         => 'Bone Marrow Aspirate',
      :TumorTissueBMBiopsyFresh    => 'Bone Marro Biopsy',
      :TumorTissueBMBiopsyResidual => 'Bone Marrow Biopsy',
      :pSYKBlock                   => 'Bone Marrow Biopsy',
      :BMABankedSlides1            => 'Bone Marrow Aspirate',
      :BMABankedSlides10           => 'Bone Marrow Aspirate',
      :BMABankedSlides2            => 'Bone Marrow Aspirate',
      :BMABankedSlides3            => 'Bone Marrow Aspirate',
      :BMABankedSlides4            => 'Bone Marrow Aspirate',
      :BMABankedSlides5            => 'Bone Marrow Aspirate',
      :BMABankedSlides6            => 'Bone Marrow Aspirate',
      :BMABankedSlides7            => 'Bone Marrow Aspirate',
      :BMABankedSlides8            => 'Bone Marrow Aspirate',
      :BMABankedSlides9            => 'Bone Marrow Aspirate',
      :Ph1bPlasmaPK05hrA1          => 'Plasma',
      :Ph1bPlasmaPK05hrA2          => 'Plasma',
      :Ph1bPlasmaPK1hrA1           => 'Plasma',
      :Ph1bPlasmaPK1hrA2           => 'Plasma',
      :Ph1bPlasmaPK24hrA1          => 'Plasma',
      :Ph1bPlasmaPK24hrA2          => 'Plasma',
      :Ph1bPlasmaPK2hrA1           => 'Plasma',
      :Ph1bPlasmaPK2hrA2           => 'Plasma',
      :Ph1bPlasmaPK3hrA1           => 'Plasma',
      :Ph1bPlasmaPK3hrA2           => 'Plasma',
      :Ph1bPlasmaPK4hrA1           => 'Plasma',
      :Ph1bPlasmaPK4hrA2           => 'Plasma',
      :Ph1bPlasmaPK8hrA1           => 'Plasma',
      :Ph1bPlasmaPK8hrA2           => 'Plasma',
      :Ph1bPlasmaPKPredoseA1       => 'Plasma',
      :Ph1bPlasmaPKPredoseA2       => 'Plasma',
      :PlasmaPIA                   => 'Plasma',
      :BuccalGermlineDNA1          => 'Buccal Cells',
      :BuccalGermlineDNA2          => 'Buccal Cells',
      :TumorTissueBloodNGS         => 'Whole Blood',
      :BloodPD                     => 'Whole Blood',
      :Ph1bBloodPD2hr              => 'Whole Blood',
      :Ph1bBloodPD8hr              => 'Whole Blood',
      :Ph1bBloodPDPredose          => 'Whole Blood',
      :TumorTissueBloodpSYK        => 'Whole Blood'
  }.freeze

  SPECIMEN_DESIGNATION =
  {
      :TumorTissueBMAFresh         => 'Tumor_Tissue BMA Fresh',
      :TumorTissueBMBiopsyFresh    => 'Tumor_Tissue BM Biopsy Fresh',
      :TumorTissueBMBiopsyResidual => 'Tumor_Tissue BM Biopsy Residual',
      :pSYKBlock                   => 'pSYK Block',
      :BMABankedSlides1            => 'BMA Banked Slides',
      :BMABankedSlides10           => 'BMA Banked Slides',
      :BMABankedSlides2            => 'BMA Banked Slides',
      :BMABankedSlides3            => 'BMA Banked Slides',
      :BMABankedSlides4            => 'BMA Banked Slides',
      :BMABankedSlides5            => 'BMA Banked Slides',
      :BMABankedSlides6            => 'BMA Banked Slides',
      :BMABankedSlides7            => 'BMA Banked Slides',
      :BMABankedSlides8            => 'BMA Banked Slides',
      :BMABankedSlides9            => 'BMA Banked Slides',
      :Ph1bPlasmaPK05hrA1          => 'PK',
      :Ph1bPlasmaPK05hrA2          => 'PK',
      :Ph1bPlasmaPK1hrA1           => 'PK',
      :Ph1bPlasmaPK1hrA2           => 'PK',
      :Ph1bPlasmaPK24hrA1          => 'PK',
      :Ph1bPlasmaPK24hrA2          => 'PK',
      :Ph1bPlasmaPK2hrA1           => 'PK',
      :Ph1bPlasmaPK2hrA2           => 'PK',
      :Ph1bPlasmaPK3hrA1           => 'PK',
      :Ph1bPlasmaPK3hrA2           => 'PK',
      :Ph1bPlasmaPK4hrA1           => 'PK',
      :Ph1bPlasmaPK4hrA2           => 'PK',
      :Ph1bPlasmaPK8hrA1           => 'PK',
      :Ph1bPlasmaPK8hrA2           => 'PK',
      :Ph1bPlasmaPKPredoseA1       => 'PK',
      :Ph1bPlasmaPKPredoseA2       => 'PK',
      :PlasmaPIA                   => 'PK',
      :BuccalGermlineDNA1          => 'Buccal Germline DNA',
      :BuccalGermlineDNA2          => 'Buccal Germline DNA',
      :TumorTissueBloodNGS         => 'Tumor_Tissue Blood NGS',
      :BloodPD                     => 'PD',
      :Ph1bBloodPD2hr              => 'PD',
      :Ph1bBloodPD8hr              => 'PD',
      :Ph1bBloodPDPredose          => 'PD',
      :TumorTissueBloodpSYK        => 'Tumor_Tissue Blood pSYK'
  }.freeze

 SPECIMEN_DESIGNATION_DETAIL =
 {
     :TumorTissueBMAFresh         => 'Tumor_Tissue BMA Fresh',
     :TumorTissueBMBiopsyFresh    => 'Tumor_Tissue BM Biopsy Fresh',
     :TumorTissueBMBiopsyResidual => 'Tumor_Tissue BM Biopsy Residual',
     :pSYKBlock                   => 'pSYK Block',
     :BMABankedSlides1            => 'BMA Banked Slides 1',
     :BMABankedSlides10           => 'BMA Banked Slides 10',
     :BMABankedSlides2            => 'BMA Banked Slides 2',
     :BMABankedSlides3            => 'BMA Banked Slides 3',
     :BMABankedSlides4            => 'BMA Banked Slides 4',
     :BMABankedSlides5            => 'BMA Banked Slides 5',
     :BMABankedSlides6            => 'BMA Banked Slides 6',
     :BMABankedSlides7            => 'BMA Banked Slides 7',
     :BMABankedSlides8            => 'BMA Banked Slides 8',
     :BMABankedSlides9            => 'BMA Banked Slides 9',
     :Ph1bPlasmaPK05hrA1          => 'Ph1b Plasma PK 0.5hr A1',
     :Ph1bPlasmaPK05hrA2          => 'Ph1b Plasma PK 0.5hr A2',
     :Ph1bPlasmaPK1hrA1           => 'Ph1b Plasma PK 1hr A1',
     :Ph1bPlasmaPK1hrA2           => 'Ph1b Plasma PK 1hr A2',
     :Ph1bPlasmaPK24hrA1          => 'Ph1b Plasma PK 24hr A1',
     :Ph1bPlasmaPK24hrA2          => 'Ph1b Plasma PK 24hr A2',
     :Ph1bPlasmaPK2hrA1           => 'Ph1b Plasma PK 2hr A1',
     :Ph1bPlasmaPK2hrA2           => 'Ph1b Plasma PK 2hr A2',
     :Ph1bPlasmaPK3hrA1           => 'Ph1b Plasma PK 3hr A1',
     :Ph1bPlasmaPK3hrA2           => 'Ph1b Plasma PK 3hr A2',
     :Ph1bPlasmaPK4hrA1           => 'Ph1b Plasma PK 4hr A1',
     :Ph1bPlasmaPK4hrA2           => 'Ph1b Plasma PK 4hr A2',
     :Ph1bPlasmaPK8hrA1           => 'Ph1b Plasma PK 8hr A1',
     :Ph1bPlasmaPK8hrA2           => 'Ph1b Plasma PK 8hr A2',
     :Ph1bPlasmaPKPredoseA1       => 'Ph1b Plasma PK Predose A1',
     :Ph1bPlasmaPKPredoseA2       => 'Ph1b Plasma PK Predose A2',
     :PlasmaPIA                   => 'Plasma PIA',
     :BuccalGermlineDNA1          => 'Buccal Germline_DNA 1',
     :BuccalGermlineDNA2          => 'Buccal Germline_DNA 2',
     :TumorTissueBloodNGS         => 'Tumor_Tissue Blood NGS',
     :BloodPD                     => 'Blood PD',
     :Ph1bBloodPD2hr              => 'Ph1b Blood PD 2hr',
     :Ph1bBloodPD8hr              => 'Ph1b Blood PD 8hr',
     :Ph1bBloodPDPredose          => 'Ph1b Blood PD Predose',
     :TumorTissueBloodpSYK        => 'Tumor_Tissue Blood pSYK'
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
      arm                   = 'Phase 1b'
      visit                 = VISIT_MAP[(outline[8].gsub(/[^a-zA-Z0-9]/, '')).to_sym]
      specimen_type         = SPECIMEN_TYPE[((outline[13].nil? ? '' : outline[13]).gsub(/[^a-zA-Z0-9]/, '')).to_sym]
      specimen_designation  = SPECIMEN_DESIGNATION[((outline[13].nil? ? '' : outline[13]).gsub(/[^a-zA-Z0-9]/, '')).to_sym]
      specimen_ddetail      = SPECIMEN_DESIGNATION_DETAIL[((outline[13].nil? ? '' : outline[13]).gsub(/[^a-zA-Z0-9]/, '')).to_sym]
      specimen_barcode      = outline[12]
      specimen_status       = "In Storage"
      specimen_location     = 'PPD'
      specimen_is_child     = 'No'
      specimen_parent       = 'NULL'
      specimen_shipdate     = (outline[11].nil?)  ? ' NULL,' : "STR_TO_DATE(#{outline[11].insert_value} '%e-%b-%y'),"

# set specimen status and vendor code base upon PPD Status mapping.

      case outline[17]
        when 'Destroyed',
             'Discarded'
          specimen_status   = 'Destroyed'
        when 'Lab Discard'
          specimen_location = 'GENT'
        when 'Shipped Genoptix Inc - Carlsbad'
          specimen_location  = 'GENO'
        when 'Shipped Tandem Labs - Durham'
          specimen_location  = 'TAND'
        when 'Shipped John Hopkins University - Levis Lab'
          specimen_location  = 'JHLV'
        when 'Shipped Personalized Molecular Med. Lab-SD'
          specimen_location  = 'PMML'
      end

      values_clause <<
          "(#{outline[1].insert_value}"                              + # study_protocol_id
          " #{outline[5].rjust(5, '0').insert_value}"                + # site_number
          " #{outline[6].rjust(9, '0').insert_value}"                + # subject_code
          ' NULL,'                                                   + # subject_gender
          ' NULL,'                                                   + # subject_DOB
          " STR_TO_DATE(#{outline[9].insert_value} '%e-%b-%y'),"     + # specimen_collect_date
          ' NULL,'                                                   + # specimen_collect_time
          " STR_TO_DATE(#{outline[10].insert_value} '%e-%b-%y'),"    + # specimen_receive_datetime
          " #{treatment.insert_value}"                               + # treatment
          " #{arm.insert_value}"                                     + # arm
          " #{visit.insert_value}"                                   + # visit_name
          " #{specimen_barcode.insert_value}"                        + # specimen_barcode
          " #{specimen_barcode.insert_value}"                        + # specimen_identifier
          " #{specimen_type.insert_value}"                           + # specimen_type
          " #{outline[13].insert_value}"                             + # specimen_name
          " #{specimen_designation.insert_value}"                    + # specimen_designation,
          " #{specimen_ddetail.insert_value}"                        + # specimen_designation_detaill
          " #{specimen_parent.insert_value}"                         + # specimen_parent
          " #{specimen_is_child.insert_value}"                       + # specimen_ischild
          ' NULL,'                                                   + # specimen_condition
          " #{specimen_status.insert_value}"                         + # specimen_status
          ' NULL,'                                                   + # specimen_comment
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
