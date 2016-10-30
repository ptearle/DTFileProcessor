require 'mysql2'

class MLN0002_3028_site

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

  SITE_COUNTRY =
  {
    :S07001 => 'United States',
    :S07002 => 'United States',
    :S07003 => 'United States',
    :S07004 => 'United States',
    :S07006 => 'United States',
    :S07007 => 'United States',
    :S07009 => 'United States',
    :S07010 => 'United States',
    :S07011 => 'United States',
    :S07014 => 'United States',
    :S07016 => 'United States',
    :S07017 => 'United States',
    :S07021 => 'United States',
    :S07022 => 'United States',
    :S07023 => 'Canada',
    :S07024 => 'Canada',
    :S07029 => 'Canada',
    :S07030 => 'Belgium',
    :S07031 => 'Belgium',
    :S07032 => 'Belgium',
    :S07034 => 'Belgium',
    :S07035 => 'Belgium',
    :S07036 => 'Czech Republic',
    :S07037 => 'Czech Republic',
    :S07038 => 'Czech Republic',
    :S07039 => 'Czech Republic',
    :S07041 => 'Czech Republic',
    :S07047 => 'France',
    :S07055 => 'Hungary',
    :S07056 => 'Hungary',
    :S07057 => 'Hungary',
    :S07058 => 'Hungary',
    :S07060 => 'Hungary',
    :S07061 => 'Hungary',
    :S07062 => 'Hungary',
    :S07063 => 'Hungary',
    :S07064 => 'Hungary',
    :S07071 => 'Italy',
    :S07072 => 'Poland',
    :S07073 => 'Poland',
    :S07075 => 'Poland',
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
        if specline[6][0..3] == distinct_line[6][0..3]
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

      site_number     = outline[6][0..3].rjust(5,'0')
      country         = "#{SITE_COUNTRY[('S'+site_number.gsub(/[^a-zA-Z0-9]/, '')).to_sym]}"
      country_capital = "#{COUNTRY_CAPITAL[(country.gsub(/[^a-zA-Z0-9]/, '')).to_sym]}"
      investigator    = outline[9][0..-5]

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

class MLN0002_3028_subject

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

      site_number         = outline[6][0..3].rjust(5,'0')
      subject_code        = outline[6][0..-5].rjust(9, '0')
      subject_initials    = outline[6][-3..-1]
      subject_treatment   = 'Vedolizumab 300mg IV'
      subject_arm         = 'Part A'
      puts "->#{outline[32].insert_value}}<-"
      icf_signing_date    = (outline[32].nil?)  ? ' NULL,' : "STR_TO_DATE(#{outline[32].insert_value} '%e-%b-%y'),"

      values_clause <<
              "(#{outline[1].insert_value}"                         + # study_protocol_id
              " #{site_number.insert_value}"                        + # site_number
              " #{subject_code.insert_value}"                       + # subject_code
              " #{subject_code.insert_value}"                       + # subject_external_id
              ' NULL,'                                              + # randomization_number
              ' NULL,'                                              + # gender
              " #{subject_initials.insert_value}"                   + # initials
              " #{outline[16].insert_value}"                        + # enrollment_status
              ' NULL,'                                              + # date_of_birth 1-Jan-34
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

class MLN0002_3028_Inv

 VISIT_MAP = {
	  :ET           => 'Early Termination',
	  :Unscheduled1 => 'Unscheduled',
	  :Unscheduled2 => 'Unscheduled',
	  :Unscheduled5 => 'Unscheduled',
	  :Visit1       => 'Visit 1',
	  :Visit2       => 'Visit 2',
	  :Visit4       => 'Visit 4',
	  :Visit5       => 'Visit 5',
	  :Visit6       => 'Visit 6',
	  :Visit7       => 'Visit 7',
    :Visit8       => 'Visit 8',
    :Visit9       => 'Visit 9'
  }.freeze

  SPECIMEN_TYPE =
  {
      :PTINR                     => 'Plasma',
      :TBAntigenTubeRedTop       => 'Plasma',
      :TBMitogenControlPurpleTop => 'Plasma',
      :TBNILControlTubeGreyTop   => 'Plasma',
      :CReactiveProtein          => 'Serum',
      :Chemistry                 => 'Serum',
      :Creatinine                => 'Serum',
      :FSH                       => 'Serum',
      :HepatitisHIV              => 'Serum',
      :Immunogenicity1           => 'Serum',
      :Immunogenicity2           => 'Serum',
      :Immunogenicity3           => 'Serum',
      :Immunogenicity4           => 'Serum',
      :SerumHCG                  => 'Serum',
      :CalprotectinFecal         => 'Stool',
      :ColonBiopsyDesSig1        => 'Synovial Fluid',
      :ColonBiopsyTrans1         => 'Synovial Fluid',
      :ColonBiopsyAscend1        => 'Tissue',
      :ColonBiopsyAscend2        => 'Tissue',
      :ColonBiopsyDesSig2        => 'Tissue',
      :ColonBiopsyIleum1         => 'Tissue',
      :ColonBiopsyIleum2         => 'Tissue',
      :ColonBiopsyRectum1        => 'Tissue',
      :ColonBiopsyRectum2        => 'Tissue',
      :ColonBiopsyTrans2         => 'Tissue',
      :Urinalysis                => 'Urine',
      :CBCwAutoDiff              => 'Whole Blood',
      :PGxDNA1                   => 'Whole Blood',
      :PGxDNA2                   => 'Whole Blood',
      :PGxRNA1                   => 'Whole Blood',
      :PGxRNA2                   => 'Whole Blood'
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
      logger.info ("Nothing to insert :(")
      exit 0
    end

    values_clause = Array.new

    @processing_lines.each do |outline|

      treatment             = 'Vedolizumab 300mg IV'
      arm                   = 'Part A'
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
          specimen_status   = 'Exhausted'
        when 'Shipped Geneuity'
          specimen_location = 'GENT'
        when 'Shipped QPS - 1 Innovation Way'
          specimen_location  = 'QPS'
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
