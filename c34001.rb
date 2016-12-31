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
    :BIOPDNA => 'Biopsy DNA',
    :C1D1    => 'Cycle 1 Day 1',
    :C1D12   => 'Cycle 1 Day 1',
    :C1D15   => 'Cycle 1 Day 15',
    :C1D1516 => 'Cycle 1 Day 16',
    :C1D8    => 'Cycle 1 Day 8',
    :RIDay1  => 'RI Day 1',
    :RIDay2  => 'RI Day 2',
    :RIDay3  => 'RI Day 3',
    :RIDay4  => 'RI Day 4',
    :RIDay5  => 'RI Day 5',
    :RIDay8  => 'RI Day 8',
    :SCREEN  => 'Screening'
  }.freeze

  SPECIMEN_TYPE =
  {
      :BMTISSUE      =>'Tissue',
      :FTISSUE       =>'Tissue',
      :ABPATH        =>'Document',
      :BMPATH        =>'Document',
      :FTPATH        =>'Document',
      :RSPATH        =>'Document',
      :DNA1          =>'DNA',
      :DNA2          =>'DNA',
      :SWABDNA1      =>'DNA',
      :ABLCK         =>'Tissue',
      :BMBLCK        =>'Tissue',
      :FBMBLCK       =>'Tissue',
      :FFTBLCK1      =>'Tissue',
      :FTBLCK        =>'Tissue',
      :ASLD1         =>'Tissue',
      :ASLD10        =>'Tissue',
      :ASLD2         =>'Tissue',
      :ASLD3         =>'Tissue',
      :ASLD4         =>'Tissue',
      :ASLD5         =>'Tissue',
      :ASLD6         =>'Tissue',
      :ASLD7         =>'Tissue',
      :ASLD8         =>'Tissue',
      :ASLD9         =>'Tissue',
      :DBSPK         =>'Whole Blood',
      :fpk124hrpost  =>'Plasma',
      :PD1           =>'Plasma',
      :PD2           =>'Plasma',
      :PK1           =>'Plasma',
      :PK2           =>'Plasma',
      :CYTO1         =>'Serum',
      :CYTO2         =>'Serum',
      :CYTO3         =>'Serum',
      :CYTO4         =>'Serum',
      :BMSLD1        =>'Tissue',
      :BMSLD10       =>'Tissue',
      :BMSLD2        =>'Tissue',
      :BMSLD3        =>'Tissue',
      :BMSLD4        =>'Tissue',
      :BMSLD5        =>'Tissue',
      :BMSLD6        =>'Tissue',
      :BMSLD7        =>'Tissue',
      :BMSLD8        =>'Tissue',
      :BMSLD9        =>'Tissue',
      :FBSLD1        =>'Tissue',
      :FBSLD10       =>'Tissue',
      :FBSLD11       =>'Tissue',
      :FBSLD12       =>'Tissue',
      :FBSLD13       =>'Tissue',
      :FBSLD14       =>'Tissue',
      :FBSLD15       =>'Tissue',
      :FBSLD16       =>'Tissue',
      :FBSLD17       =>'Tissue',
      :FBSLD18       =>'Tissue',
      :FBSLD19       =>'Tissue',
      :FBSLD2        =>'Tissue',
      :FBSLD20       =>'Tissue',
      :FBSLD3        =>'Tissue',
      :FBSLD4        =>'Tissue',
      :FBSLD5        =>'Tissue',
      :FBSLD6        =>'Tissue',
      :FBSLD7        =>'Tissue',
      :FBSLD8        =>'Tissue',
      :FBSLD9        =>'Tissue',
      :FFTSLD1       =>'Tissue',
      :FFTSLD10      =>'Tissue',
      :FFTSLD11      =>'Tissue',
      :FFTSLD12      =>'Tissue',
      :FFTSLD13      =>'Tissue',
      :FFTSLD14      =>'Tissue',
      :FFTSLD15      =>'Tissue',
      :FFTSLD16      =>'Tissue',
      :FFTSLD17      =>'Tissue',
      :FFTSLD18      =>'Tissue',
      :FFTSLD19      =>'Tissue',
      :FFTSLD2       =>'Tissue',
      :FFTSLD20      =>'Tissue',
      :FFTSLD3       =>'Tissue',
      :FFTSLD4       =>'Tissue',
      :FFTSLD5       =>'Tissue',
      :FFTSLD6       =>'Tissue',
      :FFTSLD7       =>'Tissue',
      :FFTSLD8       =>'Tissue',
      :FFTSLD9       =>'Tissue',
      :FTSLD1        =>'Tissue',
      :FTSLD10       =>'Tissue',
      :FTSLD11       =>'Tissue',
      :FTSLD12       =>'Tissue',
      :FTSLD13       =>'Tissue',
      :FTSLD14       =>'Tissue',
      :FTSLD15       =>'Tissue',
      :FTSLD16       =>'Tissue',
      :FTSLD17       =>'Tissue',
      :FTSLD18       =>'Tissue',
      :FTSLD19       =>'Tissue',
      :FTSLD2        =>'Tissue',
      :FTSLD20       =>'Tissue',
      :FTSLD3        =>'Tissue',
      :FTSLD4        =>'Tissue',
      :FTSLD5        =>'Tissue',
      :FTSLD6        =>'Tissue',
      :FTSLD7        =>'Tissue',
      :FTSLD8        =>'Tissue',
      :FTSLD9        =>'Tissue',
      :SSLD1         =>'Tissue',
      :SSLD10        =>'Tissue',
      :SSLD11        =>'Tissue',
      :SSLD12        =>'Tissue',
      :SSLD13        =>'Tissue',
      :SSLD14        =>'Tissue',
      :SSLD15        =>'Tissue',
      :SSLD16        =>'Tissue',
      :SSLD17        =>'Tissue',
      :SSLD18        =>'Tissue',
      :SSLD19        =>'Tissue',
      :SSLD2         =>'Tissue',
      :SSLD20        =>'Tissue',
      :SSLD3         =>'Tissue',
      :SSLD4         =>'Tissue',
      :SSLD5         =>'Tissue',
      :SSLD6         =>'Tissue',
      :SSLD7         =>'Tissue',
      :SSLD8         =>'Tissue',
      :SSLD9         =>'Tissue',
      :UPK1          =>'Urine',
      :UPK2          =>'Urine',
      :NGSDNA1       =>'DNA',
      :NGSDNA2       =>'DNA',
      :WBBMDNA       =>'DNA',
      :CTC1          =>'Whole Blood',
      :CTC2          =>'Whole Blood'
  }.freeze

  SPECIMEN_DESIGNATION =
  {
      :BMTISSUE      =>'Bone Marrow Biopsy',
      :FTISSUE       =>'Tumor Tissue',
      :ABPATH        =>'Pathology Report',
      :BMPATH        =>'Pathology Report',
      :FTPATH        =>'Pathology Report',
      :RSPATH        =>'Pathology Report',
      :DNA1          =>'WB DNA',
      :DNA2          =>'WB DNA',
      :SWABDNA1      =>'Buccal DNA',
      :ABLCK         =>'Archive Tumor Tissue Block',
      :BMBLCK        =>'Bone Marrow Biopsy Block',
      :FBMBLCK       =>'Fresh Bone Marro Biopsy Block',
      :FFTBLCK1      =>'Fresh Tumor Tissue Block',
      :FTBLCK        =>'Tumor Tissue',
      :ASLD1         =>'Archive Tumor Tissue Slide',
      :ASLD10        =>'Archive Tumor Tissue Slide',
      :ASLD2         =>'Archive Tumor Tissue Slide',
      :ASLD3         =>'Archive Tumor Tissue Slide',
      :ASLD4         =>'Archive Tumor Tissue Slide',
      :ASLD5         =>'Archive Tumor Tissue Slide',
      :ASLD6         =>'Archive Tumor Tissue Slide',
      :ASLD7         =>'Archive Tumor Tissue Slide',
      :ASLD8         =>'Archive Tumor Tissue Slide',
      :ASLD9         =>'Archive Tumor Tissue Slide',
      :DBSPK         =>'Dried Blood Spot PK',
      :fpk124hrpost  =>'Plasma PK',
      :PD1           =>'Plasma PD',
      :PD2           =>'Plasma PD',
      :PK1           =>'Plasma PK',
      :PK2           =>'Plasma PK',
      :CYTO1         =>'Serum Cytokines',
      :CYTO2         =>'Serum Cytokines',
      :CYTO3         =>'Serum Cytokines',
      :CYTO4         =>'Serum Cytokines',
      :BMSLD1        =>'Bone Marrow Aspirate/Biopsy Slide',
      :BMSLD10       =>'Bone Marrow Aspirate/Biopsy Slide',
      :BMSLD2        =>'Bone Marrow Aspirate/Biopsy Slide',
      :BMSLD3        =>'Bone Marrow Aspirate/Biopsy Slide',
      :BMSLD4        =>'Bone Marrow Aspirate/Biopsy Slide',
      :BMSLD5        =>'Bone Marrow Aspirate/Biopsy Slide',
      :BMSLD6        =>'Bone Marrow Aspirate/Biopsy Slide',
      :BMSLD7        =>'Bone Marrow Aspirate/Biopsy Slide',
      :BMSLD8        =>'Bone Marrow Aspirate/Biopsy Slide',
      :BMSLD9        =>'Bone Marrow Aspirate/Biopsy Slide',
      :FBSLD1        =>'Fresh Bone Marrow Biopsy Slide',
      :FBSLD10       =>'Fresh Bone Marrow Biopsy Slide',
      :FBSLD11       =>'Fresh Bone Marrow Biopsy Slide',
      :FBSLD12       =>'Fresh Bone Marrow Biopsy Slide',
      :FBSLD13       =>'Fresh Bone Marrow Biopsy Slide',
      :FBSLD14       =>'Fresh Bone Marrow Biopsy Slide',
      :FBSLD15       =>'Fresh Bone Marrow Biopsy Slide',
      :FBSLD16       =>'Fresh Bone Marrow Biopsy Slide',
      :FBSLD17       =>'Fresh Bone Marrow Biopsy Slide',
      :FBSLD18       =>'Fresh Bone Marrow Biopsy Slide',
      :FBSLD19       =>'Fresh Bone Marrow Biopsy Slide',
      :FBSLD2        =>'Fresh Bone Marrow Biopsy Slide',
      :FBSLD20       =>'Fresh Bone Marrow Biopsy Slide',
      :FBSLD3        =>'Fresh Bone Marrow Biopsy Slide',
      :FBSLD4        =>'Fresh Bone Marrow Biopsy Slide',
      :FBSLD5        =>'Fresh Bone Marrow Biopsy Slide',
      :FBSLD6        =>'Fresh Bone Marrow Biopsy Slide',
      :FBSLD7        =>'Fresh Bone Marrow Biopsy Slide',
      :FBSLD8        =>'Fresh Bone Marrow Biopsy Slide',
      :FBSLD9        =>'Fresh Bone Marrow Biopsy Slide',
      :FFTSLD1       =>'Fresh Tumor Tissue Slide',
      :FFTSLD10      =>'Fresh Tumor Tissue Slide',
      :FFTSLD11      =>'Fresh Tumor Tissue Slide',
      :FFTSLD12      =>'Fresh Tumor Tissue Slide',
      :FFTSLD13      =>'Fresh Tumor Tissue Slide',
      :FFTSLD14      =>'Fresh Tumor Tissue Slide',
      :FFTSLD15      =>'Fresh Tumor Tissue Slide',
      :FFTSLD16      =>'Fresh Tumor Tissue Slide',
      :FFTSLD17      =>'Fresh Tumor Tissue Slide',
      :FFTSLD18      =>'Fresh Tumor Tissue Slide',
      :FFTSLD19      =>'Fresh Tumor Tissue Slide',
      :FFTSLD2       =>'Fresh Tumor Tissue Slide',
      :FFTSLD20      =>'Fresh Tumor Tissue Slide',
      :FFTSLD3       =>'Fresh Tumor Tissue Slide',
      :FFTSLD4       =>'Fresh Tumor Tissue Slide',
      :FFTSLD5       =>'Fresh Tumor Tissue Slide',
      :FFTSLD6       =>'Fresh Tumor Tissue Slide',
      :FFTSLD7       =>'Fresh Tumor Tissue Slide',
      :FFTSLD8       =>'Fresh Tumor Tissue Slide',
      :FFTSLD9       =>'Fresh Tumor Tissue Slide',
      :FTSLD1        =>'Fresh Tumor Biopsy Slide',
      :FTSLD10       =>'Fresh Tumor Biopsy Slide',
      :FTSLD11       =>'Fresh Tumor Biopsy Slide',
      :FTSLD12       =>'Fresh Tumor Biopsy Slide',
      :FTSLD13       =>'Fresh Tumor Biopsy Slide',
      :FTSLD14       =>'Fresh Tumor Biopsy Slide',
      :FTSLD15       =>'Fresh Tumor Biopsy Slide',
      :FTSLD16       =>'Fresh Tumor Biopsy Slide',
      :FTSLD17       =>'Fresh Tumor Biopsy Slide',
      :FTSLD18       =>'Fresh Tumor Biopsy Slide',
      :FTSLD19       =>'Fresh Tumor Biopsy Slide',
      :FTSLD2        =>'Fresh Tumor Biopsy Slide',
      :FTSLD20       =>'Fresh Tumor Biopsy Slide',
      :FTSLD3        =>'Fresh Tumor Biopsy Slide',
      :FTSLD4        =>'Fresh Tumor Biopsy Slide',
      :FTSLD5        =>'Fresh Tumor Biopsy Slide',
      :FTSLD6        =>'Fresh Tumor Biopsy Slide',
      :FTSLD7        =>'Fresh Tumor Biopsy Slide',
      :FTSLD8        =>'Fresh Tumor Biopsy Slide',
      :FTSLD9        =>'Fresh Tumor Biopsy Slide',
      :SSLD1         =>'Archive Tumor Tissue Slide',
      :SSLD10        =>'Archive Tumor Tissue Slide',
      :SSLD11        =>'Archive Tumor Tissue Slide',
      :SSLD12        =>'Archive Tumor Tissue Slide',
      :SSLD13        =>'Archive Tumor Tissue Slide',
      :SSLD14        =>'Archive Tumor Tissue Slide',
      :SSLD15        =>'Archive Tumor Tissue Slide',
      :SSLD16        =>'Archive Tumor Tissue Slide',
      :SSLD17        =>'Archive Tumor Tissue Slide',
      :SSLD18        =>'Archive Tumor Tissue Slide',
      :SSLD19        =>'Archive Tumor Tissue Slide',
      :SSLD2         =>'Archive Tumor Tissue Slide',
      :SSLD20        =>'Archive Tumor Tissue Slide',
      :SSLD3         =>'Archive Tumor Tissue Slide',
      :SSLD4         =>'Archive Tumor Tissue Slide',
      :SSLD5         =>'Archive Tumor Tissue Slide',
      :SSLD6         =>'Archive Tumor Tissue Slide',
      :SSLD7         =>'Archive Tumor Tissue Slide',
      :SSLD8         =>'Archive Tumor Tissue Slide',
      :SSLD9         =>'Archive Tumor Tissue Slide',
      :UPK1          =>'PK Urine',
      :UPK2          =>'PK Urine',
      :NGSDNA1       =>'NGS DNA',
      :NGSDNA2       =>'NGS DNA',
      :WBBMDNA       =>'BM DNA',
      :CTC1          =>'Circulating Tumor Cells',
      :CTC2          =>'Circulating Tumor Cells'
  }.freeze

 SPECIMEN_DESIGNATION_DETAIL =
 {
     :BMTISSUE      =>'Bone Marrow Biopsy (Formalin)',
     :FTISSUE       =>'Fresh Tumor Tissue (Formalin)',
     :ABPATH        =>'Archive Tumor Tissue Pathology Report',
     :BMPATH        =>'BM Biopsy Pathology Report',
     :FTPATH        =>'Fresh Tumor Pathology Report',
     :RSPATH        =>'Residual Tumor Tissue Pathology Report',
     :DNA1          =>'WB DNA 1',
     :DNA2          =>'WB DNA 2',
     :SWABDNA1      =>'Buccal DNA',
     :ABLCK         =>'Archive Tumor Tissue Block',
     :BMBLCK        =>'Bone Marrow Biopsy Block',
     :FBMBLCK       =>'Fresh Bone Marrow Biopsy Block',
     :FFTBLCK1      =>'Fresh Tumor Tissue Block 1',
     :FTBLCK        =>'Fresh Tumor Biopsy Block',
     :ASLD1         =>'Archive Tumor Tissue Slide 1',
     :ASLD10        =>'Archive Tumor Tissue Slide 10',
     :ASLD2         =>'Archive Tumor Tissue Slide 2',
     :ASLD3         =>'Archive Tumor Tissue Slide 3',
     :ASLD4         =>'Archive Tumor Tissue Slide 4',
     :ASLD5         =>'Archive Tumor Tissue Slide 5',
     :ASLD6         =>'Archive Tumor Tissue Slide 6',
     :ASLD7         =>'Archive Tumor Tissue Slide 7',
     :ASLD8         =>'Archive Tumor Tissue Slide 8',
     :ASLD9         =>'Archive Tumor Tissue Slide 9',
     :DBSPK         =>'DBS PK',
     :fpk124hrpost  =>'Plasma PK 1',
     :PD1           =>'Plasma PD 1',
     :PD2           =>'Plasma PD 2',
     :PK1           =>'Plasma PK 1',
     :PK2           =>'Plasma PK 2',
     :CYTO1         =>'Serum Cytokines 1',
     :CYTO2         =>'Serum Cytokines 2',
     :CYTO3         =>'Serum Cytokines 3',
     :CYTO4         =>'Serum Cytokines 4',
     :BMSLD1        =>'Bone Marrow Aspirate/Biopsy Slides 1',
     :BMSLD10       =>'Bone Marrow Aspirate/Biopsy Slides 10',
     :BMSLD2        =>'Bone Marrow Aspirate/Biopsy Slides 2',
     :BMSLD3        =>'Bone Marrow Aspirate/Biopsy Slides 3',
     :BMSLD4        =>'Bone Marrow Aspirate/Biopsy Slides 4',
     :BMSLD5        =>'Bone Marrow Aspirate/Biopsy Slides 5',
     :BMSLD6        =>'Bone Marrow Aspirate/Biopsy Slides 6',
     :BMSLD7        =>'Bone Marrow Aspirate/Biopsy Slides 7',
     :BMSLD8        =>'Bone Marrow Aspirate/Biopsy Slides 8',
     :BMSLD9        =>'Bone Marrow Aspirate/Biopsy Slides 9',
     :FBSLD1        =>'Fresh Bone Marrow Biopsy Slides 1',
     :FBSLD10       =>'Fresh Bone Marrow Biopsy Slides 10',
     :FBSLD11       =>'Fresh Bone Marrow Biopsy Slides 11',
     :FBSLD12       =>'Fresh Bone Marrow Biopsy Slides 12',
     :FBSLD13       =>'Fresh Bone Marrow Biopsy Slides 13',
     :FBSLD14       =>'Fresh Bone Marrow Biopsy Slides 14',
     :FBSLD15       =>'Fresh Bone Marrow Biopsy Slides 15',
     :FBSLD16       =>'Fresh Bone Marrow Biopsy Slides 16',
     :FBSLD17       =>'Fresh Bone Marrow Biopsy Slides 17',
     :FBSLD18       =>'Fresh Bone Marrow Biopsy Slides 18',
     :FBSLD19       =>'Fresh Bone Marrow Biopsy Slides 19',
     :FBSLD2        =>'Fresh Bone Marrow Biopsy Slides 2',
     :FBSLD20       =>'Fresh Bone Marrow Biopsy Slides 20',
     :FBSLD3        =>'Fresh Bone Marrow Biopsy Slides 3',
     :FBSLD4        =>'Fresh Bone Marrow Biopsy Slides 4',
     :FBSLD5        =>'Fresh Bone Marrow Biopsy Slides 5',
     :FBSLD6        =>'Fresh Bone Marrow Biopsy Slides 6',
     :FBSLD7        =>'Fresh Bone Marrow Biopsy Slides 7',
     :FBSLD8        =>'Fresh Bone Marrow Biopsy Slides 8',
     :FBSLD9        =>'Fresh Bone Marrow Biopsy Slides 9',
     :FFTSLD1       =>'Fresh Tumor Tissue Slides 1',
     :FFTSLD10      =>'Fresh Tumor Tissue Slides 10',
     :FFTSLD11      =>'Fresh Tumor Tissue Slides 11',
     :FFTSLD12      =>'Fresh Tumor Tissue Slides 12',
     :FFTSLD13      =>'Fresh Tumor Tissue Slides 13',
     :FFTSLD14      =>'Fresh Tumor Tissue Slides 14',
     :FFTSLD15      =>'Fresh Tumor Tissue Slides 15',
     :FFTSLD16      =>'Fresh Tumor Tissue Slides 16',
     :FFTSLD17      =>'Fresh Tumor Tissue Slides 17',
     :FFTSLD18      =>'Fresh Tumor Tissue Slides 18',
     :FFTSLD19      =>'Fresh Tumor Tissue Slides 19',
     :FFTSLD2       =>'Fresh Tumor Tissue Slides 2',
     :FFTSLD20      =>'Fresh Tumor Tissue Slides 20',
     :FFTSLD3       =>'Fresh Tumor Tissue Slides 3',
     :FFTSLD4       =>'Fresh Tumor Tissue Slides 4',
     :FFTSLD5       =>'Fresh Tumor Tissue Slides 5',
     :FFTSLD6       =>'Fresh Tumor Tissue Slides 6',
     :FFTSLD7       =>'Fresh Tumor Tissue Slides 7',
     :FFTSLD8       =>'Fresh Tumor Tissue Slides 8',
     :FFTSLD9       =>'Fresh Tumor Tissue Slides 9',
     :FTSLD1        =>'Fresh Tumor Biopsy Slides 1',
     :FTSLD10       =>'Fresh Tumor Biopsy Slides 10',
     :FTSLD11       =>'Fresh Tumor Biopsy Slides 11',
     :FTSLD12       =>'Fresh Tumor Biopsy Slides 12',
     :FTSLD13       =>'Fresh Tumor Biopsy Slides 13',
     :FTSLD14       =>'Fresh Tumor Biopsy Slides 14',
     :FTSLD15       =>'Fresh Tumor Biopsy Slides 15',
     :FTSLD16       =>'Fresh Tumor Biopsy Slides 16',
     :FTSLD17       =>'Fresh Tumor Biopsy Slides 17',
     :FTSLD18       =>'Fresh Tumor Biopsy Slides 18',
     :FTSLD19       =>'Fresh Tumor Biopsy Slides 19',
     :FTSLD2        =>'Fresh Tumor Biopsy Slides 2',
     :FTSLD20       =>'Fresh Tumor Biopsy Slides 20',
     :FTSLD3        =>'Fresh Tumor Biopsy Slides 3',
     :FTSLD4        =>'Fresh Tumor Biopsy Slides 4',
     :FTSLD5        =>'Fresh Tumor Biopsy Slides 5',
     :FTSLD6        =>'Fresh Tumor Biopsy Slides 6',
     :FTSLD7        =>'Fresh Tumor Biopsy Slides 7',
     :FTSLD8        =>'Fresh Tumor Biopsy Slides 8',
     :FTSLD9        =>'Fresh Tumor Biopsy Slides 9',
     :SSLD1         =>'Archive Tumor Tissue Slides 1',
     :SSLD10        =>'Archive Tumor Tissue Slides 10',
     :SSLD11        =>'Archive Tumor Tissue Slides 11',
     :SSLD12        =>'Archive Tumor Tissue Slides 12',
     :SSLD13        =>'Archive Tumor Tissue Slides 13',
     :SSLD14        =>'Archive Tumor Tissue Slides 14',
     :SSLD15        =>'Archive Tumor Tissue Slides 15',
     :SSLD16        =>'Archive Tumor Tissue Slides 16',
     :SSLD17        =>'Archive Tumor Tissue Slides 17',
     :SSLD18        =>'Archive Tumor Tissue Slides 18',
     :SSLD19        =>'Archive Tumor Tissue Slides 19',
     :SSLD2         =>'Archive Tumor Tissue Slides 2',
     :SSLD20        =>'Archive Tumor Tissue Slides 20',
     :SSLD3         =>'Archive Tumor Tissue Slides 3',
     :SSLD4         =>'Archive Tumor Tissue Slides 4',
     :SSLD5         =>'Archive Tumor Tissue Slides 5',
     :SSLD6         =>'Archive Tumor Tissue Slides 6',
     :SSLD7         =>'Archive Tumor Tissue Slides 7',
     :SSLD8         =>'Archive Tumor Tissue Slides 8',
     :SSLD9         =>'Archive Tumor Tissue Slides D9',
     :UPK1          =>'PK Urine 1',
     :UPK2          =>'PK Urine 2',
     :NGSDNA1       =>'NGS DNA 1',
     :NGSDNA2       =>'NGS DNA 2',
     :WBBMDNA       =>'BM DNA',
     :CTC1          =>'CTC 1',
     :CTC2          =>'CTC 2'
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
      specimen_type         = SPECIMEN_TYPE[((outline[17].nil? ? '' : outline[17]).gsub(/[^a-zA-Z0-9]/, '')).to_sym]
      specimen_designation  = SPECIMEN_DESIGNATION[((outline[17].nil? ? '' : outline[17]).gsub(/[^a-zA-Z0-9]/, '')).to_sym]
      specimen_ddetail      = SPECIMEN_DESIGNATION_DETAIL[((outline[17].nil? ? '' : outline[17]).gsub(/[^a-zA-Z0-9]/, '')).to_sym]
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
          " STR_TO_DATE(#{outline[14].insert_value} '%e-%b-%Y'),"    + # specimen_collect_date
          " STR_TO_DATE(#{outline[15].insert_value} '%H:%i'),"       + # specimen_collect_time
          " STR_TO_DATE(#{outline[23].insert_value} '%e-%b-%Y'),"    + # specimen_receive_datetime
          " #{treatment.insert_value}"                               + # treatment
          " #{arm.insert_value}"                                     + # arm
          " #{visit.insert_value}"                                   + # visit_name
          " #{specimen_barcode.insert_value}"                        + # specimen_barcode
          " #{specimen_barcode.insert_value}"                        + # specimen_identifier
          " #{specimen_type.insert_value}"                           + # specimen_type
          " #{outline[17].insert_value}"                             + # specimen_name
          " #{specimen_designation.insert_value}"                    + # specimen_designation,
          " #{specimen_ddetail.insert_value}"                        + # specimen_designation_detail
          ' NULL,'                                                   + # specimen_parent
          " #{specimen_is_child.insert_value}"                       + # specimen_ischild
          " #{outline[16].insert_value}"                             + # specimen_condition
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
