class CA018001_EDD
  def initialize(logger)
    @logger = logger
    @logger.info 'CA018001_EDD filer Initialized'
  end

  def reader(inbound_file)
    @logger.debug "File name is ->#{inbound_file}<-"
    @logger.info 'CA018001_EDD reader start'
    @inbound_lines = CSV.read(inbound_file, headers: true, skip_blanks: true, skip_lines: '\r', encoding:'windows-1256:utf-8')
    @logger.info 'CA018001_EDD reader end'
    @inbound_lines.length
  end

  def processor
    @logger.info 'CA018001_EDD processor start'

    @processing_lines = Array.new
    lines = 0
    num_distinct_lines = 0

    @inbound_lines.each do |specline|
      found = false
      lines += 1

      # Ignore if ISO Value is "No specimen received"
      # Ignore if Test Status is "Assigned"

      if specline[23].strip == "No specimen received" or specline[22].strip == "Assigned"
        next
      end

      @processing_lines.each do |distinct_line|

        # see if we actually need the line in the file ... Accession+TubeNumber already seen, then ignore

        if specline[0]+specline[9] == distinct_line[0]+distinct_line[9]
          found = true
          break
        end
      end

      if !found
        @processing_lines << specline
        num_distinct_lines += 1
      end
    end

    @logger.info 'CA018001_EDD processor end'
    @processing_lines.length
  end

  def writer(vendor)
    @logger.info 'CA018001_EDD writer start'

    if @processing_lines.count == 0
      logger.info ("Nothing to insert :(")
      exit 0
    end

    values_clause = ''

    @processing_lines.each do |outline|

      parent_id        = (outline[11] == '')          ? "NULL"   : "'#{outline[8].strip}#{outline[11].strip}'"
      is_child         = (parent_id == 'NULL')        ? 'N'      : 'Y'
      specimen_type    = (outline[16][0..2] == 'SCR') ? 'Tissue' : 'Whole Blood'
      receive_datetime = (is_child == 'Y')            ? "NULL"   : "STR_TO_DATE('#{outline[15].strip}',  '%d-%b-%Y %T')"

      values_clause << "\n"                                                       +
                       " ('CA018-001',"                                           + # study_protocol_id
                       "  '#{outline[2].strip}',"                                 + # site_number
                       "  '#{outline[4].strip}',"                                 + # subject_number
                       "  '#{outline[6].strip}',"                                 + # subject_gender
                       "  STR_TO_DATE('#{outline[5].strip}',  '%d-%b-%Y'),"       + # subject_DOB
                       "  STR_TO_DATE('#{outline[7].strip}',  '%d-%b-%Y'),"       + # specimen_collect_date
                       "  STR_TO_DATE('#{outline[7].strip}',  '%d-%b-%Y %T'),"    + # specimen_collect_time
                       "  #{receive_datetime},"                                   + # specimen_receive_datetime
                       "  '#{outline[16].strip}',"                                + # visit_name
                       "  '#{outline[8].strip}#{outline[9].strip}',"              + # specimen_barcode
                       "  NULL,"                                                  + # specimen_identifier
                       "  '#{specimen_type}',"                                    + # specimen_type
                       "  #{parent_id},"                                          + # specimen_parent
                       "  '#{is_child}',"                                         + # specimen_ischild
                       "  NULL,"                                                  + # specimen_condition
                       "  'In Inventory',"                                        + # specimen_status
                       "  NULL,"                                                  + # specimen_shipdate
                       "  '#{vendor}'"                                            + # vendor_code
                       " ),"
    end

    @logger.info 'CA018001_EDD writer end'
    values_clause[0...-1]
  end
end

class CA018001_BMS
  def initialize(logger)
    @logger = logger
    @logger.info 'CA018001_BMS filer Initialized'
  end

  def reader(inbound_file)
    @logger.info 'CA018001_BMS reader start'
    @inbound_lines = CSV.read(inbound_file, headers: true, skip_blanks: true, skip_lines: '\r', encoding:'windows-1256:utf-8')
    @logger.info 'CA018001_BMS reader end'
    @inbound_lines.length
  end

  def processor
    @logger.info 'CA018001_BMS processor start'
    @processing_lines = @inbound_lines
    @logger.info 'CA018001_BMS processor end'
    @processing_lines.length
  end

  def writer(vendor)
    @logger.info 'CA018001_BMS writer start'

    values_clause = ''

    @processing_lines.each do |outline|

      site_number       =  outline[1].split(' ')[0].strip
      subject_number    =  outline[1].split(' ')[1].strip
      visit_name        = (outline[5].nil?) ? 'NULL' : "'#{outline[5].strip}'"
      specimen_barcode  =  outline[6].strip

      if outline[6][-2..-1] == '01' and visit_name.downcase.include? 'cyto'
        specimen_barcode += 'BMS'
      end

      specimen_parent   = (outline[9].nil?)  ? 'NULL' : "'#{outline[9]}'"
      specimen_ischild  = (outline[9].nil?)  ? "'N'" : "'Y'"
      specimen_shipdate = (outline[12].nil?) ? 'NULL' : "STR_TO_DATE('#{outline[4].strip}',  '%c/%e/%Y %l:%i')"

      values_clause << "\n"                                                       +
                       " ('#{outline[0]}',"                                       + # study_protocol_id
                       " '#{site_number}',"                                       + # site_number
                       " '#{subject_number}',"                                    + # subject_number
                       ' NULL,'                                                   + # subject_gender
                       ' NULL,'                                                   + # subject_DOB
                       " STR_TO_DATE('#{outline[2].strip}',  '%c/%e/%Y'),"        + # specimen_collect_date
                       " STR_TO_DATE('#{outline[3].strip}',  '%T'),"              + # specimen_collect_time
                       " STR_TO_DATE('#{outline[4].strip}',  '%c/%e/%Y %l:%i'),"  + # specimen_receive_datetime
                       " #{visit_name},"                                          + # visit_name
                       " '#{specimen_barcode}',"                                  + # specimen_barcode
                       " '#{outline[7].strip}',"                                  + # specimen_identifier
                       " '#{outline[8].strip}',"                                  + # specimen_type
                       " #{specimen_parent},"                                     + # specimen_parent
                       " #{specimen_ischild},"                                    + # specimen_ischild
                       ' NULL,'                                                   + # specimen_condition
                       " '#{outline[11].strip}',"                                 + # specimen_status
                       " #{specimen_shipdate},"                                   + # specimen_shipdate
                       " '#{vendor}'"                                             + # vendor_code
                       "),"
    end

    @logger.info 'CA018001_BMS writer end'
    values_clause[0...-1]
  end
end

class CA018001_QINV
  VISIT_MAP = {
                :PE0001     => 'SCR ARCHIVAL TISSUE',
                :PE0002     => 'SCR FRESH IHC QLAB A',
                :PE0003     => 'SCR FRESH TO LABCORP',
                :PE0004     => 'SCR FRESH IHC QLAB B',
                :PE0005     => 'SCR FRESH RNA QLAB A',
                :PE0006     => 'SCR FRESH RNA QLAB B',
                :UNSCHED_1  => 'C1 D1',
                :UNSCHED_10 => 'C1 D15 CYTOMETRY',
                :UNSCHED_12 => 'C3 D1',
                :UNSCHED_16 => 'EOT',
                :UNSCHED_18 => 'PROG/ UNSCH PBMC',
                :UNSCHED_2  => 'C1 D1 CYTOMETRY',
                :UNSCHED_20 => 'FU3 D100',
                :UNSCHED_21 => 'FU1 D30',
                :UNSCHED_22 => 'PROG/ UNSCH CYTOMETRY',
                :UNSCHED_23 => 'FU2 D60',
                :UNSCHED_24 => 'C5 D1',
                :UNSCHED_25 => 'C1 D28 TISSUE IHC A',
                :UNSCHED_26 => 'C1 D28 TISSUE IHC B',
                :UNSCHED_27 => 'C1 D28 TISSUE IHC C',
                :UNSCHED_28 => 'C1 D28 TISSUE RNA A',
                :UNSCHED_29 => 'C1 D28 TISSUE RNA B',
                :UNSCHED_3  => 'C1 D1 PBMC',
                :UNSCHED_35 => 'PROG/UNSCH IHC A',
                :UNSCHED_36 => 'PROG/ UNSCH IHC B',
                :UNSCHED_37 => 'PROG/ UNSCH IHC C',
                :UNSCHED_38 => 'PROG/ UNSCH RNA A',
                :UNSCHED_39 => 'PROG/ UNSCH RNA B',
                :UNSCHED_4	 => 'C1 D1 EOI PK',
                :UNSCHED_40 => 'PROG/ UNSCH',
                :UNSCHED_41 => 'TRACK 1 RUN-IN EOI PK',
                :UNSCHED_42 => 'TRACK 1 RUN-IN D1',
                :UNSCHED_43 => 'TRACK 1 RUN-IN D1 CYTO',
                :UNSCHED_44 => 'TRACK 1 RUN-IN D15',
                :UNSCHED_45 => 'TRACK 1 RUN-IN D15 CYTO',
                :UNSCHED_46 => 'TRACK 1 RUN-IN D15 PBMC',
                :UNSCHED_47 => 'TRACK 1 RUN-IN D1 PBMC',
                :UNSCHED_5  => 'C1 D28',
                :UNSCHED_6  => 'C1 D15 PBMC',
                :UNSCHED_7  => 'C1 D28 CYTOMETRY',
                :UNSCHED_8  => 'C1 D15',
                :UNSCHED_9  => 'C1 D28 PBMC'
               }.freeze

  SPECIMEN_TYPE = {
                    :BLOCK     => 'Tissue',
                  }.freeze

  def initialize(logger)
    @logger = logger
    @logger.info 'CA018001_QINV filer Initialized'
  end

  def reader(inbound_file)
    @logger.info 'CA018001_QINV reader start'
    @inbound_lines = CSV.read(inbound_file, headers: true, skip_blanks: true, skip_lines: '\r', encoding:'windows-1256:utf-8')
    @logger.info 'CA018001_QINV reader end'
    @inbound_lines.length
  end

  def processor
    @logger.info 'CA018001_QINV processor start'
    @processing_lines = @inbound_lines
    @logger.info 'CA018001_QINV processor end'
    @processing_lines.length
  end

  def writer(vendor)
    @logger.info 'CA018001_QINV writer start'

    values_clause = ''

    @processing_lines.each do |outline|

      visit_name         = VISIT_MAP[outline[22].to_sym]
      specimen_barcode   = outline[11].split('-')[0]+outline[11].split('-')[1].to_s.rjust(2, '0')

      subject_gender     = (outline[9] == 'F') ? 'Female' : 'Male'
      specimen_type      = (outline[13].nil?)  ? 'NULL'   : "'#{SPECIMEN_TYPE[outline[13].to_sym]}'"
      specimen_parent    = (outline[30].nil?)  ? 'NULL'   : "'#{outline[30]}'"
      specimen_ischild   = (outline[30].nil?)  ? "'N'"    : "'Y'"
      specimen_shipdate  = (outline[35].nil?)  ? 'NULL'   : "STR_TO_DATE('#{outline[35].strip}', '%Y-%m-%d')"
      specimen_condition = (outline[20].nil?)  ? 'NULL'   : "'#{outline[20]}'"

      @logger.debug("->#{outline[6].strip}<-")

      values_clause << "\n"                                           +
          " ('#{outline[0]}',"                                        + # study_protocol_id
          "  '#{outline[2]}',"                                        + # site_number
          "  '#{outline[4]}',"                                        + # subject_number
          "  '#{subject_gender}',"                                    + # subject_gender
          "  STR_TO_DATE('#{outline[6].strip}',  '%Y-%m-%d'),"        + # subject_DOB
          "  STR_TO_DATE('#{outline[17].strip}', '%Y-%m-%d'),"        + # specimen_collect_date
          "  STR_TO_DATE('#{outline[19].strip}', '%k:%i'),"           + # specimen_collect_time
          "  STR_TO_DATE('#{outline[34].strip}', '%Y-%m-%d'),"        + # specimen_receive_datetime
          "  '#{visit_name}',"                                        + # visit_name
          "  '#{specimen_barcode}',"                                  + # specimen_barcode
          "  '#{outline[11].strip}',"                                 + # specimen_identifier
          "   #{specimen_type},"                                      + # specimen_type
          "   #{specimen_parent},"                                    + # specimen_parent
          "   #{specimen_ischild},"                                   + # specimen_ischild
          "   #{specimen_condition},"                                 + # specimen_condition
          "   'In Inventory',"                                        + # specimen_status
          "   #{specimen_shipdate},"                                  + # specimen_shipdate
          "  '#{vendor}'"                                             + # vendor_code
          "),"
    end

    @logger.info 'CA018001_QINV writer end'
    values_clause[0...-1]
  end
end