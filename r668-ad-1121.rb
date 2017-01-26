class R668_AD_1121_LCRP_DNA
  class Parent_Key
    def initialize (parent_id, uuid)
      @parent_id = parent_id
      @uuid      = uuid
    end

    attr_reader :parent_id
    attr_reader :uuid
  end

  SPECIMEN_TYPE = {
      :S0	  => 'UNKNOWN',
      :S13	=> 'Plasma',
      :S16	=> 'Urine',
      :S160	=> 'Plasma',
      :S2	  => 'Serum',
      :S3   => 'Serum',
      :S31	=> 'Whole Blood',
      :S4	  => 'Serum',
      :S46	=> 'Whole Blood',
      :S50  => 'Whole Blood',
      :S725	=> 'DNA',
      :S875 => 'Biopsy',
      :S925 => 'DNA',
      :S908	=> 'Whole Blood',
      :S9   => 'Plasma',
      :S533 => 'Biospy',
      :S15  => 'Urine',
  }.freeze

  VISIT_MAP = {
      :VISIT1   => 'Visit 1',
      :VISIT2   => 'Visit 2',
      :VISIT3   => 'Visit 3',
      :VISIT4   => 'Visit 4',
      :VISIT5   => 'Visit 5',
      :VISIT6   => 'Visit 6',
      :VISIT7   => 'Visit 7',
      :VISIT10  => 'Visit 10',
      :RT       => 'Unscheduled',
  }.freeze

  CODE_KEY_INSERT =
      'INSERT INTO code_key
             (barcode,
              code_key,
              parent_code_key,
              comments
             ) VALUES'

  def initialize(logger)
    @logger = logger
    @logger.debug "#{self.class.name} filer Initialized"
  end

  def reader(inbound_file)
    @logger.debug "File name is ->#{inbound_file}<-"
    @logger.info "#{self.class.name} reader start"
    @inbound_lines = CSV.read(inbound_file, col_sep: '|', headers: true, skip_blanks: true, skip_lines: '\r', encoding:'windows-1256:utf-8')
    codekey_file = inbound_file.sub! 'DNAINVENTORY', 'KEYFILE'
    @codekey_lines = CSV.read(codekey_file, col_sep: ',', headers: true, skip_blanks: true, skip_lines: '\r', encoding:'windows-1256:utf-8')
    @logger.info "#{self.class.name} reader end"
    @inbound_lines.length
  end

  def processor(this_connection)
    @db_connection = this_connection
    @logger.info "#{self.class.name} processor start'"

    @processing_lines = Array.new
    lines = 0

    @inbound_lines.each do |specline|
      lines += 1

      # Skip non DNA specimens as they have been loaded before.
      if specline['Investigator'].strip != 'SMART'
        next
      end

      if specline['Container'] == '0' or specline['Receipt Date'].nil?
        next
      end

      @processing_lines << specline
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

    values_clause         = Array.new
    parent_ids            = Array.new
    code_key_value_clause = Array.new

    @processing_lines.each do |outline|

      if (key_line = @codekey_lines.find {|key_row| key_row['ID'][0..-3] == outline['Accession']}).nil?
        @logger.info '================= QUERY NEED ===================='
        @logger.info "RECORD EXCLUDED - No specimens in key file for accession ->#{outline['Accession']}<-"
        @logger.info "Full record ->#{outline}<-"
        next
      end

      site_id                 = key_line['SUBJECT_ID'][-6..-4].rjust(6, '0')
      subject_id              = key_line['SUBJECT_ID'][-3..-1]

      if outline['Container'] == '5' or
         outline['Container'] == '6' or
         outline['Container'] == '7'

        dna_concentration       = key_line['CONC.'].insert_value
        dna_concentration_units = key_line['CONC_UNIT'].insert_value

        dna_volume              = key_line['VOLUME'].insert_value
        dna_volume_units        = key_line['VOL_UNIT'].insert_value

        dna_yield               = key_line['NANO_YIELD'].insert_value
        dna_yield_units         = key_line['NANO_UNIT'].insert_value

        dna_quality             = key_line['NANO_QUALITY'].insert_value
      else
        dna_concentration       = ' NULL,'
        dna_concentration_units = ' NULL,'

        dna_volume              = ' NULL,'
        dna_volume_units        = ' NULL,'

        dna_yield               = ' NULL,'
        dna_yield_units         = ' NULL,'

        dna_quality             = ' NULL,'
      end

      shipped_date            = (outline['Ship Date'].nil?) ? ' NULL,' : "  STR_TO_DATE(#{outline['Ship Date'].insert_value} '%d%b%Y'),"
      specimen_type           = (outline['Type'].nil?)      ? ' NULL,' : " #{SPECIMEN_TYPE[('S' + outline['Type']).to_sym].insert_value}"

      specimen_barcode        = "'De-identified',"

      if outline['Class Desc'].include? 'DNA'
        designation           = 'DNA'.insert_value
      elsif outline['Class Desc'].include? 'RNA'
        designation           = 'RNA'.insert_value
      elsif outline['Class Desc'].include? 'Biospy'
        designation           = 'Biospy'
      else
        designation           = 'NULL,'
      end

      designation_detail      = outline['Class Desc'].insert_value

      if outline['Label 2'].nil? and outline[20].nil?
        testing_desc  = ' NULL,'
      elsif outline['Expected Class'].nil?
        testing_desc  = "'#{outline['Label 2'].strip}',"
      elsif outline['Label 2'].nil?
        testing_desc  = "'#{outline['Expected Class'].strip}',"
      else
        testing_desc  = "'#{outline['Label 2'].strip} / #{outline['Expected Class'].strip}',"
      end

      if outline['Parent'].nil?
        parent_id = " NULL,"
        ischild   = "'N',"
      else
        parent_id = "'De-identified',"
        ischild   = "'Y',"
      end

      if @logger.level == Logger::DEBUG
        @logger.info "SecureRandom would be ->#{SecureRandom.uuid}<-"
        code_key    = outline['Accession']+'-'+outline['Container']
      else
        code_key    = SecureRandom.uuid
      end

      if ischild == "'N',"
        parent_ids << Parent_Key.new(outline['Accession']+'-'+outline['Container'], code_key)
        @parent_code_key = 'NULL,'
      else
        parent_code_key_row = parent_ids.find {|id_row| id_row.parent_id == outline['Accession']+'-'+outline['Parent']}
        if parent_code_key_row.nil?
          @logger.info '================= QUERY NEED ===================='
          @logger.info "RECORD EXCLUDED - No parent code key for specimen ->#{key_line['ID']}<-, parent id ->#{outline['Accession']+'-'+outline['Parent']}<-"
          @logger.info "Full record ->#{outline}<-"
          next
        else
          @parent_code_key = parent_code_key_row.uuid.insert_value
        end
      end

      code_key_value_clause <<
          " (#{key_line['ID'].insert_value}"                                + # barcode
              "  #{code_key.insert_value}"                                  + # code_key
              "  #{@parent_code_key}"                                       + # parent_code_key
              "'ID Mapping needed'"                                         + # comment
              ' )'

      values_clause <<
          " (#{outline['Protocol'].insert_value}"                           + # study_protocol_id
              "  #{site_id.insert_value}"                                   + # site_number
              "  #{subject_id.insert_value}"                                + # subject_code
              ' NULL,'                                                      + # subject_gender
              ' NULL,'                                                      + # subject_DOB
              "  STR_TO_DATE(#{outline['Date'].insert_value} '%d%b%Y'),"    + # specimen_collect_date
              "  STR_TO_DATE(#{outline['Time'].insert_value} '%H:%i'),"     + # specimen_collect_time
              "  STR_TO_DATE(#{outline['Receipt Date'].insert_value} '%d%b%Y')," + # specimen_receive_datetime
              ' NULL,'                                                      + # treatment
              ' NULL,'                                                      + # arm
              "  #{VISIT_MAP[(outline['Visit'].sub(/#{'/'}/, '_')).to_sym].insert_value}" + # visit_name
              "  #{specimen_barcode}"                                       + # specimen_barcode
              ' NULL,'                                                      + # specimen_identifier
              "  #{specimen_type}"                                          + # specimen_type
              "  #{outline['Type Desc'].insert_value}"                      + # specimen_name
              "  #{designation}"                                            + # specimen_designation
              "  #{designation_detail}"                                     + # specimen_designation_detail
              "  #{parent_id}"                                              + # specimen_parent
              "  #{ischild}"                                                + # specimen_ischild
              "  #{outline['Spec Cond Desc'].insert_value}"                 + # specimen_condition
              "  'In Inventory',"                                           + # specimen_status
              "  #{outline['Exp Cond Desc'].insert_value}"                  + # specimen_comment
              "  #{dna_volume}"                                             + # dna_volume
              "  #{dna_volume_units}"                                       + # dna_volume units
              "  #{dna_concentration}"                                      + # dna_concentration
              "  #{dna_concentration_units}"                                + # dna_concentration units
              "  #{dna_yield}"                                              + # dna_yield
              "  #{dna_yield_units}"                                        + # dna_yield units
              "  #{dna_quality}"                                            + # dna_quality
              "  #{shipped_date}"                                           + # shipped_date
              "  #{outline['Ref Lab'].insert_value}"                        + # shipped_location
              "  #{testing_desc}"                                           + # testing_description
              "  #{vendor.insert_value}"                                    + # vendor_code
              "  '#{code_key}'"                                             + # code_key
              ')'
    end

    insert_statement = CODE_KEY_INSERT + code_key_value_clause.join(",\n")
    @logger.info "->#{insert_statement}<-"

    begin
      @db_connection.query(insert_statement)
    rescue Mysql2::Error => e
      @logger.error "DB Insert failure - #{e.message}"
      exit -1
    end

    @logger.info "#{self.class.name} writer end"
    values_clause
  end
end