class R668_AD_1117_LCRP_DNA
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
      :S2	  => 'Serum',
      :S3   => 'Serum',
      :S4	  => 'Serum',
      :S9   => 'Plasma',
      :S12	=> 'Plasma',
      :S13	=> 'Plasma',
      :S15  => 'Urine',
      :S16	=> 'Urine',
      :S24	=> 'Whole Blood',
      :S31	=> 'Whole Blood',
      :S46	=> 'Whole Blood',
      :S50  => 'Whole Blood',
      :S160	=> 'Plasma',
      :S481 => 'Mucus',
      :S533 => 'Biospy',
      :S353 => 'Whole Blood',
      :S370 => 'RNA',
      :S725	=> 'DNA',
      :S875 => 'Biospy',
      :S908	=> 'Whole Blood',
  }.freeze

  VISIT_MAP = {
      :V17EOS      => 'Visit 17',
      :V22EOS      => 'Visit 22',
      :VET         => 'Early Termination ',
      :VRT         => 'Visit 99',
      :VV17FU      => 'Visit 17',
      :VVIS14OCT   => 'Visit 14',
      :VVIS2OCT    => 'Visit 2',
      :VVIS6OCT    => 'Visit 6',
      :VVISIT1     => 'Visit 1',
      :VVISIT10    => 'Visit 10',
      :VVISIT11    => 'Visit 11',
      :VVISIT12    => 'Visit 12',
      :VVISIT13    => 'Visit 13',
      :VVISIT14    => 'Visit 14',
      :VVISIT15    => 'Visit 15',
      :VVISIT16    => 'Visit 16',
      :VVISIT17    => 'Visit 17',
      :VVISIT18    => 'Visit 18',
      :VVISIT19    => 'Visit 19',
      :VVISIT2     => 'Visit 2',
      :VVISIT20    => 'Visit 20',
      :VVISIT21    => 'Visit 21',
      :VVISIT22    => 'Visit 22',
      :VVISIT3     => 'Visit 3',
      :VVISIT4     => 'Visit 4',
      :VVISIT5     => 'Visit 5',
      :VVISIT6     => 'Visit 6',
      :VVISIT7     => 'Visit 7',
      :VVISIT8     => 'Visit 8',
      :VVISIT9     => 'Visit 9',
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
      if specline['Investigator'].strip != 'SMART' and
          specline['Investigator'].strip != 'DR.SMART' and
          specline['Investigator'].strip != 'SMRT602090'
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
      @logger.info ("Nothing to insert :(")
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

      site_id                 = key_line['SUBJECT_ID'][-6..-4].rjust(3, '0')
      subject_id              = key_line['SUBJECT_ID'][-3..-1]

      if !key_line['VOLUME'].nil?

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

      if outline['Type Desc'].include? 'Biospy'
        designation           = 'Biospy'.insert_value
      elsif outline['Type Desc'].include? 'DNA'
        designation           = 'DNA'.insert_value
      elsif outline['Type Desc'].include? 'RNA'
        designation           = 'RNA'.insert_value
      else
        designation           = 'Whole Blood'.insert_value
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
              "  #{VISIT_MAP[(('V'+outline['Visit'].sub(/#{'/'}/, '_'))).to_sym].insert_value}" + # visit_name
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