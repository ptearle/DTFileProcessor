require 'mysql2'

class D5136C00008_AssayGrp
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

    @inbound_lines.each do |inline|
      found = false
      lines += 1

      @processing_lines.each do |distinct_line|
        # Check if this is an amdin question test code ADTxxx.  If so set grp to "Administrative Questions"
        if inline[1][0..2] == 'ADT'
          inline[4] = 'Administrative Questions'
        end

        # see if we actually need the line in the file ... assay group already seen, then ignore
        if inline[4] == distinct_line[4]
          found = true
          break
        end
      end

      if !found
        @processing_lines << inline
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

      values_clause <<
          " (#{outline[4].insert_value}"                           + # Assay Group Name
          '  1'                                                   + # Assay Group Version
          ' )'
    end

    @logger.info "#{self.class.name} writer end"
    values_clause
  end
end

class D5136C00008_AssayDef
  def initialize(logger)
    @logger = logger
    @logger.debug "#{self.class.name} filer initialized"

    @master_program_id = 0
    @vendor_id = 0
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

    begin
      master_program = this_connection.query("SELECT mp.master_program_id  FROM master_protocols mp WHERE mp.name = '#{@inbound_lines.first[0].strip}';")
    rescue Mysql2::Error => e
      @logger.error "DB Master Program Select failure - #{e.message}"
      exit -1
    end

    @master_program_id = master_program.first['master_program_id']

    begin
      vendor = this_connection.query("SELECT v.id  FROM vendors v WHERE v.data_feed_code = 'CVD'")
    rescue Mysql2::Error => e
      @logger.error "DB Vendor Select failure - #{e.message}"
      exit -1
    end

    @vendor_id = vendor.first['id']

    @inbound_lines.each do |inline|
      found = false
      lines += 1

      @processing_lines.each do |distinct_line|

        # see if we actually need the line in the file ... assay code already seen, then ignore
        if inline[1] == distinct_line[1]
          found = true
          break
        end
      end

      if !found
        @processing_lines << inline
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

    if @vendor_id == 0
      @logger.info {"Unknown vendor code"}
      exit 0
    end

    if @master_program_id == 0
      @logger.info ("Unknown master program or protocol")
      exit 0
    end

    values_clause = Array.new

    @processing_lines.each do |outline|

      values_clause <<
          " (#{outline[1].insert_value}"                           + # Assay Code
              "  #{outline[2].insert_value}"                       + # Assay Name
              "  'Active',"                                        + # Assay Status
              "  #{@vendor_id},"                                   + # Vendor
              "  #{@master_program_id}"                            + # Master Program
          ' )'
    end

    @logger.info "#{self.class.name} writer end"
    values_clause
  end
end

class D5136C00008_AssayGrpAssayDef
  def initialize(logger)
    @logger = logger
    @logger.debug "#{self.class.name} filer initialized"

    @assayGrps = Array.new
    @assays    = Array.new
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

    begin
      assay_grps = this_connection.query("SELECT ag.id, ag.name  FROM assay_groups ag;")
    rescue Mysql2::Error => e
      @logger.error "DB Assay Group Select failure - #{e.message}"
      exit -1
    end

    assay_grps.each { |r| @assayGrps.push(r) }

    begin
      assays = this_connection.query("SELECT a.id, a.code  FROM assays a;")
    rescue Mysql2::Error => e
      @logger.error "DB Assay Group Select failure - #{e.message}"
      exit -1
    end

    assays.each { |r| @assays.push(r) }

    @inbound_lines.each do |inline|
      found = false
      lines += 1

      @processing_lines.each do |distinct_line|

        # see if we actually need the line in the file ... assay code already seen, then ignore
        if inline[1] == distinct_line[1]
          found = true
          break
        end
      end

      if !found
        @processing_lines << inline
        num_distinct_lines += 1
      end
    end

    @logger.info "#{self.class.name} processor end"
    @processing_lines.length
  end

  def writer(vendor)
    @logger.info "#{self.class.name} writer start"

    values_clause = Array.new

    @processing_lines.each do |outline|

#      assayGrpIndex = @assayGrps.find_index {|i| i['name'] == outline[4].strip}
#      @logger.info "Index of '#{outline[4].strip}' is ->#{assayGrpIndex}<-"
#      @logger.info "Id of '#{outline[4].strip}'    is ->#{@assayGrps[@assayGrps.find_index {|i| i['name'] == outline[4].strip}]['id']}<-"
#
#      assayIndex = @assays.find_index {|i| i['code'] == outline[1].strip}
#      @logger.info "Index of '#{outline[1].strip}' is ->#{assayIndex}<-"
#      @logger.info "Id of '#{outline[1].strip}'    is ->#{@assays[@assays.find_index {|i| i['code'] == outline[1].strip}]['id']}<-"


      values_clause <<
          ' ('                                                              +
          "  #{@assays[@assays.find_index {|i| i['code'] == outline[1].strip}]['id']},"       + # Assay Code id
          "  #{@assayGrps[@assayGrps.find_index {|i| i['name'] == outline[4].strip}]['id']}"  + # Assay Group id
          ' )'
    end

    @logger.info "#{self.class.name} writer end"
    values_clause
  end
end
