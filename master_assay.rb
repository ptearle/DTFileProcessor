require 'mysql2'

class MasterAssay_AssayDef2
  def initialize(logger)
    @logger = logger
    @logger.debug "#{self.class.name} filer initialized"

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
      assays = this_connection.query("SELECT a.code FROM assays a;")
    rescue Mysql2::Error => e
      @logger.error "DB Assay Group Select failure - #{e.message}"
      exit -1
    end

    assays.each { |r| @assays.push(r['code']) }

    @inbound_lines.each do |inline|
      found = false
      lines += 1

      if !@assays.include?(inline[0])
        next
      end

      @processing_lines.each do |distinct_line|

        # see if we actually need the line in the file ... assay code already seen, then ignore
        if inline[0] == distinct_line[0]
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

    values_clause = Array.new

    @processing_lines.each do |outline|

      values_clause <<
          " (#{outline[0].insert_value}"                       + # Assay Code
          "  #{outline[4].insert_value}"                       + # Methodology
          "  '#{outline[2].strip}'"                              + # Assay Matrix
          ' )'
    end

    @logger.info "#{self.class.name} writer end"
    values_clause
  end
end

class MasterAssay_AssayDef3
  def initialize(logger)
    @logger = logger
    @logger.debug "#{self.class.name} filer initialized"

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
      assays = this_connection.query("SELECT a.code FROM assays a;")
    rescue Mysql2::Error => e
      @logger.error "DB Assay Group Select failure - #{e.message}"
      exit -1
    end

    assays.each { |r| @assays.push(r['code']) }

    @inbound_lines.each do |inline|
      found = false
      lines += 1

      if !@assays.include?(inline[0])
        next
      end

      @processing_lines.each do |distinct_line|

        # see if we actually need the line in the file ... assay code already seen, then ignore
        if inline[0] == distinct_line[0]
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

    values_clause = Array.new

    @processing_lines.each do |outline|

      values_clause <<
          " (#{outline[0].insert_value}"                       + # Assay Code
          " '#{outline[2].strip}'"                             + # Assay details
          ' )'
    end

    @logger.info "#{self.class.name} writer end"
    values_clause
  end
end

class MasterAssay_AssayDef4
  def initialize(logger)
    @logger = logger
    @logger.debug "#{self.class.name} filer initialized"

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
      assays = this_connection.query("SELECT a.code FROM assays a;")
    rescue Mysql2::Error => e
      @logger.error "DB Assay Group Select failure - #{e.message}"
      exit -1
    end

    assays.each { |r| @assays.push(r['code']) }

    @inbound_lines.each do |inline|
      found = false
      lines += 1

      if !@assays.include?(inline[0])
        next
      end

      @processing_lines.each do |distinct_line|

        # see if we actually need the line in the file ... assay code already seen, then ignore
        if inline[0] == distinct_line[0]
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

    values_clause = Array.new

    @processing_lines.each do |outline|

      values_clause <<
          " (#{outline[0].insert_value}"                       + # Assay Code
          " '#{outline[4].strip}'"                             + # Sample Storage Conditions
          ' )'
    end

    @logger.info "#{self.class.name} writer end"
    values_clause
  end
end


class MasterAssay_AssayDef5
  def initialize(logger)
    @logger = logger
    @logger.debug "#{self.class.name} filer initialized"

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
      assays = this_connection.query("SELECT a.code FROM assays a;")
    rescue Mysql2::Error => e
      @logger.error "DB Assay Group Select failure - #{e.message}"
      exit -1
    end

    assays.each { |r| @assays.push(r['code']) }

    @inbound_lines.each do |inline|
      found = false
      lines += 1

      if !@assays.include?(inline[1])
        next
      end

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

    values_clause = Array.new

    @processing_lines.each do |outline|

      equipment_used = (outline[6] == 'Not Applicable') ? ' NULL,' : outline[6].insert_value
      volume         = (outline[7].nil?)                ? ' NULL'  : outline[7]

      @logger.info "Outline[7] ->#{outline[7]}<- is volume ->#{volume}<-"

      volume_unit    = (outline[8].nil?)                ? ' NULL'  : outline[8].strip

      values_clause <<
          " (#{outline[1].insert_value}"               + # Assay Code
          " #{equipment_used}"                         + # equipment used
          " #{volume},"                                + # volume_of_matrix
          " '#{volume_unit}'"                          + # volume units
          ' )'
    end

    @logger.info "#{self.class.name} writer end"
    values_clause
  end
end
