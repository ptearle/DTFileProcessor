class CVD_spectype

  def initialize (logger, this_connection)
    @spec_types = Array.new
    @logger = logger

    begin
      spec_types = this_connection.query('SELECT gcs.name, st.key_value FROM gss_cvd_specimentypemap gcs
                                          JOIN (specimen_types st) ON (gcs.specimen_type_id = st.id);')
    rescue Mysql2::Error => e
      @logger.error "DB Specimen Type Map Select failure - #{e.message}"
      exit -1
    end

    spec_types.each { |s| @spec_types.push(s) }
  end

  def get_spectype (cvd_spectype)

    gss_spectype = @spec_types.find {|s| s['name'].strip == cvd_spectype }

    if gss_spectype.nil?
      @logger.debug "Unknown CVD specimen type ->#{cvd_spectype}<-"
      nil
    else
      gss_spectype['key_value'].strip
    end
  end
end