require 'mysql2'

class C33001_site

  COUNTRY_CAPITAL =
      {
          :Belgium           => 'Brussels',
          :Canada            => 'Ottawa',
          :France            => 'Paris',
          :Japan             => 'Tokyo',
          :NewZealand        => 'Wellington',
          :Russia            => 'Moscow',
          :KoreaRepublicof   => 'Seoul',
          :UnitedStates      => 'Washington',
          :RussianFederation => 'Moscow'
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
      lines += 1

      if specline['ns2:Status'] == 'Disqualified' or
         specline['ns2:Status'] == 'Withdrawn' or
         specline['ns2:Status'] == 'Void'
        next
      end

      @processing_lines << specline
      num_distinct_lines += 1
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

      country_capital = (outline['ns2:Study_Country_Name'].nil?)  ? ' NULL,' : "#{COUNTRY_CAPITAL[(outline['ns2:Study_Country_Name'].gsub(/[^a-zA-Z0-9]/, '')).to_sym]}".insert_value

      case outline['ns2:Status']
        when 'Active - No Longer Recruiting',
             'Active - Recruiting',
             'Active - Not Yet Recruiting'
          site_status = "'Active',"
        when 'Close-Out Ready',
             'Closed'
          site_status = "'Closed',"
        else
          site_status = 'NULL,'
      end

      values_clause <<
          "(#{outline['ns2:Protocol_Number'].insert_value}"           + # study_protocol_id
              " #{outline['ns2:Site_Number'].insert_value}"           + # site_number
              " #{outline['ns2:Site_Number'].insert_value}"           + # site_name
              ' NULL,'                                                + # site_address
              " #{country_capital}"                                   + # site_city
              ' NULL,'                                                + # site_state
              " #{outline['ns2:Study_Country_Name'].insert_value}"    + # site_country
              ' NULL,'                                                + # site_postal_code
              ' NULL,'                                                + # site_phone
              ' NULL,'                                                + # site_fax
              ' NULL,'                                                + # site_FPFV
              ' NULL,'                                                + # site_LPLV
              ' NULL,'                                                + # planned_enrollment
              " #{(outline['ns2:PI_Last_Name']+', '+outline['ns2:PI_First_Name']).insert_value}" + # site_PI
              ' NULL,'                                                + # site_PI_email
              ' NULL,'                                                + # site_coordinator
              ' NULL,'                                                + # site_coordinator_email
              " #{site_status}"                                       + # site_status
              " '#{vendor}'"                                          + # vendor_code
              ' )'
    end

    @logger.info "#{self.class.name} writer end"
    values_clause
  end
end


class C33001_subject

  SUBJECT_STATUS =
      {
          :Completed          => 'Randomized',
          :EarlyTerminated    => 'Discontinued',
          :Enrolled           => 'Screened',
          :Randomized         => 'Randomized',
          :ScreenFailure      => 'Screen Failed',
          :TxDiscontinued     => 'Discontinued',
          :EnrollmentFailure  => 'Discontinued',
          :Screened           => 'Screened',
          :Rescreened         => 'Screened'
      }.freeze

  def initialize(logger)
    @logger = logger
    @logger.debug "#{self.class.name} filer Initialized"
  end

  def reader(inbound_file)
    @logger.debug "File name is ->#{inbound_file}<-"
    @logger.info "#{self.class.name} reader start"
    @inbound_lines = CSV.read(inbound_file, headers: true, skip_blanks: true, skip_lines: '\r', encoding:'windows-1256:utf-8')
    @logger.info "#{self.class.class.name} reader end"
    @inbound_lines.length
  end

  def processor(this_connection)
    @logger.info "#{self.class.name} processor"

    @processing_lines = Array.new

    @inbound_lines.each do |specline|

      subject_status = (specline['ns2:Primary_Status'].nil?)

      if specline['ns2:Primary_Status'].nil? or SUBJECT_STATUS[(specline['ns2:Primary_Status'].gsub(/[^a-zA-Z0-9]/, '')).to_sym].nil?
        @logger.info '================= QUERY NEED ===================='
        @logger.info "Unknown subject status ->#{specline['ns2:Primary_Status']}<- on subject ->#{specline['ns2:Subject_Number']}<-"
        @logger.info "Full record ->#{specline}<-"
        next
      end

      @processing_lines << specline
    end

    @logger.info "#{self.class.name} processor end"
    @processing_lines.length
  end

  def writer(vendor)
    @logger.info "#{self.class.name} writer start'"

    if @processing_lines.count == 0
      logger.info ("Nothing to insert :(")
      exit 0
    end

    values_clause = Array.new

    @processing_lines.each do |outline|

      subject_treatment   = ' NULL,'
      subject_arm         = ' NULL,'
      date_of_birth       = ' NULL,'
      enrollment_status   = SUBJECT_STATUS[(outline['ns2:Primary_Status'].gsub(/[^a-zA-Z0-9]/, '')).to_sym].insert_value

      values_clause <<
          "( #{outline['ns2:Protocol_Number'].insert_value}"     + # study_protocol_id
              " #{outline['ns2:Site_Number'].insert_value}"         + # site_number
              " #{outline['ns2:Subject_Number'].insert_value}"      + # subject_code
              " #{outline['ns2:Subject_Number'].insert_value}"      + # subject_external_id
              ' NULL,'                                              + # randomization_number
              ' NULL,'                                              + # gender
              ' NULL,'                                              + # initials
              " #{enrollment_status}"                               + # enrollment_status
              " #{date_of_birth}"                                   + # date_of_birth 1-Jan-34
              ' NULL,'                                              + # address
              ' NULL,'                                              + # city
              ' NULL,'                                              + # state
              ' NULL,'                                              + # region
              ' NULL,'                                              + # country
              ' NULL,'                                              + # postcode
              ' NULL,'                                              + # primary_race
              ' NULL,'                                              + # secondary_race
              ' NULL,'                                              + # ethnicity
              " #{subject_treatment}"                               + # treatment
              " #{subject_arm}"                                     + # arm
              ' NULL,'                                              + # ICF_signing_date
              ' NULL,'                                              + # ICF_withdrawl_date
              " '#{vendor}'"                                        + # vendor_code
              ")"
    end

    @logger.info "#{self.class.name} writer end"
    values_clause
  end
end
