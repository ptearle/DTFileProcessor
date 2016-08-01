require 'aws-sdk'
require 'fileutils'

require_relative 'ca018001'
require_relative 'dt_config'

class DT_File

  def initialize (vendor, client, protocol, file_type, mode, filer, logger)
    @vendor    = vendor
    @client    = client
    @protocol  = protocol
    @file_type = file_type
    @mode      = mode
    @filer     = filer


  end

  attr_reader :vendor
  attr_reader :client
  attr_reader :protocol
  attr_reader :file_type
  attr_reader :mode
  attr_reader :filer
end

class DT_Transfers

  PROD_DIR = 'C:\SFTP_PROD'
  TEST_DIR = 'C:\SFTP_TEST'
  DIR_SEPARATOR = '\\'

  def initialize(logger)
    @transfers = Array.new
    logger.info 'Initializing LCRP INVENTORY filer for CA018-001'
    @transfers << DT_File.new('LCRP',
                              'BMS',
                              'CA018-001',
                              'INVENTORY',
                              'CUMULATIVE',
                              CA018001_EDD.new(logger),
                              logger)
    logger.info 'Initializing BHPW INVENTORY filer for CA018-001'
    @transfers << DT_File.new('BHPW',
                              'BMS',
                              'CA018-001',
                              'INVENTORY',
                              'CUMULATIVE',
                              CA018001_BMS.new(logger),
                              logger)
    logger.info 'Initializing BFLC INVENTORY filer for CA018-001'
    @transfers << DT_File.new('BFLC',
                              'BMS',
                              'CA018-001',
                              'INVENTORY',
                              'CUMULATIVE',
                              CA018001_BMS.new(logger),
                              logger)
    logger.info 'Initializing QATL CLINVENTORY filer for CA018-001'
    @transfers << DT_File.new('QATL',
                              'BMS',
                              'CA018-001',
                              'CLINVENTORY',
                              'CUMULATIVE',
                              CA018001_QINV.new(logger),
                              logger)
    @my_connections = DT_Connections.new(logger)
    @logger = logger
  end

  def length
    @transfers.length
  end

  def get_transfer (vendor, protocol, file_type)
    @transfers.each do |this_transfer|
       if this_transfer.vendor == vendor and
         this_transfer.protocol == protocol and
         this_transfer.file_type == file_type
        return this_transfer
      end
    end

    raise "No such transfer for #{vendor} #{protocol} #{file_type}"

  end

  def process_files (this_transfer, env)
    @logger.info "#{(env  == 'PROD') ? 'Production' : 'Test'} file processing start - #{this_transfer.vendor} #{this_transfer.file_type} file for protocol #{this_transfer.protocol}"

    file_path = ((env == 'PROD') ? PROD_DIR : TEST_DIR) + DIR_SEPARATOR + this_transfer.vendor
    file_mask = this_transfer.protocol +
                '-' +
                this_transfer.vendor +
                '-' +
                this_transfer.file_type +
                "*.csv"
    Dir.chdir(file_path)
    file_list = Dir["#{file_mask}"]

    @logger.debug "Directory is ->#{file_path}<-"
    @logger.debug "Filemask is ->#{file_mask}<-"

    if this_transfer.mode == 'CUMULATIVE'
      if file_list.length == 0
        @logger.info 'No files to process'
        return
      end

      @logger.info("Processing #{file_list.last} in "+Dir.pwd+".")

      if (num_in = this_transfer.filer.reader(file_list.last)) == 0
        @logger.info 'No records read in'
        return
      end

      @logger.info("#{num_in} read in")

      if (num_to_process = this_transfer.filer.processor) == 0
        logger.info 'No records to process'
      end

      @logger.info("#{num_to_process} to process")

      insert_statement = 'INSERT INTO dts_specimen_inventory_v1_0 (study_protocol_id,
                                                                   site_number,
                                                                   subject_num,
                                                                   subject_gender,
                                                                   subject_date_of_birth,
                                                                   specimen_collect_date,
                                                                   specimen_collect_time,
                                                                   received_datetime,
                                                                   visit_name,
                                                                   specimen_barcode,
                                                                   specimen_identifer,
                                                                   specimen_type,
                                                                   specimen_parent_id,
                                                                   specimen_ischild,
                                                                   specimen_condition,
                                                                   specimen_status,
                                                                   shipped_date,
                                                                   vendor_code
                                                                  )
            VALUES'
      insert_statement += this_transfer.filer.writer(this_transfer.vendor)

      @logger.debug (insert_statement)

      begin
        this_connection = @my_connections.db_connect(this_transfer.client, env)
      rescue Exception => e
        @logger.error "DB Connection failure - #{e.message}"
        exit -1
      end

      begin
        this_connection.query(insert_statement)
      rescue Mysql2::Error => e
        @logger.error "DB Insert failure - #{e.message}"
        exit -1
     end

#     begin
#        this_connection.query('CALL load_dts_specimen_inventory_v1_0')
#      rescue Mysql2::Error => e
#        @logger.error "DB Load Execution failure - #{e.message}"
#        exit -1
#      end

      file_list.each do |my_file|
        FileUtils.mv(my_file, 'processed')
      end
    else
      @logger.error 'Incremental data loads not supported yet'
      exit -1
    end

    @logger.info 'File processing end'
  end
end