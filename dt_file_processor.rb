require 'rubygems'
require 'mysql2'
require 'csv'
require 'logger'
require 'aws-sdk'

#require_relative 'dt_config'
require_relative 'dt_file'

logger          = Logger.new('dt_file_processor.log', 'daily')
logger.level    = Logger::DEBUG
logger.progname = 'dt_file_processor'

logger.info '****************** START ***********************'

begin
  if ARGV.count != 4
    logger.error 'Invalid number of arguments.'
    logger.error 'Usage: dt_file_processor <vendor location> <protocol> <file type> <env>'
    logger.error 'Where <vendor location> e.g. ICON'
    logger.error '      <protocol> e.g. CA180-001'
    logger.error '      <file type> is CUMULATIVE or INCREMENTAL'
    logger.error '      <env> is PROD or TEST'
    exit -1
  end

  dt_transfers   = DT_Transfers.new(logger)

  logger.info "Number of transmissions available ... #{dt_transfers.length}"

  my_transfer = dt_transfers.get_transfer(ARGV[0], ARGV[1], ARGV[2])
  dt_transfers.process_files(my_transfer, ARGV[3])

rescue Exception => e
  logger.error "#{e.message}"
  e.backtrace.each do |trace_line|
    logger.error "#{trace_line}"
  end

  logger.error 'BAD END'
  exit -1
end

logger.info 'GOOD END'
logger.close
exit 0