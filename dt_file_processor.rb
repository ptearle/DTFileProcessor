require 'rubygems'
require 'mysql2'
require 'csv'
require 'logger'
require 'aws-sdk'

require_relative 'dt_env'
require_relative 'dt_file'

my_env = DT_env.new(ARGV[3])

logger          = Logger.new("#{my_env.get_root_dir}#{ARGV[1]}_#{ARGV[2]}_#{ARGV[0]}.log", shift_age = 'daily')
logger.level    = Logger::INFO
logger.progname = 'dt_file_processor'

logger.info '****************** START ***********************'

begin
  if ARGV.count != 4
    puts 'Invalid number of arguments.'
    puts 'Usage: dt_file_processor <vendor location> <protocol> <file type> <env>'
    puts 'Where <vendor location> e.g. ICON'
    puts '      <protocol> e.g. CA180-001'
    puts '      <file type> is SITE or INVENTORY'
    puts '      <env> is PROD or TEST'
    exit -1
  end

  dt_transfers    = DT_Transfers.new(logger)

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