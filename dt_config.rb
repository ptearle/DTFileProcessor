require 'fileutils'

class String
  def insert_value
    "  #{'\''+self.strip.gsub(/'/,  '\'\'') +'\''},"
  end
end

class NilClass
  def insert_value
    'NULL,'
  end
end

class DB_Config

  def initialize (client, client_code, env, host, user, pass, port, database)
    @client      = client
    @client_code = client_code
    @env         = env
    @host        = host
    @user        = user
    @pass        = pass
    @port        = port
    @database    = database
  end

  attr_reader :client
  attr_reader :client_code
  attr_reader :env
  attr_reader :host
  attr_reader :user
  attr_reader :pass
  attr_reader :port
  attr_reader :database
end

class DT_Connections

  AWS_REGION    = 'us-east-1'
  ARC_DIRECTORY = 'processed'

  def initialize(logger)
    @logger  = logger
    @systems = Array.new
    @systems << DB_Config.new('BMS',
                              'CL017',
                              'TEST',
                              'demo.cluster-c4phvzpqkgwp.us-east-1.rds.amazonaws.com',
                              'root',
                              '2s4t6wp3J2PJgW7QAhrD',
                              '13306',
                              'cl017-test'
                             )

    @systems << DB_Config.new('BMS',
                              'CL017',
                              'PROD',
                              'cl017.cluster-c4phvzpqkgwp.us-east-1.rds.amazonaws.com',
                              'root',
                              'xU-C6k6JspLlS98D_5fl5g',
                              '3306',
                              'cl017'
                             )

    @systems << DB_Config.new('Takeda',
                              'CL015',
                              'TEST',
                              'demo.cluster-c4phvzpqkgwp.us-east-1.rds.amazonaws.com',
                              'root',
                              '2s4t6wp3J2PJgW7QAhrD',
                              '13306',
                              'cl015-test'
                             )

    @systems << DB_Config.new('Takeda',
                              'CL015',
                              'PROD',
                              'cl015-cluster-1.cluster-c4phvzpqkgwp.us-east-1.rds.amazonaws.com',
                              'root',
                              'zgMrv5gdW1bBWdXfGd9Sbw',
                              '3306',
                              'cl015'
                             )

    @systems << DB_Config.new('Regeneron',
                              'CL019',
                              'TEST',
                              'demo.cluster-c4phvzpqkgwp.us-east-1.rds.amazonaws.com',
                              'root',
                              '2s4t6wp3J2PJgW7QAhrD',
                              '13306',
                              'cl019-test'
                             )

    @systems << DB_Config.new('Regeneron',
                              'CL019',
                              'PROD',
                              'cl019-cluster.cluster-c4phvzpqkgwp.us-east-1.rds.amazonaws.com',
                              'root',
                              'JZcRRCo8Mn4law13VhKaDA',
                              '3306',
                              'cl019'
                             )

    @systems << DB_Config.new('gss-demo',
                              'GSS',
                              'PROD',
                              'demo.cluster-c4phvzpqkgwp.us-east-1.rds.amazonaws.com',
                              'root',
                              '2s4t6wp3J2PJgW7QAhrD',
                              '13306',
                              'gss-demo'
                             )

    @systems << DB_Config.new('gss-uat',
                              'GSS',
                              'TEST',
                              'demo.cluster-c4phvzpqkgwp.us-east-1.rds.amazonaws.com',
                              'root',
                              '2s4t6wp3J2PJgW7QAhrD',
                              '13306',
                              'gss-uat'
    )
  end

  def length
    @systems.length
  end

  def db_connect (client, env)

    @logger.debug "Looking for #{client} #{env} connection"

    @systems.each do |system|
      if (system.client == client or system.client_code == client) and system.env == env
        return Mysql2::Client.new(:host     => system.host,
                                  :username => system.user,
                                  :password => system.pass,
                                  :port     => system.port,
                                  :database => system.database)
      end
    end

    raise 'No such envirnoment'
  end

  def s3_connect (client, env)

    @logger.debug "Looking for #{client} #{env} connection"

    Aws.use_bundled_cert!

    @systems.each do |system|
      if (system.client == client or system.client_code == client) and system.env == env
        mycreds = Aws::SharedCredentials.new(:profile_name => system.database)
        return Aws::S3::Bucket.new(system.database, region: AWS_REGION, credentials: mycreds)
      end
    end

    raise 'No such envirnoment'
  end

  def s3_archive_file (mybucket, protocol, vendor, file_type, file_name, acl = nil)
    s3_key = "#{protocol}/#{vendor}/#{file_type}/#{file_name}"
    if acl.nil?
       mybucket.object(s3_key).upload_file(file_name)
    else
       mybucket.object(s3_key).upload_file(file_name, acl: acl)
    end

    FileUtils.mv file_name, ARC_DIRECTORY
  end
end
