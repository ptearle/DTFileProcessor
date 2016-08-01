require_relative 'dt_config'

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

    @logger.info "Looking for #{client} and #{env}"

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

  def s3_connect (client, eny)

    s3 = Aws::S3::Resource.new(credentials: Aws::Credentials.new('AKIAIJEJU7CDWIMHWNCQ', 't+T92/PFOSSpzqQ/GvFa3eKTA4qif/zagH27ZZ6U'),
                               region:     'us-standard')
    @obj = s3.bucket('cl017-test').object('uploads')

  end

  def s3_upload_file (file_path)
    @obj.upload_file(file_path, acl:'public-read')
  end
end
