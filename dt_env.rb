class DT_env

  def initialize (env)
    if ENV['OS'] =='Windows_NT'
      @root_dir = (env == 'PROD') ? 'C:\SFTP_PROD' : 'C:\SFTP_TEST'
      @dir_separator = '\\'
    else
      @root_dir = (env == 'PROD') ? '/home/vendors/gss/prod' : '/home/vendors/gss/test'
      @dir_separator = '/'
    end
  end

  def get_root_dir ()
    @root_dir + @dir_separator
  end

end