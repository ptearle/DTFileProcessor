class Data_Feed

  my_select = "SELECT   MAX(dsv.id),
                          dsv.site_number,
                          dsv.subject_code,
                          dsv.arm
                 FROM     dts_subject_v1_0 dsv
                 WHERE    dsv.study_protocol_id = 'R668-AD-1334'
                 GROUP BY dsv.site_number,
                          dsv.subject_code;"

  def initialize (db, )
  end