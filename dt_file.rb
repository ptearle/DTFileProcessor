require 'aws-sdk'

# DT Support functions
require_relative 'dt_config'

# Regeneron
require_relative 'regeneron'
require_relative 'r1033_hv_1107'
require_relative 'r1033_hv_1204'
require_relative 'r2222_hv_1326'
require_relative 'r2009_hv_1304'
require_relative 'r727_cl_1118'
require_relative 'r727_cl_1112'
require_relative 'r727_cl_1308'
require_relative 'r1033_src_1239'
require_relative 'il1t-ga-1101'
require_relative 'r668-ad-1416'
require_relative 'r1033_hv_1223'
require_relative 'r1193_dm_1402'
require_relative 'r2176_3_amd_1303'
require_relative 'r668_ad_1307'
require_relative 'r668_ad_1314'
require_relative 'r668_ad_1334'
require_relative 'r1500-cl-1321'
require_relative 'r1908_1909_alg_1325'

#Takeda
require_relative 'c16014'
require_relative 'c16019'
require_relative 'c16021'

# BMS FRACTION
require_relative 'ca018001'

class DT_File

  def initialize (vendor, client, protocol, file_type, file_version, mode, filer, logger)
    @vendor       = vendor
    @client       = client
    @protocol     = protocol
    @file_type    = file_type
    @file_version = file_version
    @mode         = mode
    @filer        = filer
  end

  attr_reader :vendor
  attr_reader :client
  attr_reader :protocol
  attr_reader :file_type
  attr_reader :file_version
  attr_reader :mode
  attr_reader :filer
end

class DT_Transfers

  puts "->#{ENV['OS']}<-"

  if ENV['OS'] =='Windows_NT'
    PROD_DIR = 'C:\SFTP_PROD'
    TEST_DIR = 'C:\SFTP_TEST'
    DIR_SEPARATOR = '\\'
  else
    PROD_DIR = '/home/vendors/gss/prod'
    TEST_DIR = '/home/vendors/gss/test'
    DIR_SEPARATOR = '/'
  end

  INSERT_FRAME = 9000
  INSERT_STATEMENTS = {
      :INVENTORY_V1_0 =>
          '
           INSERT INTO dts_specimen_inventory_v1_0
             (study_protocol_id,
              site_number,
              subject_num,
              subject_gender,
              subject_date_of_birth,
              specimen_collect_date,
              specimen_collect_time,
              received_datetime,
              treatment,
              arm,
              visit_name,
              specimen_barcode,
              specimen_identifier,
              specimen_type,
              specimen_name,
              specimen_designation,
              specimen_designation_detail,
              specimen_parent_id,
              specimen_ischild,
              specimen_condition,
              specimen_status,
              specimen_comment,
              shipped_date,
              shipped_location,
              testing_description,
              vendor_code
             ) VALUES',
      :ASSAY_V1_0 =>
          '
           INSERT INTO dts_assay_results_v1_0
             (study_protocol_id,
              site_number,
              subject_number,
              collection_date,
              visit_name,
              lab_barcode,
              analysis_barcode,
              assay_batch_id,
              exclusion_flag,
              assay_date,
              result_repeated,
              replicate_number,
              reported_datetime,
              reported_rr_high,
              reported_rr_low,
              result_categorical,
              result_categorical_code_list,
              result_category,
              assay_comment,
              result_numeric,
              result_numeric_precision,
              result_type,
              result_units,
              assay_run_id,
              vendor_id,
              analyte,
              assay_code,
              assay_description,
              assay_method,
              assay_name,
              assay_protocol_id,
              assay_protocol_version,
              equipment_used,
              lab_assay_protocol_id,
              lab_assay_protocol_version,
              lab_test_name,
              lab_test_number,
              LOINC_code,
              sample_storage_conditions,
              sensitivity,
              assay_status,
              test_type,
              vendor_code
             ) VALUES',
      :SITE_V1_0 =>
          '
           INSERT INTO dts_site_v1_0
             (study_protocol_id,
              site_number,
              site_name,
              site_address,
              site_city,
              site_state,
              site_country,
              site_postal_code,
              site_phone,
              site_fax,
              site_FPFV,
              site_LPLV,
              planned_enrollment,
              site_PI,
              site_PI_email,
              site_coordinator,
              site_coordinator_email,
              site_status,
              vendor_code
             ) VALUES',
      :SUBJECT_V1_0 =>
          '
           INSERT INTO dts_subject_v1_0
             (study_protocol_id,
              site_number,
              subject_code,
              subject_external_id,
              randomization_number,
              gender,
              initials,
              enrollment_status,
              date_of_birth,
              address,
              city,
			        state,
			        region,
			        country,
			        postcode,
			        primary_race,
			        secondary_race,
			        ethnicity,
              treatment,
              arm,
              ICF_signing_date,
              ICF_withdrawl_date,
			        vendor_code
             ) VALUES',
      :IVRT_V1_0 =>
          '
           INSERT INTO dts_subject_v1_0
             (study_protocol_id,
              site_number,
              subject_code,
              subject_external_id,
              randomization_number,
              gender,
              initials,
              enrollment_status,
              date_of_birth,
              address,
              city,
			        state,
			        region,
			        country,
			        postcode,
			        primary_race,
			        secondary_race,
			        ethnicity,
              treatment,
              arm,
              ICF_signing_date,
              ICF_withdrawl_date,
			        vendor_code
             ) VALUES',
    }.freeze

  def initialize(logger)
    @transfers = Array.new
    @transfers << DT_File.new('ICON',
                              'BMS',
                              'CA018-001',
                              'IVRT',
                              'V1_0',
                              'CUMULATIVE',
                              CA018001_ivrt.new(logger),
                              logger)
    @transfers << DT_File.new('ICON',
                              'BMS',
                              'CA018-001',
                              'SITE',
                              'V1_0',
                              'CUMULATIVE',
                              CA018001_CTMS.new(logger),
                              logger)
    @transfers << DT_File.new('LCRP',
                              'BMS',
                              'CA018-001',
                              'INVENTORY',
                              'V1_0',
                              'CUMULATIVE',
                              CA018001_EDD.new(logger),
                              logger)
    @transfers << DT_File.new('BHPW',
                              'BMS',
                              'CA018-001',
                              'INVENTORY',
                              'V1_0',
                              'CUMULATIVE',
                              CA018001_BMS.new(logger),
                              logger)
    @transfers << DT_File.new('BFLC',
                              'BMS',
                              'CA018-001',
                              'INVENTORY',
                              'V1_0',
                              'CUMULATIVE',
                              CA018001_BMS.new(logger),
                              logger)
    @transfers << DT_File.new('QATL',
                              'BMS',
                              'CA018-001',
                              'INVENTORY',
                              'V1_0',
                              'CUMULATIVE',
                              CA018001_QINV.new(logger),
                              logger)
    @transfers << DT_File.new('QATL',
                              'BMS',
                              'CA018-001',
                              'ASSAY',
                              'V1_0',
                              'CUMULATIVE',
                              CA018001_QASY.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R688-AD-1021',
                              'SITE',
                              'V1_0',
                              'CUMULATIVE',
                              Regeneron_site.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R688-AD-1021',
                              'SUBJECT',
                              'V1_0',
                              'CUMULATIVE',
                              Regeneron_subject.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R688-AD-1021',
                              'INVENTORY',
                              'V1_0',
                              'CUMULATIVE',
                              Regeneron_RGinv.new(logger),
                              logger)
    @transfers << DT_File.new('LCRP',
                              'Regeneron',
                              'R688-AD-1021',
                              'INVENTORY',
                              'V1_0',
                              'CUMULATIVE',
                              Regeneron_LCRP.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R727-CL-1110',
                              'SITE',
                              'V1_0',
                              'CUMULATIVE',
                              Regeneron_site.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R727-CL-1110',
                              'SUBJECT',
                              'V1_0',
                              'CUMULATIVE',
                              Regeneron_subject.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R727-CL-1110',
                              'INVENTORY',
                              'V1_0',
                              'CUMULATIVE',
                              Regeneron_RGinv.new(logger),
                              logger)
    @transfers << DT_File.new('LCRP',
                              'Regeneron',
                              'R727-CL-1110',
                              'INVENTORY',
                              'V1_0',
                              'CUMULATIVE',
                              Regeneron_LCRP.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R1033-HV-1204',
                              'SITE',
                              'V1_0',
                              'CUMULATIVE',
                              Regeneron_site.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R1033-HV-1204',
                              'SUBJECT',
                              'V1_0',
                              'CUMULATIVE',
                              Regeneron_subject.new(logger),
                              logger)
   @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R1033-HV-1204',
                              'INVENTORY',
                              'V1_0',
                              'CUMULATIVE',
                              R1033_HV_1204_RGInv.new(logger),
                              logger)
    @transfers << DT_File.new('LCRP',
                              'Regeneron',
                              'R1033-HV-1204',
                              'INVENTORY',
                              'V1_0',
                              'CUMULATIVE',
                              R1033_HV_1204_LCRP.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R727-CL-1118',
                              'SITE',
                              'V1_0',
                              'CUMULATIVE',
                              R727_CL_1118_site.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R727-CL-1118',
                              'SUBJECT',
                              'V1_0',
                              'CUMULATIVE',
                              R727_CL_1118_subject.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R727-CL-1118',
                              'INVENTORY',
                              'V1_0',
                              'CUMULATIVE',
                              R727_CL_1118_RGRNinv.new(logger),
                              logger)
    @transfers << DT_File.new('LCRP',
                              'Regeneron',
                              'R727-CL-1118',
                              'INVENTORY',
                              'V1_0',
                              'CUMULATIVE',
                              R727_CL_1118_LCRPInv.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R727-CL-1112',
                              'SITE',
                              'V1_0',
                              'CUMULATIVE',
                              R727_CL_1112_site.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R727-CL-1112',
                              'SUBJECT',
                              'V1_0',
                              'CUMULATIVE',
                              R727_CL_1112_subject.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R727-CL-1112',
                              'INVENTORY',
                              'V1_0',
                              'CUMULATIVE',
                              R727_CL_1112_RGRNinv.new(logger),
                              logger)
    @transfers << DT_File.new('LCRP',
                              'Regeneron',
                              'R727-CL-1112',
                              'INVENTORY',
                              'V1_0',
                              'CUMULATIVE',
                              R727_CL_1112_LCRPInv.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R727-CL-1308',
                              'SITE',
                              'V1_0',
                              'CUMULATIVE',
                              R727_CL_1308_site.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R727-CL-1308',
                              'SUBJECT',
                              'V1_0',
                              'CUMULATIVE',
                              R727_CL_1308_subject.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R727-CL-1308',
                              'INVENTORY',
                              'V1_0',
                              'CUMULATIVE',
                              R727_CL_1308_RGRNinv.new(logger),
                              logger)
   @transfers << DT_File.new('MDPC',
                              'Regeneron',
                              'R727-CL-1308',
                              'INVENTORY',
                              'V1_0',
                              'CUMULATIVE',
                              R727_CL_1308_MDPCInv.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R1033-SRC-1239',
                              'SITE',
                              'V1_0',
                              'CUMULATIVE',
                              R1033_SRC_1239_site.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R1033-SRC-1239',
                              'SUBJECT',
                              'V1_0',
                              'CUMULATIVE',
                              R1033_SRC_1239_subject.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R1033-SRC-1239',
                              'INVENTORY',
                              'V1_0',
                              'CUMULATIVE',
                              R1033_SRC_1239_RGRNinv.new(logger),
                              logger)
    @transfers << DT_File.new('LCRP',
                              'Regeneron',
                              'R1033-SRC-1239',
                              'INVENTORY',
                              'V1_0',
                              'CUMULATIVE',
                              R1033_SRC_1239_LCRPInv.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'IL1T-GA-1101',
                              'SITE',
                              'V1_0',
                              'CUMULATIVE',
                              IL1T_GA_1101_site.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'IL1T-GA-1101',
                              'SUBJECT',
                              'V1_0',
                              'CUMULATIVE',
                              IL1T_GA_1101_subject.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'IL1T-GA-1101',
                              'INVENTORY',
                              'V1_0',
                              'CUMULATIVE',
                              IL1T_GA_1101_RGRNinv.new(logger),
                              logger)
    @transfers << DT_File.new('LCRP',
                              'Regeneron',
                              'IL1T-GA-1101',
                              'INVENTORY',
                              'V1_0',
                              'CUMULATIVE',
                              IL1T_GA_1101_LCRPInv.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R668-AD-1416',
                              'SITE',
                              'V1_0',
                              'CUMULATIVE',
                              R668_AD_1416_site.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R668-AD-1416',
                              'SUBJECT',
                              'V1_0',
                              'CUMULATIVE',
                              R668_AD_1416_subject.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R668-AD-1416',
                              'INVENTORY',
                              'V1_0',
                              'CUMULATIVE',
                              R668_AD_1416_RGRNInv.new(logger),
                              logger)
    @transfers << DT_File.new('PPDL',
                              'Regeneron',
                              'R668-AD-1416',
                              'INVENTORY',
                              'V1_0',
                              'CUMULATIVE',
                              R668_AD_1416_PPDLInv.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R1033-HV-1223',
                              'SITE',
                              'V1_0',
                              'CUMULATIVE',
                              R1033_HV_1223_site.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R1033-HV-1223',
                              'SUBJECT',
                              'V1_0',
                              'CUMULATIVE',
                              R1033_HV_1223_subject.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R1033-HV-1223',
                              'INVENTORY',
                              'V1_0',
                              'CUMULATIVE',
                              R1033_HV_1223_RGRNInv.new(logger),
                              logger)
    @transfers << DT_File.new('LCRP',
                              'Regeneron',
                              'R1033-HV-1223',
                              'INVENTORY',
                              'V1_0',
                              'CUMULATIVE',
                              R1033_HV_1223_LCRPInv.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R2176-3-AMD-1303',
                              'SITE',
                              'V1_0',
                              'CUMULATIVE',
                              R2176_3_AMD_1303_site.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R2176-3-AMD-1303',
                              'SUBJECT',
                              'V1_0',
                              'CUMULATIVE',
                              R2176_3_AMD_1303_subject.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R2176-3-AMD-1303',
                              'INVENTORY',
                              'V1_0',
                              'CUMULATIVE',
                              R2176_3_AMD_1303_RGRNInv.new(logger),
                              logger)
    @transfers << DT_File.new('LCRP',
                              'Regeneron',
                              'R2176-3-AMD-1303',
                              'INVENTORY',
                              'V1_0',
                              'CUMULATIVE',
                              R2176_3_AMD_1303_LCRPInv.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R668-AD-1314',
                              'SITE',
                              'V1_0',
                              'CUMULATIVE',
                              R668_AD_1314_site.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R668-AD-1314',
                              'SUBJECT',
                              'V1_0',
                              'CUMULATIVE',
                              R668_AD_1314_subject.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R668-AD-1314',
                              'INVENTORY',
                              'V1_0',
                              'CUMULATIVE',
                              R668_AD_1314_RGRNInv.new(logger),
                              logger)
    @transfers << DT_File.new('LCRP',
                              'Regeneron',
                              'R668-AD-1314',
                              'INVENTORY',
                              'V1_0',
                              'CUMULATIVE',
                              R668_AD_1314_LCRPInv.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R1500-CL-1321',
                              'SITE',
                              'V1_0',
                              'CUMULATIVE',
                              R1500_CL_1321_site.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R1500-CL-1321',
                              'SUBJECT',
                              'V1_0',
                              'CUMULATIVE',
                              R1500_CL_1321_subject.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R1500-CL-1321',
                              'INVENTORY',
                              'V1_0',
                              'CUMULATIVE',
                              R1500_CL_1321_RGRNInv.new(logger),
                              logger)
    @transfers << DT_File.new('MDPC',
                              'Regeneron',
                              'R1500-CL-1321',
                              'INVENTORY',
                              'V1_0',
                              'CUMULATIVE',
                              R1500_CL_1321_MDPCInv.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R1908-1909-ALG-1325',
                              'SITE',
                              'V1_0',
                              'CUMULATIVE',
                              R1908_1909_ALG_1325_site.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R1908-1909-ALG-1325',
                              'SUBJECT',
                              'V1_0',
                              'CUMULATIVE',
                              R1908_1909_ALG_1325_subject.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R1908-1909-ALG-1325',
                              'INVENTORY',
                              'V1_0',
                              'CUMULATIVE',
                              R1908_1909_ALG_1325_RGRNInv.new(logger),
                              logger)
    @transfers << DT_File.new('LCRP',
                              'Regeneron',
                              'R1908-1909-ALG-1325',
                              'INVENTORY',
                              'V1_0',
                              'CUMULATIVE',
                              R1908_1909_ALG_1325_LCRPInv.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R668-AD-1307',
                              'SITE',
                              'V1_0',
                              'CUMULATIVE',
                              R668_AD_1307_site.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R668-AD-1307',
                              'SUBJECT',
                              'V1_0',
                              'CUMULATIVE',
                              R668_AD_1307_subject.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R668-AD-1307',
                              'INVENTORY',
                              'V1_0',
                              'CUMULATIVE',
                              R668_AD_1307_RGRNInv.new(logger),
                              logger)
    @transfers << DT_File.new('LCRP',
                              'Regeneron',
                              'R668-AD-1307',
                              'INVENTORY',
                              'V1_0',
                              'CUMULATIVE',
                              R668_AD_1307_LCRPInv.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R2222-HV-1326',
                              'SITE',
                              'V1_0',
                              'CUMULATIVE',
                              R2222_HV_1326_site.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R2222-HV-1326',
                              'SUBJECT',
                              'V1_0',
                              'CUMULATIVE',
                              R2222_HV_1326_subject.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R2222-HV-1326',
                              'INVENTORY',
                              'V1_0',
                              'CUMULATIVE',
                              R2222_HV_1326_RGRNInv.new(logger),
                              logger)
    @transfers << DT_File.new('LCRP',
                              'Regeneron',
                              'R2222-HV-1326',
                              'INVENTORY',
                              'V1_0',
                              'CUMULATIVE',
                              R2222_HV_1326_LCRPInv.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R668-AD-1334',
                              'SITE',
                              'V1_0',
                              'CUMULATIVE',
                              R668_AD_1334_site.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R668-AD-1334',
                              'SUBJECT',
                              'V1_0',
                              'CUMULATIVE',
                              R668_AD_1334_subject.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R668-AD-1334',
                              'INVENTORY',
                              'V1_0',
                              'CUMULATIVE',
                              R668_AD_1334_RGRNInv.new(logger),
                              logger)
    @transfers << DT_File.new('PPDL',
                              'Regeneron',
                              'R668-AD-1334',
                              'INVENTORY',
                              'V1_0',
                              'CUMULATIVE',
                              R668_AD_1334_PPDLInv.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R2009-HV-1304',
                              'SITE',
                              'V1_0',
                              'CUMULATIVE',
                              R2009_HV_1304_site.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R2009-HV-1304',
                              'SUBJECT',
                              'V1_0',
                              'CUMULATIVE',
                              R2009_HV_1304_subject.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R2009-HV-1304',
                              'INVENTORY',
                              'V1_0',
                              'CUMULATIVE',
                              R2009_HV_1304_RGRNInv.new(logger),
                              logger)
    @transfers << DT_File.new('MDPC',
                              'Regeneron',
                              'R2009-HV-1304',
                              'INVENTORY',
                              'V1_0',
                              'CUMULATIVE',
                              R2009_HV_1304_MDPCInv.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R1193-DM-1402',
                              'SITE',
                              'V1_0',
                              'CUMULATIVE',
                              R1193_DM_1402_site.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R1193-DM-1402',
                              'SUBJECT',
                              'V1_0',
                              'CUMULATIVE',
                              R1193_DM_1402_subject.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R1193-DM-1402',
                              'INVENTORY',
                              'V1_0',
                              'CUMULATIVE',
                              R1193_DM_1402_RGRNInv.new(logger),
                              logger)
    @transfers << DT_File.new('MDPC',
                              'Regeneron',
                              'R1193-DM-1402',
                              'INVENTORY',
                              'V1_0',
                              'CUMULATIVE',
                              R1193_DM_1402_MDPCInv.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R1033-HV-1107',
                              'SITE',
                              'V1_0',
                              'CUMULATIVE',
                              R1033_HV_1107_site.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R1033-HV-1107',
                              'SUBJECT',
                              'V1_0',
                              'CUMULATIVE',
                              R1033_HV_1107_subject.new(logger),
                              logger)
    @transfers << DT_File.new('RGRN',
                              'Regeneron',
                              'R1033-HV-1107',
                              'INVENTORY',
                              'V1_0',
                              'CUMULATIVE',
                              R1033_HV_1107_RGRNInv.new(logger),
                              logger)
    @transfers << DT_File.new('TKDA',
                              'Takeda',
                              'C16014',
                              'SITE',
                              'V1_0',
                              'CUMULATIVE',
                              C16014_site.new(logger),
                              logger)
    @transfers << DT_File.new('TKDA',
                              'Takeda',
                              'C16014',
                              'SUBJECT',
                              'V1_0',
                              'CUMULATIVE',
                              C16014_subject.new(logger),
                              logger)
    @transfers << DT_File.new('TKDA',
                              'Takeda',
                              'C16014',
                              'INVENTORY',
                              'V1_0',
                              'CUMULATIVE',
                              C16014_Inv.new(logger),
                              logger)
    @transfers << DT_File.new('TKDA',
                              'Takeda',
                              'C16019',
                              'SITE',
                              'V1_0',
                              'CUMULATIVE',
                              C16019_site.new(logger),
                              logger)
    @transfers << DT_File.new('TKDA',
                              'Takeda',
                              'C16019',
                              'SUBJECT',
                              'V1_0',
                              'CUMULATIVE',
                              C16019_subject.new(logger),
                              logger)
    @transfers << DT_File.new('TKDA',
                              'Takeda',
                              'C16019',
                              'INVENTORY',
                              'V1_0',
                              'CUMULATIVE',
                              C16019_Inv.new(logger),
                              logger)
    @transfers << DT_File.new('TKDA',
                              'Takeda',
                              'C16021',
                              'SITE',
                              'V1_0',
                              'CUMULATIVE',
                              C16021_site.new(logger),
                              logger)
    @transfers << DT_File.new('TKDA',
                              'Takeda',
                              'C16021',
                              'SUBJECT',
                              'V1_0',
                              'CUMULATIVE',
                              C16021_subject.new(logger),
                              logger)
    @transfers << DT_File.new('TKDA',
                              'Takeda',
                              'C16021',
                              'INVENTORY',
                              'V1_0',
                              'CUMULATIVE',
                              C16021_Inv.new(logger),
                              logger)
    @my_connections = DT_Connections.new(logger)
    @logger = logger
  end

  def length
    @transfers.length
  end

  def get_transfer (vendor, protocol, file_type)
    @transfers.each do |this_transfer|
      @logger.debug "->#{this_transfer.vendor}<- ->#{this_transfer.protocol}<- ->#{this_transfer.file_type}<-"
       if this_transfer.vendor    == vendor   and
          this_transfer.protocol  == protocol and
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

    @logger.debug "Directory is ->#{file_path}<-"
    @logger.debug "Filemask is ->#{file_mask}<-"

    if (file_list = Dir["#{file_mask}"]).length == 0
      @logger.info 'No files to process'
      return
    end

    begin
      this_connection = @my_connections.db_connect(this_transfer.client, env)
    rescue Exception => e
      @logger.error "DB Connection failure - #{e.message}"
      exit -1
    end

    begin
      this_s3 = @my_connections.s3_connect(this_transfer.client, env)
    rescue Exception => e
      @logger.error "S3 Connection failure - #{e.message}"
      exit -1
    end

    if this_transfer.mode == 'CUMULATIVE'
      @logger.info("Processing #{file_list.last} in "+Dir.pwd+".")

      if (num_in = this_transfer.filer.reader(file_list.last)) == 0
        @logger.info 'No records read in'
        return
      end

      @logger.info("#{num_in} read in")

      if (num_to_process = this_transfer.filer.processor(this_connection)) == 0
        @logger.info 'No records to process'
      end

      @logger.info("#{num_to_process} to process")

      insert_clause = INSERT_STATEMENTS[(this_transfer.file_type+'_'+this_transfer.file_version).to_sym]
      value_clauses = this_transfer.filer.writer(this_transfer.vendor)

      num_of_frames = 0

      while value_clauses.count > 0 do
        num_of_frames += 1

        value_frame = value_clauses.slice!(0, (value_clauses.count < INSERT_FRAME) ? value_clauses.count : INSERT_FRAME)
        insert_statement = insert_clause + value_frame.join(",\n")

        @logger.debug "#{insert_statement}"

        begin
         this_connection.query(insert_statement)
         rescue Mysql2::Error => e
           @logger.error "DB Insert failure - #{e.message}"
           exit -1
        end
      end

#     begin
#        this_connection.query('CALL load_dts_specimen_inventory_v1_0')
#      rescue Mysql2::Error => e
#        @logger.error "DB Load Execution failure - #{e.message}"
#        exit -1
#      end

      file_list.each do |my_file|
        @my_connections.s3_archive_file(this_s3,
                                        this_transfer.protocol,
                                        this_transfer.vendor,
                                        this_transfer.file_type,
                                        my_file)
      end
    else
      @logger.error 'Incremental data loads not supported yet'
      exit -1
    end

    @logger.info 'File processing end'
  end
end