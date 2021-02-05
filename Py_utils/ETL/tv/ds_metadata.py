import os


class System_Prop:


    ####################################################################
    ##                                                                ##
    ##        System Properties                                       ##
    ##                                                                ##
    ####################################################################
    
    # #Intermidiate/staging db an table in hive metastore datasource for Development
    # dev_staging_db="tv_mta_staging_dev"
    # dev_staging_s3_loc="s3://tv-mta/tv-staging-dev"
    
    # #Output DB for tv impressions for Development
    # dev_result_db="tv_mta_results_dev"
    # dev_result_s3_loc="s3://tv-mta/tv-results-dev"


    # #Intermidiate/staging db an table in hive metastore datasource for Production
    # prod_staging_db="tv_mta_staging_prod"
    # prod_staging_s3_loc="s3://tv-mta/tv-staging-prod"
    
    # #Output DB for tv impressions for Production
    # prod_result_db="tv_mta_results_prod"
    # prod_result_s3_loc="s3://tv-mta/tv-results-prod"

    @classmethod
    def init(cls, profile:str, db_suffix:str=None):

        if profile is None or len(profile.strip()) == 0:
            assert False, "Please define non empty System Profile like DEV, PROD etc."
        else: profile = profile.lower()
        if db_suffix is None or len(db_suffix.strip()) == 0: db_suffix = '' 
        else: db_suffix = db_suffix.lower()

        #Intermidiate/staging db an table in hive metastore datasource for given profile
        cls.staging_db =  "tv_mta_staging_{}{}".format(profile, db_suffix)
        cls.staging_s3_loc = "s3://tv-mta/tv-staging-{}".format(profile)
        
        #Output DB for tv impressions
        cls.result_db = "tv_mta_results_{}{}".format(profile, db_suffix)
        cls.result_s3_loc= "s3://tv-mta/tv-results-{}".format(profile)
        
        # if profile == 'PROD':
        #     cls.staging_db =  cls.prod_staging_db
        #     cls.staging_s3_loc = cls.prod_staging_s3_loc
        #     cls.result_db = cls.prod_result_db
        #     cls.result_s3_loc= cls.prod_result_s3_loc
        # elif profile == 'DEV':
        #     cls.staging_db =  cls.dev_staging_db
        #     cls.staging_s3_loc = cls.dev_staging_s3_loc
        #     cls.result_db = cls.dev_result_db
        #     cls.result_s3_loc= cls.dev_result_s3_loc
        # else:
        #     cls.staging_db =  "tv_mta_staging_{}{}".format(profile, db_suffix)
        #     cls.staging_s3_loc = "s3://tv-mta/tv-staging-{}".format(profile)
        #     cls.result_db = "tv_mta_results_{}{}".format(profile, db_suffix)
        #     cls.result_s3_loc= "s3://tv-mta/tv-results-{}".format(profile)



class S3Location:

    ####################################################################
    ##                                                                ##
    ##        S3 Data Locations constants                             ##
    ##                                                                ##
    ####################################################################



    # Input S3 datasources
    tv_s3_root="s3://tv-mta"
    
    tivo_s3="s3://tv-mta/tivo"
    kantar_s3="s3://tv-mta/kantar"
    exparian_s3="s3://tv-mta/experian"
    
    #Daily cadence date is part of filename
    #this data use to 1 week behind from current date
    tivo_s3_incremental_data=tivo_s3 + "/channel-change/weekly-new" 
    #tivo_s3_incremental_data="s3://tv-mta/static/tmp/tivo_raw/"

    kantar_tvweekly_s3_root = kantar_s3 + "/tvweekly/"

    #Weekly cadence week num is part of file name (00 to 52)
    # Every Tuesday of the week new file will arrive
    # what about next year ?? what will be the week representation?
    kantar_s3_incremental_data=kantar_s3 + "/tvweekly/{year}/tvweekly_{week}" 
    
    #Weekly cadence week num is part of file name (00 to 52)
    #This is basically a LMT always latest needs to be used
    #? Do we need older/hist mapping to resolve? - 
    #                       I guess no because it is a part of weekly data. each week will have its own creative file
    kantar_creative_loc=kantar_s3_incremental_data + "/creative" 


    kantar_log_files = ['tv_locnet', 'tv_locsln', 'tv_locspot', 'tv_locsyn', \
        'tv_natcab', 'tv_natcue', 'tv_natnet', 'tv_natsln', 'tv_natsyn']
                                        
    
    kantar_master_dir=kantar_s3 + "/master"
    kantar_tv_master=kantar_s3 + "/tv_master"
    
    #Quaterly arriaval it always brings new mappings. 
    #Once a new file available we need to use only new data and ignore all previously data file
    exparian_crosswalk_s3=exparian_s3+"/crosswalk"

    #Daily Arrivals needs to use latest mapping
    exparian_mapping_s3=exparian_s3

    #Daily Arrivals needs to use latest mapping
    #exparian_mango_s3="s3://tv-mta/static/tmp/experian_mango3_extracted"
    exparian_mango_s3=exparian_s3+"/experian_mango"
    
    #Static files provide by our System(Matt and Manmeet)
    kantar_tivo_channel_mapping = "s3://tv-mta/static/kantar_tivo_channel_mapping"
    kantar_tivo_dma_mapping = "s3://tv-mta/static/kantar_tivo_dma_mapping"

    #Temparory Static files provide by our System(Matt and Manmeet)
    kantar_tivo_channel_mapping_mz = "s3://tv-mta/static/tmp/kantar_tivo_channel_mapping_mz"
    kantar_tivo_channel_mapping_basic = "s3://tv-mta/static/tmp/kantar_tivo_channel_mapping_basic"
    hosted_ultimateowner_ids = "s3://tv-mta/static/hosted_ultimateowner_ids/"
    
    advertiser=kantar_master_dir + "/advertiser"
    brand=kantar_master_dir + "/brand"
    category=kantar_master_dir + "/category"
    industry=kantar_master_dir + "/industry" 
    major=kantar_master_dir + "/major" 
    market=kantar_master_dir + "/market" 
    media=kantar_master_dir + "/media" 
    microcategory  =kantar_master_dir + "/microcategory" 
    oproduct  =kantar_master_dir+ "/oproduct" 
    parent  =kantar_master_dir+ "/parent" 
    product  =kantar_master_dir+ "/product" 
    product_descriptor  =kantar_master_dir+ "/product_descriptor" 
    product_name  =kantar_master_dir+ "/product_name" 
    product_type  =kantar_master_dir+ "/product_type" 
    property  =kantar_master_dir+ "/property" 
    section  =kantar_master_dir+ "/section" 
    subcategory=kantar_master_dir+ "/subcategory"
    subsid=kantar_master_dir+ "/subsid"
    ultimateowner =kantar_master_dir+  "/ultimateowner"
    
    
    aff=kantar_tv_master+ "/aff"
    daypart=kantar_tv_master+ "/daypart" 
    oprog  =kantar_tv_master+ "/oprog" 
    prog  =kantar_tv_master+ "/prog" 
    progtype  =kantar_tv_master+ "/progtype"

    mta_tv_impressions_dir =  "mta-tv-impressions"
    creative_lmt_dir = "kantar-creative-lmt"
    stat_creative_dir = "stats-creative"
    stat_mta_impressions_dir = "stats-impressions"
    tv_etl_states_leaf_dir = "states/state/"
    kantar_master_lmts_dir = "kantar-lmts/master"
    kantar_tv_master_lmts_dir = "kantar-lmts/tv-master"
    
    

    @classmethod
    def set_configs(cls, configs_data:dict):
        
        if configs_data.get('mta_tv_impressions_dir'):
            cls.mta_tv_impressions_dir =  configs_data.get('mta_tv_impressions_dir')
        if configs_data.get('creative_lmt_dir'):
            cls.creative_lmt_dir =  configs_data.get('creative_lmt_dir')
        if configs_data.get('stat_creative_dir'):
            cls.stat_creative_dir =  configs_data.get('stat_creative_dir')
        if configs_data.get('stat_mta_impressions_dir'):
            cls.stat_mta_impressions_dir =  configs_data.get('stat_mta_impressions_dir')
        if configs_data.get('tv_etl_states_leaf_dir'):
            cls.tv_etl_states_leaf_dir =  configs_data.get('tv_etl_states_leaf_dir')
        if configs_data.get('kantar_master_lmts_dir'):
            cls.kantar_master_lmts_dir =  configs_data.get('kantar_master_lmts_dir')
        if configs_data.get('kantar_tv_master_lmts_dir'):
            cls.kantar_tv_master_lmts_dir =  configs_data.get('kantar_tv_master_lmts_dir')

        result_s3_loc = System_Prop.result_s3_loc
        cls.mta_impressions_result_loc = os.path.join(result_s3_loc, cls.mta_tv_impressions_dir)
        cls.creative_lmt_result_loc = os.path.join(result_s3_loc, cls.creative_lmt_dir)
        cls.stat_creative = os.path.join(result_s3_loc, cls.stat_creative_dir)
        cls.stat_mta_impressions = os.path.join(result_s3_loc, cls.stat_mta_impressions_dir)
        cls.tv_etl_states_loc = os.path.join(result_s3_loc, cls.tv_etl_states_leaf_dir)
        cls.kantar_master_lmts_dir = os.path.join(result_s3_loc, cls.kantar_master_lmts_dir)
        cls.kantar_tv_master_lmts_dir = os.path.join(result_s3_loc, cls.kantar_tv_master_lmts_dir)
        


    @classmethod
    def kantar_creative_location(cls, year: int, week: str):
        return cls.kantar_creative_loc.format(year= str(year), week=week)

    @classmethod
    def kantar_log_locations(cls, year: int, week: str):
        
        kantar_log_loc = cls.kantar_s3_incremental_data.format(year= year, week=week)
        results : list = list()
        for i in cls.kantar_log_files:
            results.append("{}/{}/".format(kantar_log_loc, i))
        return results




class TableName:
   
    ####################################################################
    ##                                                                ##
    ##        Table names constants                                   ##
    ##                                                                ##
    ####################################################################

    tivo_log = "ext_raw_tivo_log"
    kantar_log = "ext_raw_kantar_log"
    creative = "ext_raw_creative"
    experian_crosswalk = "ext_raw_experian_crosswalk"
    experian_lu = "raw_experian_lu"
    experian_mango = "ext_raw_experian_mango"
    market="ext_raw_market"
    oproduct="ext_raw_oproduct"
    product="ext_raw_product"
    media="ext_raw_media"
    product_name="ext_raw_product_name"
    property="ext_raw_property"
    ultimateowner="ext_raw_ultimateowner"
    advertiser="ext_raw_advertiser"
    oprog="ext_raw_oprog"
    prog="ext_raw_prog"
    dma_mapping="ext_kantar_tivo_dma_mapping"
    channel_mapping="ext_kantar_tivo_channel_mapping"
    #TEMP tables till combine data has not created for ext_kantar_tivo_channel_mapping
    channel_mapping_basic="ext_kantar_tivo_channel_mapping_basic"
    channel_mapping_mz="ext_kantar_tivo_channel_mapping_mz"
    hosted_ultimateowner_id ="ext_hosted_ultimateowner_id"

    # Intermediate table name prefixes
    tivo_resolved = "tivo_resolved_{}"
    kantar_resolved = "kantar_resolved_{}"
    experian_resolved = "experian_resolved_{}"
    tivo_kantar_merged = "tivo_kantar_merged_{}"
    tv_impressions = "impressions_{}_{}"
    creative_staging = "staging_creative_{}"
    
    creative_lmt = "kantar_creative_lmt"
    mta_tv_impressions="mta_tv_impressions"

    # Stat Tables
    stat_creative_lmt = "stat_kantar_creative_lmt"
    stat_mta_tv_impressions="stat_mta_tv_impressions"

    