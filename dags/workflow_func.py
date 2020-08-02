
def cas_docker_connector():
    "attempts to make a connection to CAS & Prints a string upon completion"
    # import sys
    # sys.version
    # import os
    # os.system('which python')
    from _config import container_cas35_login
    import swat
    host = container_cas35_login()[2]
    user = container_cas35_login()[0]
    pswd = container_cas35_login()[1]
    try:
        sess = swat.CAS(host,5571,user,pswd)
        print("SUCCESS! Connection to CAS Container Established")
        sess.close()
    except:
        # print(sess)
        print("ERROR! Can't connect to CAS")


def prep_data():
    "Use Vaex to open the parquet file and add new features to the input dataset"
    import vaex
    # import os
    # print(os.system('ls ~/store_files_airflow/'))
    try:
        df = vaex.open('/usr/local/airflow/store_files_airflow/churn_xl.parquet')
        df['total_mins'] = df.day_mins + df.eve_mins + df.night_mins + df.intl_mins
        print('INFO: Added Column Total Mins' )
        df['total_charge'] = df.day_charge + df.eve_charge + df.night_charge + df.intl_charge
        print('INFO: Added Column Total Charge' )
        samp_df=df.sample(n=3500,random_state=123)
        samp_df=samp_df.to_pandas_df(virtual=True)
        samp_df.to_csv('/usr/local/airflow/store_files_airflow/churn-vx.csv',index=False)
        print('Generated & Saved ABT' )
    except:
        print("ERROR: Cant perform data prep steps")
    

def call_cas_automl():
    import swat
    from _config import container_cas35_login
    import numpy as np
    import pandas as pd
    import os

    host = container_cas35_login()[2]
    user = container_cas35_login()[0]
    pswd = container_cas35_login()[1]

    sess = swat.CAS(host,5571,user,pswd)
    sess.setsessopt(caslib="casuser")
    sess.setsessopt(locale='en-US')
    sess.loadactionset(actionset="dataSciencePilot")

    churn_df=pd.read_csv('/usr/local/airflow/store_files_airflow/churn-vx.csv')
    churn_df.columns = [i.replace(' ','_').replace("'",'').lower() for i in churn_df.columns]
    #Check out how the resultant dataset look like
    # Load Table to CAS
    out=sess.upload(churn_df,casout=dict(name='churn',caslib='casuser'))
    print('INFO: Loaded Data Into CAS - TableName: Churn')
    sess.loadactionset('fedSQL') #Enable SQL actions - for distributed SQL         
    out=out.casTable #get CASTAble
    #programmatically build query
    col_list= [i for i in out.columns if i not in ('area_code','churn','intl_plan','vmail_plan')]
    cas_lib='casuser'
    option_params='{options replace=true}'
    query = """create table {}.churn {} as select {}, 
    cast(intl_plan as char) as intl_plan,
    cast(vmail_plan as char) as vmail_plan,
    cast(area_code as varchar) as area_code,
    cast(churn as varchar) as churn
    from casuser.churn """.format(cas_lib,
                                option_params,
                                col_list).replace('[','').replace(']','').replace("'",'')

    #execute query and check the results
    sess.fedsql.execdirect(query) # run the query
    print('INFO: Executed FedSQL Sucessfully - All Set to Launch AutoML')
    out = sess.CASTable('CHURN', caslib ='casuser')

    effect_vars = {'area_code','account_length', 'custserv_calls', 'day_calls', 'day_charge','day_mins',
    'eve_calls','eve_charge', 'eve_mins', 'intl_calls', 'intl_charge', 'intl_mins', 'intl_plan','night_calls',
    'night_charge','night_mins', 'vmail_message', 'vmail_plan'}

    sess.dataSciencePilot.dsAutoMl(   
            table                   = out,
            target                  = "CHURN",
            inputs = effect_vars,
            transformationPolicy    = {"missing":True, "cardinality":True,
                                    "entropy":True, "iqv":True,
                                    "skewness":True, "kurtosis":True, "Outlier":True},
            modelTypes              = ["decisionTree", "GRADBOOST"],
            objective               = "AUC",
            sampleSize              = 20,
            topKPipelines           = 10,
            kFolds                  = 2,
            transformationOut       = {"name" : "TRANSFORMATION_OUT", "replace" : True},
            featureOut              = {"name" : "FEATURE_OUT", "replace" : True},
            pipelineOut             = {"name" : "PIPELINE_OUT", "replace" : True},
            saveState               = dict(modelNamePrefix='churn_model', replace = True)      
    )
    print('INFO: AutoML Completed Successfully')
    # Save Feature Machine Astore
    churn_model_fm_ = sess.CASTable('CHURN_MODEL_FM_')
    feature_machine_astore = 'automl_churn_features_astore.sashdat'
    sess.save(table=churn_model_fm_,name= feature_machine_astore, replace=True)
    print(f'INFO: Saved Feature Machine Astore - {feature_machine_astore}')

    # Save Top N Pipelines Assessed
    pipeline_out = sess.CASTable('PIPELINE_OUT')
    top_pipelines = 'topNPipeLines.csv'
    pipeline_out.iloc[:,:'NFeatures'].to_frame().to_csv(f'/usr/local/airflow/store_files_airflow/{top_pipelines}', index=False)
    print(f'INFO: Saved Top N PipeLines - {top_pipelines}')
    
    # Save Champion Model
    champ_model = sess.CASTable('CHURN_MODEL_GRADBOOST_1')
    champ_model_astore = 'automl_churn_champ.sashdat'
    sess.save(table=champ_model,name= champ_model_astore, replace=True)
    print(f'INFO: Saved Champion Model Astore - {champ_model_astore}')
    sess.close()


def enterprise_viya_aws_connector():
    "attempts to make a connection to CAS & Prints a string upon completion"
    # import sys
    # sys.version
    # import os
    # os.system('which python')
    from _config import enterprise_viya_login
    import swat
    import os
    host = enterprise_viya_login()[2]
    user = enterprise_viya_login()[0]
    pswd = enterprise_viya_login()[1]
    os.environ['CAS_CLIENT_SSL_CA_LIST']='/usr/local/airflow/store_files_airflow/trustedcerts.pem'
    try:
        sess = swat.CAS(host,8777,user,pswd,protocol='https')
        print("SUCCESS! Connection to Enterprise Viya-CAS-Established - Using HTTPS")
        sess.close()
    except Exception as err:
        # print(sess)
        print(err)
        print("ERROR! Can't connect to CAS")


def enterprise_viya_race_connector():
    "attempts to make a connection to CAS & Prints a string upon completion"
    # import sys
    # sys.version
    # import os
    # os.system('which python')
    from _config import race_viya_login
    import swat
    import os
    host = race_viya_login()[2]
    user = race_viya_login()[0]
    pswd = race_viya_login()[1]
    # os.environ['CAS_CLIENT_SSL_CA_LIST']='/usr/local/airflow/store_files_airflow/trustedcerts.pem'
    try:
        sess = swat.CAS(host,5570,user,pswd)
        print("SUCCESS! Connection to Enterprise Viya-CAS-Established - Using Binary Protocol")
        sess.close()
    except Exception as err:
        # print(sess)
        print(err)
        print("ERROR! Can't connect to CAS")


def score_and_download_artifacts():
    import swat
    from _config import container_cas35_login
    import numpy as np
    import pandas as pd
    import os

    host = container_cas35_login()[2]
    user = container_cas35_login()[0]
    pswd = container_cas35_login()[1]

    sess = swat.CAS(host,5571,user,pswd)
    sess.setsessopt(caslib="casuser")
    sess.setsessopt(locale='en-US')
    sess.loadtable('automl_churn_features_astore.sashdat',casout=dict(name='fm_astore'))

    sess.loadtable('automl_churn_champ.sashdat',casout=dict(name='champ_model_astore'))
    ## Load some Input data
    churn_df=pd.read_csv('/usr/local/airflow/store_files_airflow/churn-vx.csv')
    churn_df.columns = [i.replace(' ','_').replace("'",'').lower() for i in churn_df.columns]
    out=sess.upload(churn_df,casout=dict(name='churn',caslib='casuser'))
    sess.loadactionset('fedSQL') #Enable SQL actions - for distributed SQL         
    out=out.casTable #get CASTAble
    #programmatically build query
    col_list= [i for i in out.columns if i not in ('area_code','churn','intl_plan','vmail_plan')]
    cas_lib='casuser'
    option_params='{options replace=true}'
    query = """create table {}.churn {} as select {}, 
    cast(intl_plan as char) as intl_plan,
    cast(vmail_plan as char) as vmail_plan,
    cast(area_code as varchar) as area_code,
    cast(churn as varchar) as churn
    from casuser.churn """.format(cas_lib,
                                option_params,
                                col_list).replace('[','').replace(']','').replace("'",'')

    #execute query and check the results
    sess.fedsql.execdirect(query) # run the query
    out = sess.CASTable('CHURN', caslib ='casuser') #get the results
    sess.loadactionset('astore')
    #score with feature machine astore
    sess.score(table='CHURN',casout=dict(name='fm_result', caslib='casuser', replace =True),rstore='FM_ASTORE', copyvars=['CHURN'])

    # score the feature machine result with the champ model astore
    sess.score(table='fm_result',
            casout=dict(name='model_result', caslib='casuser', replace =True),
            rstore='champ_model_astore')

    # Download Scored Data - If Big Data - Solve with volume mapping / defining CASLib
    model_result=sess.CASTable('model_result').to_frame()
    model_result.to_csv('/usr/local/airflow/store_files_airflow/scored_data.csv',index=False)

    #  Download Feature Machine
    feature_machine=sess.download(rstore='fm_astore')
    with open('/usr/local/airflow/store_files_airflow/feature_machine.astore','wb') as file:
        file.write(feature_machine['blob'])

    # Download Champion Model
    champ_model=sess.download(rstore='champ_model_astore')
    with open('/usr/local/airflow/store_files_airflow/champ_model.astore','wb') as file:
        file.write(champ_model['blob'])

    sess.close()


def push_model_artifacts_x_enterprise_model_manager():
    import swat
    from _config import race_viya_login
    import os
    import sasctl
    from sasctl import get
    from sasctl.tasks import register_model

    host = race_viya_login()[2]
    user = race_viya_login()[0]
    pswd = race_viya_login()[1]
    # mm_host = enterprise_viya_login()[3]
    # os.environ['CAS_CLIENT_SSL_CA_LIST']='/usr/local/airflow/store_files_airflow/trustedcerts.pem'

    ent_sess = swat.CAS(host,5570,user,pswd) 
    ent_sess.loadactionset('aStore')
    ent_sess.setsessopt(locale='en-US')

    with open('/usr/local/airflow/store_files_airflow/champ_model.astore','rb') as file:
        blob = file.read()
        store_ = swat.blob(blob)
        ent_sess.astore.upload(rstore=dict(name='champ_model', replace=True),store = store_)

    with open('/usr/local/airflow/store_files_airflow/feature_machine.astore','rb') as file:
        blob = file.read()
        store_ = swat.blob(blob)
        ent_sess.astore.upload(rstore=dict(name='feature_machine_churn', replace=True),store = store_)

    s = sasctl.Session(f'http://{host}/cas-shared-default-http/', user, pswd,protocol='http',verify_ssl=False)
    # get project
    mm_project = sasctl._services.model_repository.ModelRepository.get_project('sg_churn_auto_ml')

    # save astores
    champ_model = ent_sess.CASTable('champ_model')
    feature_machine = ent_sess.CASTable('feature_machine_churn')

    # if project doesnt exist - create, else update version and load artifacts

    if mm_project is None:
        champ_model_mm = register_model(champ_model, 'auto-ml-champ', 'sg_churn_auto_ml',force=True,version='latest')
        feature_machine_mm = register_model(feature_machine, 'auto-ml-feature-machine', 'sg_churn_auto_ml',force=False,version='latest')
    else:
        champ_model_mm = register_model(champ_model, 'auto-ml-champ', 'sg_churn_auto_ml',force=False,version='new')
        feature_machine_mm = register_model(feature_machine, 'auto-ml-feature-machine', 'sg_churn_auto_ml',force=False,version='latest')

    print(f"INFO:Successfully saved feature machine & model to project: **{champ_model_mm.projectName}** in new project version: **{champ_model_mm.projectVersionName}**  ")
    s.close()
    ent_sess.close()


