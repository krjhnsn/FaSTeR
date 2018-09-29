# -*- coding: utf-8 -*-
"""
Created on Mon Jan  9 10:41:52 2017

@author: kwjohnson

Description: module to execute FaSTer solves

General description of algorithm:

1. get_req_factor is called to get initial req_factor_data in parallel.
It uses a text file containing sql needed (this should be converted
to python entirely in the future)

2. A loop executes to call remaining solver feeds

3. A master .dat file is created with all solver feed data except req_factor.
this master .dat file is reused through the iterations of the solve.

4. Solve models are generated and solved in parallel.

5. Solve results are written to text file and then back to database
after all iterations have completed.


Updates:
    01.31.17    kwjo   initial creation
    
"""

#==============================================================================
# import all packages needed
#==============================================================================

#import base packages
from threading import Thread
import time
import sys
import os
import pyodbc as pyodbc
import pandas as pd
import solve_base_sandbox as sb
import cplex

#import solutions packages
import get_req_factor as rf

#==============================================================================
# inputs for testing (can leave commented out)   
#==============================================================================

# inputs
scen_id = 1225
area = 'CMP'
database = # removed for security
iteration = 1
   
# database connection stuff
  # removed for security
# connection = pyodbc.connect(conn_string)

# parameters for master .dat file
dat_file_path = "C:\PythonSolves\FaSTer"
dat_file_name = "FaSTer.dat"

# parameters for loops where a retry may be needed (e.g. getting solver feeds)
retry_limit = 3
retry_counter = 0
success = 0

#==============================================================================
# global constants and some initial setup
#==============================================================================

# constant list of valid areas. Uncomment all when pushing to production
area_list = [
        'CMP'
        ,'CVD'
        ,'DIFFUSION'
        ,'DRY ETCH'
        ,'IMPLANT'
        ,'METROLOGY'
        ,'PHOTO'
        ,'PVD'
        ,'RDA'
        ,'WET PROCESS'
] 

# create list of areas we want in final result set
if area == '%':
    area_list_in = [i for i in area_list]
else:
    area_list_in = [i for i in area_list if i == area]


# these solve feeds are generated for each area individually
area_solve_feeds = {
"zSolverConvIndex": ["setConvIndex", 0],
"zSolverAvailTools": ["setAvailTools", 0],
"zSolverMaxWSLoad": ["setMaxWSLoad", 0],
"zSolverObjWts": ["setObjWts", 0],
"zSolverObsoleteTools": ["setObsoleteTools", 0],
"zSolverQual": ["setQual", 0],
"zSolverWSMaxCounts": ["setWSMaxCounts", 0],
"zSolverWSMaxToolAdds": ["setWSMaxToolAdds", 0],
"zSolverRPTBasis": ["setRPTBasis", 0],
"zSolverMinMaxLoadPerc": ["setMinMaxLoad", 0],
}

# these solve feeds are generated once and are used by all areas
master_solve_feeds = {
"zSolverReqFactor": ["setReqFactor", 0],
"zSelectIter": ["NA", 0],
"zvw_ui_iter_param_changes": ["NA", 0], #pulling data from view instead to do data manipulations in python for iterations
}

# initialize dictionarys to hold solve feed dataframes
area_solve_feed_data = {}

connection = pyodbc.connect(conn_string)
# connection.close()


######################## get master solver feeds #############################

# get iterations turned on for the scenario
query = "exec zSelectIter {0}, '{1}'".format(scen_id, area)
t_start = time.time()

try:
    df_select_iter = pd.read_sql_query(query, con = connection)  
    master_solve_feeds["zSelectIter"][1] = 1 

except:
    print("a failure has occured generating: zSelectIter")
    print(sys.exc_info()) 
   
df_selected_iter = df_select_iter[df_select_iter['qty']==1]
iter_list = df_selected_iter['iter'].tolist()
iter_list = [int(i) for i in iter_list] # to be sure that all iterations are int data type


# get parameter iteration data for all iterations (rpt, util)
sql = """
select 	
scen_id,
iter,
facility_id,
area_name,
WS_name,
recipe_name,
step_name,
trav_name,
node,
start_ww_num,
end_ww_num,
type,
qty,
qty2,
qty3,
qty4,
qty5,
text1,
text2
from zvw_ui_iter_param_changes	--	01.26.2015
where scen_id = {0}
and ((case when area_name = 'ALL' then '{1}' else area_name end) like '{1}')
and iter in (select iter from z_slvr_iter where scen_id = {0} and qty = 1)
"""
query = sql.format(scen_id, area)

try:
    t1 = time.time()
    print("executing solver feed: zvw_ui_iter_param_changes, {0}").format(time.ctime(t1))    
    df_iter_param_changes = pd.read_sql_query(query, con = connection)
    master_solve_feeds["zvw_ui_iter_param_changes"][1] = 1
    t2 = time.time()
    print("finished: zvw_ui_iter_param_changes, in {0} seconds").format(round(t2-t1, 2))

except:
    print("a failure has occured for: zvw_ui_iter_param_changes")
    print (sys.exc_info())


######################## get initial req_factor values ########################

try:
    t1 = time.time()
    print("executing solver feed: zSolverReqFactor, {0}").format(time.ctime(t1))    
    df_req_factor = rf.get_req_factor(scen_id = scen_id
                                     ,area = area
                                     ,conn_string = conn_string)
                                     
    master_solve_feeds["zSolverReqFactor"][1] = 1
    t2 = time.time()
    print("finished: zSolverReqFactor, in {0} seconds").format(round(t2-t1, 2))
     
except:
    print("a failure has occured for: zSolverReqFactor")
    print(sys.exc_info())

t_end = time.time()

# check if all master solve feeds were successful
check_sum = [master_solve_feeds[key][1] for key in master_solve_feeds]
if len(master_solve_feeds) == sum(check_sum):
    print("all master solver feeds executed successfully in {0} seconds, {1}").format(round(t_end-t_start, 2), time.ctime(time.time()))
else:
    print("master solver feed generation (zSolverReqfactor, zSelectIter, zvw_ui_iter_param_changes) has failed")

# close connection to release any locks on database
connection.close()

#==============================================================================
# function used to generate solve feeds for each area
#==============================================================================

def get_area_solver_feeds(scen_id, area, area_solve_feed_list, conn_string):
    '''
    generate the general purpose solve feeds for FaSTer
    
    returns: dictionary containing dataframes of solve feeds
    '''
    
    # open database connection
    connection = pyodbc.connect(conn_string)
    
    # retry & success variables for the loop
    retry_limit = 3
    retry_counter = 0
    success = 0    
    
    # dict to hold results from solver feeds
    area_solve_feed_data = {}    
    
    # loop through the solver feeds list getting one at a time
    while retry_counter < retry_limit and success == 0:
    
        t_start = time.time()    
            
        for feed_name in area_solve_feed_list:
            
            data_set = area_solve_feed_list[feed_name][0]        
            
            if (area_solve_feed_list[feed_name][1] == 0 and retry_counter < retry_limit):
                
                if feed_name == "zSolverObjWts": # doesn't need area_name param
                    query = "exec {0} {1}".format(feed_name, scen_id)
                else:
                    query = "exec {0} {1}, '{2}'".format(feed_name, scen_id, area)
            
                try:
                    t1 = time.time()                 
                    print("executing {0} solver feed: {1}").format(area, feed_name)                
                    area_solve_feed_data[data_set] = pd.read_sql_query(query, con = connection)
                    t2 = time.time()-t1                
                    print("finished {0} solver feed: {1}, in {2} seconds").format(area, feed_name, round(t2,1))
                    area_solve_feed_list[feed_name][1] = 1
                except:
                    print("a failure has occured for {0}: {1}, {2}").format(area, feed_name, time.ctime(time.time()))
                    print(sys.exc_info())
                    
        # end for loop                    
                    
        # check if all flags are set to 1 to verify successful solver feed generation        
        check_sum = [area_solve_feed_list[key][1] for key in area_solve_feed_list]    
        if len(area_solve_feeds) == sum(check_sum):
            success = 1
            t_end = time.time()
            print("all {0} solver feeds executed successfully in {1} seconds, {2}").format(area, round(t_end-t_start, 2), time.ctime(time.time()))
        else:
            retry_counter = retry_counter+1
            print("solver feed execution failed, retry attempt {0} of {1}, {2}").format(retry_counter
                                                                    ,retry_limit
                                                                    ,time.ctime(time.time()))
            print(sys.exc_info())
               
        if retry_counter == retry_limit:
           print("retry limit has been reached, solver feed execution failed")
    
    # end while loop    
    
    # close connection to release any locks on the database
    connection.close()
    
    # reset loop variables
    retry_limit = 3
    retry_counter = 0
    success = 0
    
    # reset success flags to 0
    for feed_name in area_solve_feed_list:
        area_solve_feed_list[feed_name][1] = 0
    
    # return dict of solve feeds
    return area_solve_feed_data


#==============================================================================
# various functions used during each iteration of FaSTeR
#==============================================================================

def get_iter_params(df_req_factor, iteration):
    '''
    used to get parameters (rpt, util) changing for each iteration
    and update req_factor values accordingly
    
    returns: dataframe with updated req_factor values
    '''

    # get util and rpt iteration data
    df_iter_param_util = df_iter_params[(df_iter_params['iter'] == iteration) & (df_iter_params['type' == 'util_change'])]
    df_iter_param_rpt = df_iter_params[(df_iter_params['iter'] == iteration) & (df_iter_params['type' == 'rpt_change'])]
    
    # perform updates    
    # todo: find way to handle condition joins for values of 'ALL' in the param data
    
    # clean up    
    
    return df_req_factor

def get_projn_changes(df_req_factor, scen_id, iteration):
    '''
    used to get projections for each iteration and update req_factor
    values accordingly
    
    returns: dataframe with updated req_factor values
    '''
    
    # get projection iteration data
    sql = "exec zActionIterProjnChanges {0}, {1}" 
    query = sql.format(scen_id, iteration)
    connection = pyodbc.connect(conn_string)
    
    try:
        df_projn_changes = pd.read_sql_query(query, con = connection)
    except:    
        print("a failure occured while executing 'zActionIterProjnChanges', iteration: {0}".format(iteration))        
        
    df_projn_changes = df_projn_changes.rename(columns={'fab_loc_name': 'facility_id',
                                                        'ww_num': 'outs_ww'})
                                                        
    df_projn_changes = df_projn_changes.dropna(subset=['qty']) #drop any null values

    # perform updates
    df_merged = pd.merge(df_req_factor, df_projn_changes, how='inner',
                         on=['node', 'trav_name', 'outs_ww', 'facility_id'],
                         sort=False,
                         suffixes=('', '_df_projn_changes'))

    f = lambda x: x['req_factor']*x['qty']
    df_merged['req_factor'] = pd.DataFrame(df_merged.apply(f, axis=1))

    df_req_factor = df_merged[['scen_id'
                                ,'facility_id'
                                ,'area_name'
                                ,'WS_name'
                                ,'node'
                                ,'trav_name'
                                ,'step_name'
                                ,'recipe_name'
                                ,'ww_num'
                                ,'outs_ww'
                                ,'req_factor']].copy()
                                
    # clean up
    connection.close()
    del df_merged
    del df_projn_changes    
    
    return df_req_factor

# used to set up the list to hold results of multi-threaded solves
def set_solve_threads (scen_id, area, conn_string, list_results):
    '''
    used to setup a list which can be used to retrieve results from a threaded process
    '''
    # setup list with the cplex solve objects

#==============================================================================
# function to make updates to .dat file, this will be modified by each iteration
#==============================================================================

# update .mod and .dat file with scen_id, area

# create one set of .mod and .dat files for each iteration

# use the solve base to create the cplex objects and solve them

# use the 'get_solution' function from the solve base to get the results

# use solve base to generate LP file (generate_lp), the .mod file needs code that exports .lp file to local folder


def update_dat_file(solver_feed_dict, dat_file_name, dat_file_path, area, Iter, suffix):
    '''
    used to update the master .dat file. Pass in a dictionary
    containing dataframes of each solver feed you want updated
    in the master .dat file. Make sure the keys in the dictionary
    match the set name exactly in the .dat file (e.g. 'setReqFactor')
    
    solver_feed_dict: dictionary used to identify which solve feeds in .dat file to update
        e.g. area_solve_feeds = {
        "setReqFactor": data_frame_object,
        }
        
    dat_file_name: name of the .dat file you want updated
        e.g. "FaSTer_master.dat"
    
    suffix: text to be appended to .dat file name
        e.g. "master"
        
    returns: dat_file_name+suffix
    '''

#    dat_file_name = "FaSTer.dat" # for troubleshooting
#    solver_feed_dict = area_solve_feed_data # for troubleshooting  
#    suffix = "master" # for troubleshooting
    
    counter = 0
    for feed_name in solver_feed_dict:
        
        try:
            t1 = time.time()            
            #test = area_solve_feed_data["zSolverMaxWSLoad"]            
            print("updating .dat file: {0}, {1}").format(feed_name, time.ctime(t1))                
            sb.insert_tuple(dat_file_name = dat_file_name
                        ,dat_file_path = dat_file_path
                        ,tuple_name = str(feed_name)
                        ,dat_file_suffix = suffix
                        ,query = ''
                        ,conn_string = ''
                        ,df = solver_feed_dict[feed_name])
            t2 = time.time()-t1                
            print("finished: {0}, in {1} seconds").format(feed_name, round(t2,2))
            
            # we want to change the .dat file name only once even though we may have many solver feeds to update
            if counter == 0:            
                dat_file_name = dat_file_name.replace(".dat", "_")+suffix+".dat"
                counter = 1
                suffix = ""
    
        except:
            print("a failure occured while updating .dat file: {0}, {1}").format(feed_name, time.ctime(time.time()))
            print(sys.exc_info())    
        
    return dat_file_name

#==============================================================================
# function to generate .lp file, this will be modified by each iteration
#==============================================================================

    # params: .dat file, .mod file

    # cplex stuff - generate model

    # return model for adding to parallel threads

    #cpx = cplex.Cplex("model.lp")
    #cplex.solve()

#==============================================================================
# loop through each iteration, solving each area in parallel
#==============================================================================
'''
for iter in iter_list:
    '''
    # general description of process
    # 1. each iteration is solved in a loop
    # 2. update parameters and then update .lp model
    # 3. solve each area in parallel
    # 4. export results to text file
    '''

    # setup lists to be used in each iteration
    list_threads = [] #instantiate parallel threads
    list_results = [] #instantiate list to hold query results

    retry_limit = 3
    try_count = 0
    success = 0
    success_test = 0

    # make updates to req_factor values    
    
    # aggregate req_factor to recipe level
    df_req_factor_final = df_req_factor.groupby(['WS_name',
                                         'recipe_name',
                                         'ww_num'])['req_factor'].sum().reset_index()
                                         
    f = lambda x: round(x['req_factor'])
    df_req_factor_final = pd.DataFrame(df_trav3.apply(f, axis=1)))

    
    # update .lp model with new req_factor values

    # append individual areas .lp model's to parallel solve list
    for i in range(len(AreaList)):    
        list_threads.append(Thread(target = set_query_threads,
                                            args = (scen_id
                                                    ,AreaList[i]
                                                    ,conn_string
                                                    ,list_results)))
        list_threads[i].start()
        print('Starting query for {0}, {1}'.format(AreaList[i], time.ctime(time.time())))
        time.sleep(5)  # add a slight delay to avoid deadlock issues

    for thread in list_threads:
        thread.join()

    # get results from solve

    # write results to text file

    #concat all the results into one final dataframe
    df_req_factor = pd.concat(list_results)

'''

#==============================================================================
# get area solve feeds
#==============================================================================

big_pile_of_area_solve_feeds = {}
for area in area_list_in:
    big_pile_of_area_solve_feeds[area] = get_area_solver_feeds(scen_id = scen_id
                                        ,area = area
                                        ,area_solve_feed_list = area_solve_feeds
                                        ,conn_string = conn_string)

# todo: parallelize the area solve feeds retrieval

#==============================================================================
#  generate master .dat files for all areas
#==============================================================================

area_master_dat_files = {}
for area in area_list_in:
    
    # create dataframes that hold area, scen_id, and the Iter to be added to .dat file
    suffix = "_"+str(area)
    
    # call function to setup master .dat file for the area        
    area_master_dat_files[area] = update_dat_file(solver_feed_dict = big_pile_of_area_solve_feeds[area]
                                            ,dat_file_name = dat_file_name
                                            ,dat_file_path = dat_file_path
                                            ,area = area
                                            ,Iter = ''
                                            ,suffix = suffix)    

#==============================================================================
# setup solve for a given iteration
#==============================================================================

#inputs: area_list_in, Iter, scen_id, file_path

# intialize dictionary to hold a cplex object for each area
cpx = {}

for area in area_list_in:

    # iteration = iter_list[0] # troubleshooting    

    # update master .dat file with Iter # and the appropriate req_factor data    

    # call function to generate .lp    
    cpx[area] = sb.generate_lp(file_path="C:\PythonSolves\FaSTer",
                    mod_file_name="FaSTer",
                    dat_file_name="FaSTer_"+str(area),
                    lp_file_name="FasTer_"+str(area)+str(Iter),
                    scen_id=scen_id
                    )

    # solve the .lp
    cpx[area].solve()

    # get results

    # write results to csv file
    df_solve_results.to_csv("C:\PythonSolves\FaSTeR\solve_results.csv"
                        ,header=True
                        ,index=False                        
                        ,mode='w')

# following iterations
    
   
    
    # call function to update req_factor values
    df_req_factor = get_projn_changes(df_req_factor, scen_id, iteration)
    
    # call function to update .dat file with new req_factor values
    solver_feed_dict = {"zSolverReqFactor": df_req_factor}
    dat_file_name = "FaSTer_master.dat"
    suffix = str(area+"_"+iteration)    
    dat_file_name_modified = update_dat_file(solver_feed_dict, dat_file_name, suffix):


    # call function to setup parallel solves

    # solve

    # call function to get results
    #TravIndex = ('trav', 'out_ww')
    #df=get_solution(cpx,'wo_trav',TravIndex)

    # write results to csv file
    df_req_factor.to_csv("C:\PythonSolves\FaSTeR\solve_results.csv"
                        ,header=False
                        ,index=False                        
                        ,mode='a')
    
    
    
    # get final csv containing all results
    df_final_results = pd.read_csv
    
    # write results back to db
    

#==============================================================================
# collect results and writeback to db
#==============================================================================